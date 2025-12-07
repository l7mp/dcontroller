package composite

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	viewv1a1 "github.com/l7mp/dcontroller/pkg/api/view/v1alpha1"
	"github.com/l7mp/dcontroller/pkg/object"
)

var _ cache.Cache = &DelegatingViewCache{}

// DelegatingViewCache is a view cache that delegates storage operations to a shared ViewCache
// while maintaining its own local informers. This allows multiple operators to share view storage
// while having independent lifecycles.
//
// Architecture:
//   - Storage: Delegates Add/Update/Delete/Get/List to a shared ViewCache
//   - Informers: Creates local ViewCacheInformer instances that wrap shared indexers
//   - Lifecycle: Each operator can Start/Stop independently
//   - Cross-operator watches: Work because all informers wrap the same shared indexers
type DelegatingViewCache struct {
	// storage is the shared ViewCache that holds the actual data
	storage *ViewCache

	// informers are local to this operator, wrapping shared indexers from storage
	informers map[schema.GroupVersionKind]*ViewCacheInformer

	// discovery for GVK conversions
	discovery ViewDiscoveryInterface

	// Mutex protects the informers map
	mu sync.RWMutex

	logger, log logr.Logger
}

// NewDelegatingViewCache creates a new delegating view cache that shares storage with other
// operators but maintains independent informer lifecycle.
func NewDelegatingViewCache(storage *ViewCache, opts CacheOptions) *DelegatingViewCache {
	logger := opts.Logger
	if logger.GetSink() == nil {
		logger = logr.Discard()
	}

	return &DelegatingViewCache{
		storage:   storage,
		informers: make(map[schema.GroupVersionKind]*ViewCacheInformer),
		discovery: NewViewDiscovery(),
		logger:    logger,
		log:       logger.WithName("delegating-cache"),
	}
}

// GetInformer fetches or constructs a local informer for the given object.
// The informer wraps the shared storage's indexer, so it sees all changes across operators.
func (d *DelegatingViewCache) GetInformer(ctx context.Context, obj client.Object, opts ...cache.InformerGetOption) (cache.Informer, error) {
	gvk := obj.GetObjectKind().GroupVersionKind()
	return d.GetInformerForKind(ctx, gvk, opts...)
}

// GetInformerForKind is similar to GetInformer, except that it takes a group-version-kind.
func (d *DelegatingViewCache) GetInformerForKind(ctx context.Context, gvk schema.GroupVersionKind, _ ...cache.InformerGetOption) (cache.Informer, error) {
	if !viewv1a1.IsViewKind(gvk) {
		return nil, fmt.Errorf("not a view GVK: %s", gvk)
	}

	d.mu.RLock()
	informer, exists := d.informers[gvk]
	d.mu.RUnlock()

	if exists {
		return informer, nil
	}

	err := d.registerInformerForKind(gvk)
	if err != nil {
		return nil, fmt.Errorf("could not create informer for GVK %s: %w", gvk, err)
	}

	d.mu.RLock()
	informer, exists = d.informers[gvk]
	d.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("informer lost for GVK %s", gvk)
	}

	return informer, nil
}

// registerInformerForKind creates a local informer that wraps the shared storage's indexer
// and registers it with the shared storage for event propagation.
func (d *DelegatingViewCache) registerInformerForKind(gvk schema.GroupVersionKind) error {
	if !viewv1a1.IsViewKind(gvk) {
		return fmt.Errorf("not a view GVK: %s", gvk)
	}

	d.log.V(4).Info("registering local informer for GVK", "gvk", gvk)

	// Get or create the indexer from shared storage
	indexer, err := d.storage.GetCacheForKind(gvk)
	if err != nil {
		return err
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if _, exists := d.informers[gvk]; exists {
		d.log.V(8).Info("refusing to register informer for GVK: already exists", "gvk", gvk)
		return nil
	}

	// Create a local informer wrapping the shared indexer
	// Multiple operators create their own informers, but they all wrap the same shared indexer
	informer := NewViewCacheInformer(gvk, indexer, d.log)
	d.informers[gvk] = informer

	// Register with shared storage so it can propagate events to this informer
	if err := d.storage.RegisterDelegatingInformer(gvk, informer); err != nil {
		return err
	}

	return nil
}

// RemoveInformer removes a local informer entry and unregisters it from shared storage.
func (d *DelegatingViewCache) RemoveInformer(ctx context.Context, obj client.Object) error {
	gvk := obj.GetObjectKind().GroupVersionKind()

	d.mu.Lock()
	informer, exists := d.informers[gvk]
	if exists {
		delete(d.informers, gvk)
	}
	d.mu.Unlock()

	// Unregister from shared storage
	if exists {
		if err := d.storage.UnregisterDelegatingInformer(gvk, informer); err != nil {
			return err
		}
	}

	return nil
}

// IndexField adds an index to the shared storage.
// Note: This affects all operators since storage is shared.
func (d *DelegatingViewCache) IndexField(ctx context.Context, obj client.Object, field string, extractValue client.IndexerFunc) error {
	// Delegate to shared storage
	return d.storage.IndexField(ctx, obj, field, extractValue)
}

// Get retrieves an object from the shared storage.
func (d *DelegatingViewCache) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	target, ok := obj.(object.Object)
	if !ok {
		return apierrors.NewBadRequest("must be called with an object.Object")
	}

	gvk := obj.GetObjectKind().GroupVersionKind()

	if !viewv1a1.IsViewKind(gvk) {
		return fmt.Errorf("not a view GVK: %s", gvk)
	}

	d.log.V(5).Info("get (delegating to shared storage)", "gvk", gvk, "key", key.String())

	// Delegate to shared storage
	return d.storage.Get(ctx, key, target, opts...)
}

// List retrieves a list of objects from the shared storage.
func (d *DelegatingViewCache) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	listGVK := list.GetObjectKind().GroupVersionKind()
	objGVK := d.discovery.ObjectGVKFromListGVK(listGVK)

	if !viewv1a1.IsViewKind(objGVK) {
		return fmt.Errorf("not a view GVK: %s", objGVK)
	}

	d.log.V(5).Info("list (delegating to shared storage)", "listGVK", listGVK, "objGVK", objGVK)

	// Delegate to shared storage
	return d.storage.List(ctx, list, opts...)
}

// Start runs until the context is closed. It blocks.
// Starts all local informers and waits for context cancellation.
func (d *DelegatingViewCache) Start(ctx context.Context) error {
	d.log.V(2).Info("delegating cache start")

	// Start all local informers
	d.mu.RLock()
	for gvk, informer := range d.informers {
		d.log.V(4).Info("starting local informer", "gvk", gvk)
		go informer.Run(ctx.Done())
	}
	d.mu.RUnlock()

	// Wait for context cancellation
	<-ctx.Done()

	d.log.V(2).Info("delegating cache stopped")

	// Clean up: unregister all informers from shared storage
	d.mu.RLock()
	informersToUnregister := make(map[schema.GroupVersionKind]*ViewCacheInformer)
	for gvk, informer := range d.informers {
		informersToUnregister[gvk] = informer
	}
	d.mu.RUnlock()

	for gvk, informer := range informersToUnregister {
		if err := d.storage.UnregisterDelegatingInformer(gvk, informer); err != nil {
			d.log.Error(err, "failed to unregister informer on shutdown", "gvk", gvk)
		}
	}

	return nil
}

// WaitForCacheSync waits for all local informers to sync.
// Since our informers wrap the shared storage's indexers, they're always synced.
func (d *DelegatingViewCache) WaitForCacheSync(ctx context.Context) bool {
	// Our informers wrap shared storage's indexers which are always synced
	return true
}

// GetClient returns a client that can read/write to the shared storage.
func (d *DelegatingViewCache) GetClient() client.WithWatch {
	// Return the shared storage's client - it already handles Add/Update/Delete
	return d.storage.GetClient()
}

// Watch delegates to the shared storage.
func (d *DelegatingViewCache) Watch(ctx context.Context, list client.ObjectList, opts ...client.ListOption) (watch.Interface, error) {
	return d.storage.Watch(ctx, list, opts...)
}

// Add is not part of the cache.Cache interface, but we provide it for compatibility
// with code that uses ViewCache directly. It delegates to shared storage and triggers
// local informers.
//
// Note: This triggers informers in ALL operators because they all wrap the same indexers.
// The shared storage's Add() already updates the indexer, and ViewCacheInformer will see
// the change via its wrapper. However, we need to explicitly trigger events since the
// shared storage doesn't know about our local informers.
func (d *DelegatingViewCache) Add(obj object.Object) error {
	gvk := obj.GetObjectKind().GroupVersionKind()

	if !viewv1a1.IsViewKind(gvk) {
		return fmt.Errorf("not a view GVK: %s", gvk)
	}

	d.log.V(5).Info("add (delegating to shared storage)", "gvk", gvk,
		"key", client.ObjectKeyFromObject(obj).String())

	// Delegate storage operation to shared storage
	// The shared storage will update the indexer and trigger its own informers
	return d.storage.Add(obj)
}

// Update is not part of the cache.Cache interface, but we provide it for compatibility.
// It delegates to shared storage.
func (d *DelegatingViewCache) Update(oldObj, newObj object.Object) error {
	gvk := newObj.GetObjectKind().GroupVersionKind()

	if !viewv1a1.IsViewKind(gvk) {
		return fmt.Errorf("not a view GVK: %s", gvk)
	}

	d.log.V(5).Info("update (delegating to shared storage)", "gvk", gvk,
		"key", client.ObjectKeyFromObject(newObj).String())

	// Delegate to shared storage
	return d.storage.Update(oldObj, newObj)
}

// Delete is not part of the cache.Cache interface, but we provide it for compatibility.
// It delegates to shared storage.
func (d *DelegatingViewCache) Delete(obj object.Object) error {
	gvk := obj.GetObjectKind().GroupVersionKind()

	if !viewv1a1.IsViewKind(gvk) {
		return fmt.Errorf("not a view GVK: %s", gvk)
	}

	d.log.V(5).Info("delete (delegating to shared storage)", "gvk", gvk,
		"key", client.ObjectKeyFromObject(obj).String())

	// Delegate to shared storage
	return d.storage.Delete(obj)
}

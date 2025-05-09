package cache

// composite cache is a cache that serves views from the view cache and the rest from the default
// Kubernetes cache

import (
	"context"
	"errors"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/l7mp/dcontroller/pkg/object"
)

var _ cache.Cache = &FakeRuntimeCache{}

// FakeRuntimeCache is a fake implementation of Informers. Client can store only a single object.
// Source: sigs.k8s.io/controller-runtime/pkg/cache/informertest/fake_cache.go.
type FakeRuntimeCache struct {
	InformersByGVK map[schema.GroupVersionKind]toolscache.SharedIndexInformer
	Error          error
	Synced         *bool
	Store          *Store
}

func NewFakeRuntimeCache(s *runtime.Scheme) *FakeRuntimeCache {
	return &FakeRuntimeCache{
		Store:          NewStore(),
		InformersByGVK: map[schema.GroupVersionKind]toolscache.SharedIndexInformer{},
	}
}

// GetInformerForKind implements Informers.
func (c *FakeRuntimeCache) GetInformerForKind(ctx context.Context, gvk schema.GroupVersionKind, opts ...cache.InformerGetOption) (cache.Informer, error) {
	if c.InformersByGVK == nil {
		c.InformersByGVK = map[schema.GroupVersionKind]toolscache.SharedIndexInformer{}
	}
	informer, ok := c.InformersByGVK[gvk]
	if ok {
		return informer, nil
	}

	c.InformersByGVK[gvk] = &FakeInformer{Indexer: c.Store}
	return c.InformersByGVK[gvk], nil
}

// GetInformer implements Informers.
func (c *FakeRuntimeCache) GetInformer(ctx context.Context, obj client.Object, opts ...cache.InformerGetOption) (cache.Informer, error) {
	u, ok := obj.(object.Object)
	if !ok {
		return nil, errors.New("expecting an object.Object")
	}
	gvk := u.GroupVersionKind()
	return c.GetInformerForKind(ctx, gvk, opts...)
}

// RemoveInformer implements Informers.
func (c *FakeRuntimeCache) RemoveInformer(ctx context.Context, obj client.Object) error {
	u, ok := obj.(object.Object)
	if !ok {
		return errors.New("expecting an object.Object")
	}
	gvk := u.GroupVersionKind()
	delete(c.InformersByGVK, gvk)
	return nil
}

// WaitForCacheSync implements Informers.
func (c *FakeRuntimeCache) WaitForCacheSync(ctx context.Context) bool {
	if c.Synced == nil {
		return true
	}
	return *c.Synced
}

// Start implements Informers.
func (c *FakeRuntimeCache) Start(ctx context.Context) error {
	return c.Error
}

// IndexField implements Cache.
func (c *FakeRuntimeCache) IndexField(ctx context.Context, obj client.Object, field string, extractValue client.IndexerFunc) error {
	return nil
}

func (c *FakeRuntimeCache) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	get, ok, err := c.Store.GetByKey(key.String())
	if err != nil {
		return err
	}
	if !ok {
		return apierrors.NewNotFound(schema.GroupResource{
			Group:    obj.GetObjectKind().GroupVersionKind().Group,
			Resource: obj.GetObjectKind().GroupVersionKind().Kind,
		}, key.String())
	}

	object.DeepCopyInto(get, obj.(object.Object))

	return nil
}

func (c *FakeRuntimeCache) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	for _, obj := range c.Store.List() {
		object.AppendToListItem(list, obj)
	}
	return nil
}

func (c *FakeRuntimeCache) Add(obj any) error {
	u, ok := obj.(object.Object)
	if !ok {
		return errors.New("Add: object must be an unstuctured object")
	}

	if err := c.Store.Add(u); err != nil {
		return err
	}

	informer, err := c.GetInformer(context.Background(), u)
	if err != nil {
		return err
	}

	informer.(*FakeInformer).Add(u)

	return nil
}

func (c *FakeRuntimeCache) Update(oldObj, newObj any) error {
	u1, ok := oldObj.(object.Object)
	if !ok {
		return errors.New("Add: oldObj object must be an unstuctured object")
	}

	u2, ok := newObj.(object.Object)
	if !ok {
		return errors.New("Add: newObj object must be an unstuctured object")
	}

	if err := c.Store.Update(u2); err != nil {
		return err
	}

	informer, err := c.GetInformer(context.Background(), u2)
	if err != nil {
		return err
	}

	informer.(*FakeInformer).Update(u1, u2)

	return nil
}

func (c *FakeRuntimeCache) Delete(obj any) error {
	u, ok := obj.(object.Object)
	if !ok {
		return errors.New("Add: object must be a list")
	}

	if err := c.Store.Delete(u); err != nil {
		return err
	}

	informer, err := c.GetInformer(context.Background(), u)
	if err != nil {
		return err
	}

	informer.(*FakeInformer).Delete(u)

	return nil
}

// /////////////////////////////////
// taken from sigs.k8s.io/controller-runtime/pkg/controller/controllertest
//
// bug fixed: let AddEventHandler send the initial object list to the handler
var _ toolscache.SharedIndexInformer = &FakeInformer{}

type Lister interface {
	List() []object.Object
}

// FakeInformer provides fake Informer functionality for testing.
type FakeInformer struct {
	// Synced is returned by the HasSynced functions to implement the Informer interface
	Synced bool

	// RunCount is incremented each time RunInformersAndControllers is called
	RunCount int

	Indexer Lister

	handlers []eventHandlerWrapper
}

type modernResourceEventHandler interface {
	OnAdd(obj interface{}, isInInitialList bool)
	OnUpdate(oldObj, newObj interface{})
	OnDelete(obj interface{})
}

type legacyResourceEventHandler interface {
	OnAdd(obj interface{})
	OnUpdate(oldObj, newObj interface{})
	OnDelete(obj interface{})
}

// eventHandlerWrapper wraps a ResourceEventHandler in a manner that is compatible with client-go 1.27+ and older.
// The interface was changed in these versions.
type eventHandlerWrapper struct {
	handler any
}

func (e eventHandlerWrapper) OnAdd(obj interface{}) {
	if m, ok := e.handler.(modernResourceEventHandler); ok {
		m.OnAdd(obj, false)
		return
	}
	e.handler.(legacyResourceEventHandler).OnAdd(obj)
}

func (e eventHandlerWrapper) OnUpdate(oldObj, newObj interface{}) {
	if m, ok := e.handler.(modernResourceEventHandler); ok {
		m.OnUpdate(oldObj, newObj)
		return
	}
	e.handler.(legacyResourceEventHandler).OnUpdate(oldObj, newObj)
}

func (e eventHandlerWrapper) OnDelete(obj interface{}) {
	if m, ok := e.handler.(modernResourceEventHandler); ok {
		m.OnDelete(obj)
		return
	}
	e.handler.(legacyResourceEventHandler).OnDelete(obj)
}

// AddIndexers does nothing.  TODO(community): Implement this.
func (f *FakeInformer) AddIndexers(indexers toolscache.Indexers) error {
	return nil
}

// GetIndexer does nothing.  TODO(community): Implement this.
func (f *FakeInformer) GetIndexer() toolscache.Indexer {
	return nil
}

// Informer returns the fake Informer.
func (f *FakeInformer) Informer() toolscache.SharedIndexInformer {
	return f
}

// HasSynced implements the Informer interface.  Returns f.Synced.
func (f *FakeInformer) HasSynced() bool {
	return f.Synced
}

// AddEventHandler implements the Informer interface.  Adds an EventHandler to the fake Informers. TODO(community): Implement Registration.
func (f *FakeInformer) AddEventHandler(handler toolscache.ResourceEventHandler) (toolscache.ResourceEventHandlerRegistration, error) {
	f.handlers = append(f.handlers, eventHandlerWrapper{handler})

	for _, item := range f.Indexer.List() {
		handler.OnAdd(item, true)
	}

	return nil, nil
}

// Run implements the Informer interface.  Increments f.RunCount.
func (f *FakeInformer) Run(<-chan struct{}) {
	f.RunCount++
}

// Add fakes an Add event for obj.
func (f *FakeInformer) Add(obj metav1.Object) {
	for _, h := range f.handlers {
		h.OnAdd(obj)
	}
}

// Update fakes an Update event for obj.
func (f *FakeInformer) Update(oldObj, newObj metav1.Object) {
	for _, h := range f.handlers {
		h.OnUpdate(oldObj, newObj)
	}
}

// Delete fakes an Delete event for obj.
func (f *FakeInformer) Delete(obj metav1.Object) {
	for _, h := range f.handlers {
		h.OnDelete(obj)
	}
}

// AddEventHandlerWithResyncPeriod does nothing.  TODO(community): Implement this.
func (f *FakeInformer) AddEventHandlerWithResyncPeriod(handler toolscache.ResourceEventHandler, _ time.Duration) (toolscache.ResourceEventHandlerRegistration, error) {
	return f.AddEventHandler(handler)
}

// RemoveEventHandler does nothing.  TODO(community): Implement this.
func (f *FakeInformer) RemoveEventHandler(handle toolscache.ResourceEventHandlerRegistration) error {
	return nil
}

// GetStore does nothing.  TODO(community): Implement this.
func (f *FakeInformer) GetStore() toolscache.Store {
	return nil
}

// GetController does nothing.  TODO(community): Implement this.
func (f *FakeInformer) GetController() toolscache.Controller {
	return nil
}

// LastSyncResourceVersion does nothing.  TODO(community): Implement this.
func (f *FakeInformer) LastSyncResourceVersion() string {
	return ""
}

// SetWatchErrorHandler does nothing.  TODO(community): Implement this.
func (f *FakeInformer) SetWatchErrorHandler(toolscache.WatchErrorHandler) error {
	return nil
}

// SetTransform does nothing.  TODO(community): Implement this.
func (f *FakeInformer) SetTransform(t toolscache.TransformFunc) error {
	return nil
}

// IsStopped does nothing.  TODO(community): Implement this.
func (f *FakeInformer) IsStopped() bool {
	return false
}

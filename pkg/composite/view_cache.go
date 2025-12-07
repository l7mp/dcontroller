package composite

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	viewv1a1 "github.com/l7mp/dcontroller/pkg/api/view/v1alpha1"
	"github.com/l7mp/dcontroller/pkg/object"
)

var _ cache.Cache = &ViewCache{}

// DefaultWatchChannelBuffer defines the default buffer size for watches.
const DefaultWatchChannelBuffer = 256

// ViewCache implements an ephemeral store for view objects. The view cache has an internal cache
// per each GVK that can be stored in the cache. ViewCache is now operator-agnostic and can store
// views from any operator.
type ViewCache struct {
	mu        sync.RWMutex
	caches    map[schema.GroupVersionKind]toolscache.Indexer
	informers map[schema.GroupVersionKind]*ViewCacheInformer
	// delegatingInformers maintains a registry of informers from DelegatingViewCache instances.
	// When shared storage triggers events via Add/Update/Delete, it propagates them to all
	// registered delegating informers, enabling cross-operator watch functionality.
	delegatingInformers map[schema.GroupVersionKind][]*ViewCacheInformer
	discovery           ViewDiscoveryInterface
	logger, log         logr.Logger
}

// NewViewCache creates a new view cache that can store views from any operator.
func NewViewCache(opts CacheOptions) *ViewCache {
	logger := opts.Logger
	if logger.GetSink() == nil {
		logger = logr.Discard()
	}

	c := &ViewCache{
		caches:              make(map[schema.GroupVersionKind]toolscache.Indexer),
		informers:           make(map[schema.GroupVersionKind]*ViewCacheInformer),
		delegatingInformers: make(map[schema.GroupVersionKind][]*ViewCacheInformer),
		discovery:           NewViewDiscovery(),
		logger:              logger,
		log:                 logger.WithName("cache"),
	}
	return c
}

// RegisterCacheForKind registers a new GVK in the cache.
func (c *ViewCache) RegisterCacheForKind(gvk schema.GroupVersionKind) error {
	c.log.V(1).Info("registering cache for new GVK", "gvk", gvk)

	if !viewv1a1.IsViewKind(gvk) {
		return fmt.Errorf("not a view GVK: %s", gvk)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.caches[gvk]; exists {
		c.log.V(8).Info("refusing to register cache for GVK %s: cache already exists", "gvk", gvk)
		return nil
	}

	indexer := toolscache.NewIndexer(
		toolscache.MetaNamespaceKeyFunc,
		toolscache.Indexers{toolscache.NamespaceIndex: toolscache.MetaNamespaceIndexFunc},
	)

	c.caches[gvk] = indexer

	return nil
}

// GetCacheForKind returns the internal cache for a given GVK.
func (c *ViewCache) GetCacheForKind(gvk schema.GroupVersionKind) (toolscache.Indexer, error) {
	if !viewv1a1.IsViewKind(gvk) {
		return nil, fmt.Errorf("not a view GVK: %s", gvk)
	}

	c.mu.RLock()
	indexer, exists := c.caches[gvk]
	c.mu.RUnlock()

	if !exists {
		if err := c.RegisterCacheForKind(gvk); err != nil {
			return nil, err
		}
		c.mu.RLock()
		indexer, exists = c.caches[gvk]
		c.mu.RUnlock()
	}

	if !exists {
		return nil, fmt.Errorf("could not create cache for GVK %s", gvk)
	}

	return indexer, nil
}

// RegisterInformerForKind registers an informer for a GVK.
func (c *ViewCache) RegisterInformerForKind(gvk schema.GroupVersionKind) error {
	if !viewv1a1.IsViewKind(gvk) {
		return fmt.Errorf("not a view GVK: %s", gvk)
	}

	c.log.V(4).Info("registering informer for new GVK", "gvk", gvk)

	cache, err := c.GetCacheForKind(gvk)
	if err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.informers[gvk]; exists {
		c.log.V(8).Info("refusing to register informer for GVK %s: informer already exists", "gvk", gvk)
		return nil
	}

	informer := NewViewCacheInformer(gvk, cache, c.log)
	c.informers[gvk] = informer

	return nil
}

// RegisterDelegatingInformer registers an informer from a DelegatingViewCache for a GVK.
// This allows the shared storage to propagate events to all delegating caches.
func (c *ViewCache) RegisterDelegatingInformer(gvk schema.GroupVersionKind, informer *ViewCacheInformer) error {
	if !viewv1a1.IsViewKind(gvk) {
		return fmt.Errorf("not a view GVK: %s", gvk)
	}

	c.log.V(4).Info("registering delegating informer for GVK", "gvk", gvk)

	c.mu.Lock()
	defer c.mu.Unlock()

	c.delegatingInformers[gvk] = append(c.delegatingInformers[gvk], informer)

	return nil
}

// UnregisterDelegatingInformer removes a delegating informer from the registry.
func (c *ViewCache) UnregisterDelegatingInformer(gvk schema.GroupVersionKind, informer *ViewCacheInformer) error {
	if !viewv1a1.IsViewKind(gvk) {
		return fmt.Errorf("not a view GVK: %s", gvk)
	}

	c.log.V(4).Info("unregistering delegating informer for GVK", "gvk", gvk)

	c.mu.Lock()
	defer c.mu.Unlock()

	informers := c.delegatingInformers[gvk]
	for i, inf := range informers {
		if inf == informer {
			// Remove by replacing with last element and truncating
			c.delegatingInformers[gvk][i] = c.delegatingInformers[gvk][len(c.delegatingInformers[gvk])-1]
			c.delegatingInformers[gvk] = c.delegatingInformers[gvk][:len(c.delegatingInformers[gvk])-1]
			break
		}
	}

	return nil
}

// GetInformer fetches or constructs an informer for the given object.
func (c *ViewCache) GetInformer(ctx context.Context, obj client.Object, opts ...cache.InformerGetOption) (cache.Informer, error) {
	gvk := obj.GetObjectKind().GroupVersionKind()
	return c.GetInformerForKind(ctx, gvk, opts...)
}

// GetInformerForKind is similar to GetInformer, except that it takes a group-version-kind, instead
// of the underlying object.
func (c *ViewCache) GetInformerForKind(ctx context.Context, gvk schema.GroupVersionKind, _ ...cache.InformerGetOption) (cache.Informer, error) {
	if !viewv1a1.IsViewKind(gvk) {
		return nil, fmt.Errorf("not a view GVK: %s", gvk)
	}

	c.mu.RLock()
	informer, exists := c.informers[gvk]
	c.mu.RUnlock()

	if exists {
		return informer, nil
	}

	err := c.RegisterInformerForKind(gvk)
	if err != nil {
		return nil, fmt.Errorf("could not create informer for GVK %s: %w", gvk, err)
	}

	c.mu.RLock()
	informer, exists = c.informers[gvk]
	c.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("informer lost for GVK %s", gvk)
	}

	return informer, nil
}

// RemoveInformer removes an informer entry and stops it if it was running.
func (c *ViewCache) RemoveInformer(ctx context.Context, obj client.Object) error {
	gvk := obj.GetObjectKind().GroupVersionKind()

	c.mu.RLock()
	_, exists := c.informers[gvk]
	c.mu.RUnlock()

	if exists {
		delete(c.informers, gvk)
	}

	return nil
}

// Add inserts an object into the cache.
func (c *ViewCache) Add(obj object.Object) error {
	gvk := obj.GetObjectKind().GroupVersionKind()

	if !viewv1a1.IsViewKind(gvk) {
		return fmt.Errorf("not a view GVK: %s", gvk)
	}

	c.log.V(5).Info("add", "gvk", gvk, "key", client.ObjectKeyFromObject(obj).String(),
		"object", object.Dump(obj))

	cache, err := c.GetCacheForKind(gvk)
	if err != nil {
		return err
	}

	// The cache does not apply deepcopy to stored objects.
	obj = object.DeepCopy(obj)

	// Make sure to have a valid UID.
	object.WithUID(obj)

	// Add object to the cache.
	if err := cache.Add(obj); err != nil {
		return err
	}

	informer, err := c.GetInformerForKind(context.Background(), gvk)
	if err != nil {
		return err
	}

	// Trigger event on shared informer
	informer.(*ViewCacheInformer).TriggerEvent(toolscache.Added, nil, obj, false)

	// Also trigger event on all delegating informers
	c.mu.RLock()
	delegatingInfs := c.delegatingInformers[gvk]
	c.mu.RUnlock()

	for _, dinf := range delegatingInfs {
		dinf.TriggerEvent(toolscache.Added, nil, obj, false)
	}

	return nil
}

// Update modifies the object stored in the cache.
func (c *ViewCache) Update(oldObj, newObj object.Object) error {
	// The cache does not apply deepcopy to stored objects.
	newObj = object.DeepCopy(newObj)

	// Make sure to have a valid UID (cached views always have an UID).
	object.WithUID(newObj)

	gvk := newObj.GetObjectKind().GroupVersionKind()
	if !viewv1a1.IsViewKind(gvk) {
		return fmt.Errorf("not a view GVK: %s", gvk)
	}

	if object.DeepEqual(oldObj, newObj) {
		c.log.V(4).Info("update: suppressing object update", "gvk", gvk,
			"key", client.ObjectKeyFromObject(newObj).String())
		return nil
	}

	c.log.V(5).Info("update", "gvk", gvk, "key", client.ObjectKeyFromObject(newObj).String(),
		"object", object.Dump(newObj))

	cache, err := c.GetCacheForKind(gvk)
	if err != nil {
		return err
	}

	if err := cache.Update(newObj); err != nil {
		return err
	}

	informer, err := c.GetInformerForKind(context.Background(), gvk)
	if err != nil {
		return err
	}

	// Trigger event on shared informer
	informer.(*ViewCacheInformer).TriggerEvent(toolscache.Updated, oldObj, newObj, false)

	// Also trigger event on all delegating informers
	c.mu.RLock()
	delegatingInfs := c.delegatingInformers[gvk]
	c.mu.RUnlock()

	for _, dinf := range delegatingInfs {
		dinf.TriggerEvent(toolscache.Updated, oldObj, newObj, false)
	}

	return nil
}

// Delete removes an object from the cache.
func (c *ViewCache) Delete(obj object.Object) error {
	gvk := obj.GetObjectKind().GroupVersionKind()

	if !viewv1a1.IsViewKind(gvk) {
		return fmt.Errorf("not a view GVK: %s", gvk)
	}

	c.log.V(5).Info("delete", "gvk", gvk, "key", client.ObjectKeyFromObject(obj).String(),
		"object", object.Dump(obj))

	cache, err := c.GetCacheForKind(gvk)
	if err != nil {
		return err
	}

	// Delete the existing object from cache by key lookup (not the incoming obj directly).
	// This ensures we remove what's actually cached, handling cases where the caller passes a
	// stale object version. However, we trigger the informer event with the incoming obj, not
	// existingObj, to preserve pipeline semantics: the incoming obj represents the computed
	// delete delta from an upstream controller's pipeline, which must propagate downstream
	// unchanged. If we sent existingObj instead, controller chains would break because
	// downstream controllers would receive stale cached state rather than the transformed
	// delta. This differs from typical Kubernetes delete watches (which only send
	// name/namespace), but aligns with delta-controller's incremental computation model where
	// delete deltas carry meaningful transformed state.
	key, err := toolscache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		return err
	}

	existingObj, exists, err := cache.GetByKey(key)
	if err != nil {
		return err
	}
	if !exists {
		return apierrors.NewNotFound(schema.GroupResource{
			Group:    obj.GetObjectKind().GroupVersionKind().Group,
			Resource: obj.GetObjectKind().GroupVersionKind().Kind,
		}, key)
	}

	if err := cache.Delete(existingObj); err != nil {
		return err
	}

	informer, err := c.GetInformerForKind(context.Background(), gvk)
	if err != nil {
		return err
	}

	// Trigger event on shared informer
	// Trigger the informer event with the incoming obj. This may break some watchers, but
	// critically needed for controller chains to work.
	informer.(*ViewCacheInformer).TriggerEvent(toolscache.Deleted, nil, obj, false)

	// Also trigger event on all delegating informers
	c.mu.RLock()
	delegatingInfs := c.delegatingInformers[gvk]
	c.mu.RUnlock()

	for _, dinf := range delegatingInfs {
		dinf.TriggerEvent(toolscache.Deleted, nil, obj, false)
	}

	return nil
}

// IndexField adds an index with the given field name on the given object type.
func (c *ViewCache) IndexField(ctx context.Context, obj client.Object, field string, extractValue client.IndexerFunc) error {
	gvk := obj.GetObjectKind().GroupVersionKind()
	c.log.Info("IndexField called on ViewCache", "gvk", gvk)
	return errors.New("field indexing is not supported for ViewCache")
}

// Get retrieves an obj for the given object key from the cache.
func (c *ViewCache) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	target, ok := obj.(object.Object)
	if !ok {
		return apierrors.NewBadRequest("must be called with an object.Object")
	}

	gvk := obj.GetObjectKind().GroupVersionKind()

	if !viewv1a1.IsViewKind(gvk) {
		return fmt.Errorf("not a view GVK: %s", gvk)
	}

	cache, err := c.GetCacheForKind(gvk)
	if err != nil {
		return apierrors.NewBadRequest("invalid GVK")
	}

	c.log.V(5).Info("get", "gvk", gvk, "key", key.String())

	// Use toolscache.MetaNamespaceKeyFunc to look up the object, which stringifies an empty
	// namespace differently from the way client.ObjectKey would stringify it
	item, exists, err := cache.GetByKey(clientKeyToCacheKey(key).String())
	if err != nil {
		return err
	}

	if !exists {
		return apierrors.NewNotFound(schema.GroupResource{
			Group:    obj.GetObjectKind().GroupVersionKind().Group,
			Resource: obj.GetObjectKind().GroupVersionKind().Kind,
		}, key.String())
	}

	object.DeepCopyInto(item.(object.Object), target)

	return nil
}

// List retrieves a list of objects for a given namespace and list options from the cache.
func (c *ViewCache) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	listGVK := list.GetObjectKind().GroupVersionKind()
	objGVK := c.discovery.ObjectGVKFromListGVK(listGVK)

	if !viewv1a1.IsViewKind(objGVK) {
		return fmt.Errorf("not a view GVK: %s", objGVK)
	}

	cache, err := c.GetCacheForKind(objGVK)
	if err != nil {
		return apierrors.NewBadRequest("invalid GVK")
	}

	c.log.V(5).Info("list", "listGVK", listGVK, "objGVK", objGVK)

	// Extract selectors from options
	var labelSelector labels.Selector
	var fieldSelector fields.Selector
	var namespace string

	for _, opt := range opts {
		switch o := opt.(type) {
		case client.MatchingLabelsSelector:
			labelSelector = o.Selector
		case client.MatchingLabels:
			set := (map[string]string)(o)
			labelSelector = labels.SelectorFromSet(set)
		case client.MatchingFields:
			set := (fields.Set)(o)
			fieldSelector = fields.SelectorFromSet(set)
		case client.MatchingFieldsSelector:
			fieldSelector = o.Selector
		case client.InNamespace:
			namespace = string(o)
		}
	}

	for _, item := range cache.List() {
		target, ok := item.(object.Object)
		if !ok {
			return apierrors.NewConflict(
				schema.GroupResource{Group: objGVK.Group, Resource: objGVK.Kind},
				client.ObjectKeyFromObject(item.(client.Object)).String(),
				errors.New("cache must store object.Objects only"))
		}

		// Apply namespace filter
		if namespace != "" && target.GetNamespace() != namespace {
			continue
		}

		// Apply label selector
		if labelSelector != nil && !labelSelector.Matches(labels.Set(target.GetLabels())) {
			continue
		}

		// Apply field selector
		if fieldSelector != nil && !matchesFieldSelector(target, fieldSelector) {
			continue
		}

		AppendToListItem(list, object.DeepCopy(target))
	}

	return nil
}

// matchesFieldSelector is a helper function to match field selectors.
func matchesFieldSelector(obj object.Object, selector fields.Selector) bool {
	fieldSet := fields.Set{}

	// Always include standard metadata fields
	fieldSet["metadata.name"] = obj.GetName()
	fieldSet["metadata.namespace"] = obj.GetNamespace()

	// Add some domain-specific fields
	if status, found, _ := unstructured.NestedString(obj.Object, "status", "phase"); found {
		fieldSet["status.phase"] = status
	}

	if ready, found, _ := unstructured.NestedBool(obj.Object, "status", "ready"); found {
		fieldSet["status.ready"] = fmt.Sprintf("%t", ready)
	}

	return selector.Matches(fieldSet)
}

// Dump returns a string representation of all objects stored in a cache given a GVK. Used mostly
// for testing and debugging.
func (c *ViewCache) Dump(ctx context.Context, gvk schema.GroupVersionKind) []string {
	ret := []string{}
	cache, err := c.GetCacheForKind(gvk)
	if err != nil {
		return ret
	}
	for _, item := range cache.List() {
		target, ok := item.(object.Object)
		if ok {
			ret = append(ret, object.Dump(target))
		}
	}

	return ret
}

// Watch lets clients to wait for events occurring in the cache.
func (c *ViewCache) Watch(ctx context.Context, list client.ObjectList, opts ...client.ListOption) (watch.Interface, error) {
	listGVK := list.GetObjectKind().GroupVersionKind()
	objGVK := c.discovery.ObjectGVKFromListGVK(listGVK)

	c.log.V(5).Info("watch: adding watch", "listGVK", listGVK, "objGVK", objGVK)

	informer, err := c.GetInformerForKind(ctx, objGVK)
	if err != nil {
		return nil, err
	}

	// Extract selectors from options (same logic as List method)
	var labelSelector labels.Selector
	var fieldSelector fields.Selector
	var namespace string

	for _, opt := range opts {
		switch o := opt.(type) {
		case client.MatchingLabelsSelector:
			labelSelector = o.Selector
		case client.MatchingLabels:
			set := (map[string]string)(o)
			labelSelector = labels.SelectorFromSet(set)
		case client.MatchingFields:
			set := (fields.Set)(o)
			fieldSelector = fields.SelectorFromSet(set)
		case client.MatchingFieldsSelector:
			fieldSelector = o.Selector
		case client.InNamespace:
			namespace = string(o)
		}
	}

	watcher := &ViewCacheWatcher{
		eventChan:     make(chan watch.Event, DefaultWatchChannelBuffer),
		stopCh:        make(chan struct{}),
		labelSelector: labelSelector,
		fieldSelector: fieldSelector,
		namespace:     namespace,
		logger:        c.logger,
	}

	handler := toolscache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			if watcher.shouldIncludeObject(obj) {
				watcher.sendEvent(watch.Added, obj)
			}
		},
		UpdateFunc: func(oldObj, newObj any) {
			// Check if object matches selectors after update
			if watcher.shouldIncludeObject(newObj) {
				watcher.sendEvent(watch.Modified, newObj)
			}
		},
		DeleteFunc: func(obj any) {
			// For delete events, we should send the event if the object
			// was previously visible (matched selectors before deletion)
			if watcher.shouldIncludeObject(obj) {
				watcher.sendEvent(watch.Deleted, obj)
			}
		},
	}

	handlerReg, err := informer.AddEventHandler(handler)
	if err != nil {
		return nil, fmt.Errorf("failed to add event handler: %w", err)
	}

	go func() {
		<-ctx.Done()

		c.log.V(5).Info("stopping watcher", "gvk", objGVK.String())

		informer.RemoveEventHandler(handlerReg) //nolint:errcheck
		watcher.Stop()
	}()

	return watcher, nil
}

// Start runs all the informers known to this cache until the context is closed.  It blocks.
func (c *ViewCache) Start(ctx context.Context) error {
	c.log.V(4).Info("starting cache")

	// Start the informers
	c.mu.RLock()
	for _, informer := range c.informers {
		go informer.Run(ctx.Done())
	}
	c.mu.RUnlock()

	// We should wait for caches to sync here, but in our case they are always sync'd
	<-ctx.Done()

	return nil
}

// WaitForCacheSync waits for all the caches to sync. Returns false if it could not sync a cache.
func (c *ViewCache) WaitForCacheSync(_ context.Context) bool { return true }

var _ watch.Interface = &ViewCacheWatcher{}

// ViewCacheWatcher is a watcher specialized tof the view cache.
type ViewCacheWatcher struct {
	eventChan chan watch.Event
	stopCh    chan struct{}
	mutex     sync.Mutex
	stopped   bool

	labelSelector labels.Selector
	fieldSelector fields.Selector
	namespace     string

	logger logr.Logger
}

// shouldIncludeObject determines if an object should be included based on selectors.
func (w *ViewCacheWatcher) shouldIncludeObject(o any) bool {
	obj, ok := o.(object.Object)
	if !ok {
		w.logger.V(4).Info("watch: ignoring non-object.Object")
		return false
	}

	// Apply namespace filter
	if w.namespace != "" && obj.GetNamespace() != w.namespace {
		return false
	}

	// Apply label selector
	if w.labelSelector != nil && !w.labelSelector.Matches(labels.Set(obj.GetLabels())) {
		return false
	}

	// Apply field selector
	if w.fieldSelector != nil && !w.matchesFieldSelector(obj, w.fieldSelector) {
		return false
	}

	return true
}

// matchesFieldSelector checks if object matches field selector.
func (w *ViewCacheWatcher) matchesFieldSelector(obj object.Object, selector fields.Selector) bool {
	fieldSet := fields.Set{}

	// Always include standard metadata fields.
	fieldSet["metadata.name"] = obj.GetName()
	fieldSet["metadata.namespace"] = obj.GetNamespace()

	// Add some domain-specific fields.
	if status, found, _ := unstructured.NestedString(obj.Object, "status", "phase"); found {
		fieldSet["status.phase"] = status
	}

	if ready, found, _ := unstructured.NestedBool(obj.Object, "status", "ready"); found {
		fieldSet["status.ready"] = fmt.Sprintf("%t", ready)
	}

	return selector.Matches(fieldSet)
}

// sendEvent sends a watch event on the event channel.
func (w *ViewCacheWatcher) sendEvent(eventType watch.EventType, o any) {
	obj, ok := o.(object.Object)
	if !ok {
		w.logger.Info("refusing send object that is not an object.Object")
		return
	}

	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.stopped {
		return
	}

	event := watch.Event{Type: eventType, Object: obj}

	w.logger.V(8).Info("watcher sending object", "object", object.Dump(obj))

	select {
	case w.eventChan <- event:
	case <-time.After(time.Second):
		// If we can't send the event in 1 second, log and continue
		w.logger.Info("failed to send event, channel might be full", "event", event)
	}
}

// Stop stops the watcher.
func (w *ViewCacheWatcher) Stop() {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if !w.stopped {
		w.stopped = true
		close(w.stopCh)
		close(w.eventChan)
	}
}

// ResultChan returns the event channel of the watcher.
func (w *ViewCacheWatcher) ResultChan() <-chan watch.Event {
	return w.eventChan
}

// clientKeyToCacheKey converts a controller-runtime client.ObjectKey to a cache.ObjectName.  This
// conversion is necessary because client.ObjectKey (a type alias for types.NamespacedName) and
// cache.ObjectName have different String() implementations for cluster-scoped objects:
//
//   - cache.ObjectName.String() for empty namespace: "object-name"
//   - types.NamespacedName.String() for empty namespace: "/object-name" (note the leading slash)
//
// Since cache.MetaNamespaceKeyFunc uses cache.ObjectName internally to generate index keys,
// we must use the same type when looking up objects to ensure the key strings match.
// Without this conversion, lookups for cluster-scoped objects will fail because the keys
// won't match (e.g., looking up "/foo" when the cache indexed it as "foo").
//
// See: https://github.com/kubernetes/client-go/blob/master/tools/cache/store.go
// The comment in cache.ObjectName.String() explicitly states this behavior is intentional
// to maintain historical compatibility with MetaNamespaceKeyFunc.
func clientKeyToCacheKey(key client.ObjectKey) toolscache.ObjectName {
	return toolscache.ObjectName{
		Namespace: key.Namespace,
		Name:      key.Name,
	}
}

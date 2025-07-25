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

	"github.com/l7mp/dcontroller/pkg/object"
)

const DefaultWatchChannelBuffer = 256

var _ cache.Cache = &ViewCache{}

type ViewCache struct {
	mu          sync.RWMutex
	caches      map[schema.GroupVersionKind]toolscache.Indexer
	informers   map[schema.GroupVersionKind]*ViewCacheInformer
	discovery   ViewDiscoveryInterface
	logger, log logr.Logger
}

func NewViewCache(opts CacheOptions) *ViewCache {
	logger := opts.Logger
	if logger.GetSink() == nil {
		logger = logr.Discard()
	}

	c := &ViewCache{
		caches:    make(map[schema.GroupVersionKind]toolscache.Indexer),
		informers: make(map[schema.GroupVersionKind]*ViewCacheInformer),
		discovery: NewViewDiscovery(),
		logger:    logger,
		log:       logger.WithName("cache"),
	}
	return c
}

func (c *ViewCache) RegisterCacheForKind(gvk schema.GroupVersionKind) error {
	c.log.V(1).Info("registering cache for new GVK", "gvk", gvk)

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

func (c *ViewCache) GetCacheForKind(gvk schema.GroupVersionKind) (toolscache.Indexer, error) {
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

func (c *ViewCache) RegisterInformerForKind(gvk schema.GroupVersionKind) error {
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

func (c *ViewCache) GetInformerForKind(ctx context.Context, gvk schema.GroupVersionKind, _ ...cache.InformerGetOption) (cache.Informer, error) {
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

func (c *ViewCache) GetInformer(ctx context.Context, obj client.Object, opts ...cache.InformerGetOption) (cache.Informer, error) {
	gvk := obj.GetObjectKind().GroupVersionKind()
	return c.GetInformerForKind(ctx, gvk, opts...)
}

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

// Add can manually insert objects into the cache.
func (c *ViewCache) Add(obj object.Object) error {
	gvk := obj.GetObjectKind().GroupVersionKind()

	c.log.V(5).Info("add", "gvk", gvk, "key", client.ObjectKeyFromObject(obj).String(),
		"object", object.Dump(obj))

	cache, err := c.GetCacheForKind(gvk)
	if err != nil {
		return err
	}

	// cache does not apply deepcopy to stored objects
	obj = object.DeepCopy(obj)
	if err := cache.Add(obj); err != nil {
		return err
	}

	informer, err := c.GetInformerForKind(context.Background(), gvk)
	if err != nil {
		return err
	}

	informer.(*ViewCacheInformer).TriggerEvent(toolscache.Added, nil, obj, false)

	return nil
}

// Update can manually modify objects in the cache.
func (c *ViewCache) Update(oldObj, newObj object.Object) error {
	gvk := newObj.GetObjectKind().GroupVersionKind()

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

	// cache does not apply deepcopy to stored objects
	newObj = object.DeepCopy(newObj)
	if err := cache.Update(newObj); err != nil {
		return err
	}

	informer, err := c.GetInformerForKind(context.Background(), gvk)
	if err != nil {
		return err
	}
	informer.(*ViewCacheInformer).TriggerEvent(toolscache.Updated, oldObj, newObj, false)

	return nil
}

// Delete can manually remove objects from the cache.
func (c *ViewCache) Delete(obj object.Object) error {
	gvk := obj.GetObjectKind().GroupVersionKind()

	c.log.V(5).Info("delete", "gvk", gvk, "key", client.ObjectKeyFromObject(obj).String(),
		"object", object.Dump(obj))

	cache, err := c.GetCacheForKind(gvk)
	if err != nil {
		return err
	}

	// grab the object from the cache and then delete the grabbed object, this handles the
	// problems when the caller uses delete with an old object version
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
	informer.(*ViewCacheInformer).TriggerEvent(toolscache.Deleted, nil, existingObj.(object.Object), false)

	return nil
}

func (c *ViewCache) IndexField(ctx context.Context, obj client.Object, field string, extractValue client.IndexerFunc) error {
	gvk := obj.GetObjectKind().GroupVersionKind()
	c.log.Info("IndexField called on ViewCache", "gvk", gvk)
	return errors.New("field indexing is not supported for ViewCache")
}

func (c *ViewCache) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	target, ok := obj.(object.Object)
	if !ok {
		return apierrors.NewBadRequest("must be called with an object.Object")
	}

	gvk := obj.GetObjectKind().GroupVersionKind()
	cache, err := c.GetCacheForKind(gvk)
	if err != nil {
		return apierrors.NewBadRequest("invalid GVK")
	}

	c.log.V(5).Info("get", "gvk", gvk, "key", key)

	item, exists, err := cache.GetByKey(key.String())
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

func (c *ViewCache) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	listGVK := list.GetObjectKind().GroupVersionKind()
	objGVK := c.discovery.ObjectGVKFromListGVK(listGVK)
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

// Helper function to match field selectors
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
	// Initialize any resources if needed

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

func (c *ViewCache) WaitForCacheSync(_ context.Context) bool { return true }

// watcher: the thingie that is returned for callers of Watch
var _ watch.Interface = &ViewCacheWatcher{}

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

// shouldIncludeObject determines if an object should be included based on selectors
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

// matchesFieldSelector checks if object matches field selector (reuse logic from List method)
func (w *ViewCacheWatcher) matchesFieldSelector(obj object.Object, selector fields.Selector) bool {
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

func (w *ViewCacheWatcher) Stop() {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if !w.stopped {
		w.stopped = true
		close(w.stopCh)
		close(w.eventChan)
	}
}

func (w *ViewCacheWatcher) ResultChan() <-chan watch.Event {
	return w.eventChan
}

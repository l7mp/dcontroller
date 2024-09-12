package cache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"hsnlab/dcontroller-runtime/pkg/object"
)

const DefaultWatchChannelBuffer = 256

type ViewCache struct {
	mu           sync.RWMutex
	caches       map[schema.GroupVersionKind]toolscache.Indexer
	informers    map[schema.GroupVersionKind]*ViewCacheInformer
	informersMux sync.RWMutex
	logger, log  logr.Logger
}

func New(logger logr.Logger) *ViewCache {
	c := &ViewCache{
		caches:    make(map[schema.GroupVersionKind]toolscache.Indexer),
		informers: make(map[schema.GroupVersionKind]*ViewCacheInformer),
		logger:    logger,
		log:       logger.WithName("viewcache"),
	}
	return c
}

// cache handlers
func (c *ViewCache) RegisterCacheForGVK(gvk schema.GroupVersionKind) error {
	c.log.V(1).Info("registering cache for new GVK", "gvk", gvk)

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.caches[gvk]; exists {
		return fmt.Errorf("cache is already registered for GVK %s", gvk)
	}

	indexer := toolscache.NewIndexer(
		toolscache.MetaNamespaceKeyFunc,
		toolscache.Indexers{toolscache.NamespaceIndex: toolscache.MetaNamespaceIndexFunc},
	)

	c.caches[gvk] = indexer

	return nil
}

func (c *ViewCache) GetCacheForGVK(gvk schema.GroupVersionKind) (toolscache.Indexer, error) {
	c.mu.RLock()
	indexer, exists := c.caches[gvk]
	c.mu.RUnlock()

	if !exists {
		if err := c.RegisterCacheForGVK(gvk); err != nil {
			return nil, err
		}
		c.mu.RLock()
		indexer, exists = c.caches[gvk]
		c.mu.RUnlock()
	}

	if !exists {
		return nil, fmt.Errorf("cache list for GVK %s", gvk)
	}

	return indexer, nil
}

// informer handlers
func (c *ViewCache) RegisterInformerForGVK(gvk schema.GroupVersionKind) error {
	c.log.V(1).Info("registering informer for new GVK", "gvk", gvk)

	c.mu.Lock()
	cache, exists := c.caches[gvk]
	c.mu.Unlock()

	if !exists || cache == nil {
		if err := c.RegisterCacheForGVK(gvk); err != nil {
			return err
		}
		c.mu.Lock()
		cache, _ = c.caches[gvk]
		c.mu.Unlock()
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.informers[gvk]; exists {
		return fmt.Errorf("informer is already registered for GVK %s", gvk)
	}

	informer := NewViewCacheInformer(cache, c.logger)
	c.informers[gvk] = informer

	return nil
}

func (c *ViewCache) GetInformer(ctx context.Context, obj client.Object, opts ...cache.InformerGetOption) (cache.Informer, error) {
	gvk := obj.GetObjectKind().GroupVersionKind()
	return c.GetInformerForKind(ctx, gvk, opts...)
}

func (c *ViewCache) GetInformerForKind(ctx context.Context, gvk schema.GroupVersionKind, _ ...cache.InformerGetOption) (cache.Informer, error) {
	c.mu.RLock()
	informer, exists := c.informers[gvk]
	c.mu.RUnlock()

	if exists {
		return informer, nil
	}

	err := c.RegisterInformerForGVK(gvk)
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

// functions to add/update/delete objects in the cache(s)
func (c *ViewCache) Add(obj client.Object) error {
	gvk := obj.GetObjectKind().GroupVersionKind()
	cache, err := c.GetCacheForGVK(gvk)
	if err != nil {
		return err
	}

	if err := cache.Add(obj); err != nil {
		return err
	}

	informer, err := c.GetInformerForKind(context.Background(), gvk)
	if err != nil {
		return err
	}
	informer.(*ViewCacheInformer).TriggerEvent(toolscache.Added, obj)

	return nil
}

func (c *ViewCache) Update(obj client.Object) error {
	gvk := obj.GetObjectKind().GroupVersionKind()
	cache, err := c.GetCacheForGVK(gvk)
	if err != nil {
		return err
	}

	if err := cache.Update(obj); err != nil {
		return err
	}

	informer, err := c.GetInformerForKind(context.Background(), gvk)
	if err != nil {
		return err
	}
	informer.(*ViewCacheInformer).TriggerEvent(toolscache.Updated, obj)

	return nil
}

func (c *ViewCache) Delete(obj client.Object) error {
	gvk := obj.GetObjectKind().GroupVersionKind()
	cache, err := c.GetCacheForGVK(gvk)
	if err != nil {
		return err
	}

	if err := cache.Delete(obj); err != nil {
		return err
	}

	informer, err := c.GetInformerForKind(context.Background(), gvk)
	if err != nil {
		return err
	}
	informer.(*ViewCacheInformer).TriggerEvent(toolscache.Deleted, obj)

	return nil
}

// indexer is unimplemented
func (c *ViewCache) IndexField(ctx context.Context, obj client.Object, field string, extractValue client.IndexerFunc) error {
	gvk := obj.GetObjectKind().GroupVersionKind()
	c.log.Info("IndexField called on ViewCache", "gvk", gvk)
	return errors.New("field indexing is not supported for ViewCache")
}

// client.Reader
func (c *ViewCache) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	target, ok := obj.(object.Object)
	if !ok {
		return errors.New("invalid argument: call with an object.Object, not a generic client.Object")
	}

	gvk := obj.GetObjectKind().GroupVersionKind()
	c.mu.RLock()
	indexer, exists := c.caches[gvk]
	c.mu.RUnlock()

	if !exists {
		return apierrors.NewBadRequest("GVK not registered")
	}

	item, exists, err := indexer.GetByKey(key.String())
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
	gvk := list.GetObjectKind().GroupVersionKind()

	c.mu.RLock()
	indexer, exists := c.caches[gvk]
	c.mu.RUnlock()

	if !exists {
		// no problem, kist leave list empty
		return nil
	}

	for _, item := range indexer.List() {
		target, ok := item.(object.Object)
		if !ok {
			return errors.New("invalid argument: cache must store only object.Objects, not generic client.Objects")
		}
		object.AppendToListItem(list, object.DeepCopy(target))
	}

	return nil
}

// watcher
func (c *ViewCache) Watch(ctx context.Context, list client.ObjectList, opts ...client.ListOption) (watch.Interface, error) {
	gvk := list.GetObjectKind().GroupVersionKind()

	c.logger.V(2).Info("watch: adding watch", "gvk", gvk)

	informer, err := c.GetInformerForKind(ctx, gvk)
	if err != nil {
		return nil, err
	}

	watcher := &ViewCacheWatcher{
		eventChan: make(chan watch.Event, DefaultWatchChannelBuffer),
		stopCh:    make(chan struct{}),
		logger:    c.logger,
	}

	handler := toolscache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			watcher.sendEvent(watch.Added, obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			watcher.sendEvent(watch.Modified, newObj)
		},
		DeleteFunc: func(obj interface{}) {
			watcher.sendEvent(watch.Deleted, obj)
		},
	}

	handlerReg, err := informer.AddEventHandler(handler)
	if err != nil {
		return nil, fmt.Errorf("failed to add event handler: %w", err)
	}

	go func() {
		<-ctx.Done()

		c.logger.V(4).Info("stopping watcher", "gvk", gvk)

		informer.RemoveEventHandler(handlerReg)
		watcher.Stop()
	}()

	// send initial list
	cache, err := c.GetCacheForGVK(gvk)
	if err != nil {
		return nil, err
	}

	for _, item := range cache.List() {
		informer.(*ViewCacheInformer).TriggerEvent(toolscache.Added, item.(client.Object))
	}

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
	logger    logr.Logger
}

func (w *ViewCacheWatcher) sendEvent(eventType watch.EventType, obj interface{}) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.stopped {
		return
	}

	event := watch.Event{Type: eventType, Object: obj.(runtime.Object)}
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

package cache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	toolscache "k8s.io/client-go/tools/cache"

	"github.com/l7mp/dcontroller/pkg/object"
)

var _ toolscache.SharedIndexInformer = &ViewCacheInformer{}

// ViewCacheInformer is an informer for implementing watchers on the view cache.
type ViewCacheInformer struct {
	gvk            schema.GroupVersionKind // just for logging
	cache          toolscache.Indexer
	handlers       map[int64]handlerEntry
	handlerCounter int64
	mutex          sync.RWMutex
	transform      toolscache.TransformFunc
	stopped        atomic.Bool
	log            logr.Logger
}

// handlerEntry defines a handler.
type handlerEntry struct {
	toolscache.ResourceEventHandler
	id int64
}

// HasSynced return true if the informers underlying store has synced.
func (h *handlerEntry) HasSynced() bool {
	return true
}

// NewViewCacheInformer returns a new informer for the view cache.
func NewViewCacheInformer(gvk schema.GroupVersionKind, indexer toolscache.Indexer, logger logr.Logger) *ViewCacheInformer {
	if logger.GetSink() == nil {
		logger = logr.Discard()
	}

	return &ViewCacheInformer{
		gvk:      gvk,
		cache:    indexer,
		handlers: make(map[int64]handlerEntry),
		log:      logger.WithValues("GVK", gvk.String()),
	}
}

// Implement the cache.Informer interface.

// AddEventHandler adds an event handler to the shared informer.
func (c *ViewCacheInformer) AddEventHandler(handler toolscache.ResourceEventHandler) (toolscache.ResourceEventHandlerRegistration, error) {
	c.mutex.Lock()

	id := atomic.AddInt64(&c.handlerCounter, 1)
	he := handlerEntry{
		ResourceEventHandler: handler,
		id:                   id,
	}
	c.handlers[id] = he
	c.mutex.Unlock()

	c.log.V(4).Info("registering event handler: sending initial object list", "handler-id", id,
		"cache-size", len(c.cache.List()))

	// Send initial events to the newly registered handler
	for _, item := range c.cache.List() {
		obj, ok := item.(object.Object)
		if !ok {
			return nil, apierrors.NewInternalError(errors.New("cache must store object.Objects only"))
		}

		// Apply transform if needed (same logic as TriggerEvent)
		newObj := obj
		if c.transform != nil {
			newObj = object.DeepCopy(newObj)
			item, err := c.transform(newObj)
			if err != nil {
				c.log.Error(err, "Failed to transform object during initial sync")
				continue
			}

			var ok bool
			newObj, ok = item.(object.Object)
			if !ok {
				c.log.Info("transform must produce an object.Object during initial sync")
				continue
			}
		}

		// Send to the newly registered handler
		handler.OnAdd(object.DeepCopy(newObj), true)
	}

	return &he, nil
}

// AddEventHandlerWithResyncPeriod adds an event handler to the shared informer informer using the
// specified resync period.
func (c *ViewCacheInformer) AddEventHandlerWithResyncPeriod(handler toolscache.ResourceEventHandler, resyncPeriod time.Duration) (toolscache.ResourceEventHandlerRegistration, error) {
	// Ignore custom resyncPeriod as we're not actually syncing with an API server
	return c.AddEventHandler(handler)
}

// AddEventHandlerWithOptions is a variant of AddEventHandlerWithResyncPeriod where
// all optional parameters are passed in as a struct.
func (c *ViewCacheInformer) AddEventHandlerWithOptions(handler toolscache.ResourceEventHandler, _ toolscache.HandlerOptions) (toolscache.ResourceEventHandlerRegistration, error) {
	// Ignore handler options: this would be useful to change the resync period that we do not
	// need anyway and change the logger which we do not support either.
	return c.AddEventHandler(handler)
}

// RemoveEventHandler removes a previously added event handler given by its registration handle.
func (c *ViewCacheInformer) RemoveEventHandler(registration toolscache.ResourceEventHandlerRegistration) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if reg, ok := registration.(*handlerEntry); ok {
		c.log.V(4).Info("removing  event handler: ready", "handler-id", reg.id)
		delete(c.handlers, reg.id)
		return nil
	}

	return fmt.Errorf("unknown registration type")
}

// TriggerEvent will send an event on newObj of eventType to all registered handlers. Set
// isInitialList to true if event is an Added as a part of the initial object list. For all event
// types except Update events the oldObj must not be nil.
func (c *ViewCacheInformer) TriggerEvent(eventType toolscache.DeltaType, oldObj, newObj object.Object, isInitialList bool) {
	if len(c.handlers) == 0 {
		c.log.V(4).Info("suppressing event trigger: no handlers", "event", eventType,
			"object", object.Dump(newObj))
		return
	}

	c.mutex.RLock()
	defer c.mutex.RUnlock()

	c.log.V(8).Info("triggering event", "event", eventType, "object", object.Dump(newObj),
		"isInitial", isInitialList)

	if c.transform != nil {
		newObj = object.DeepCopy(newObj)

		item, err := c.transform(newObj)
		if err != nil {
			c.log.Error(err, "Failed to transform object")
			return
		}

		var ok bool
		newObj, ok = item.(object.Object)
		if !ok {
			c.log.Info("transform must produce an object.Object")
			return
		}

		c.log.V(4).Info("trigger-event: transformer ready", "object", object.Dump(newObj))
	}

	events := 0
	for _, handler := range c.handlers {
		c.log.V(8).Info("trigger-event: sending event to handler", "event", eventType,
			"object", object.Dump(newObj), "handler-id", handler.id)

		switch eventType {
		case toolscache.Added:
			handler.OnAdd(object.DeepCopy(newObj), false)
			events++
		case toolscache.Updated:
			handler.OnUpdate(oldObj, object.DeepCopy(newObj))
			events++
		case toolscache.Deleted:
			handler.OnDelete(object.DeepCopy(newObj))
			events++
		default:
			c.log.V(4).Info("trigger-event: ignoring event", "event", eventType)
		}
	}
}

// GetStore returns the informer's local cache as a Store.
func (c *ViewCacheInformer) GetStore() toolscache.Store {
	return c.cache
}

// GetIndexer returns the indexer for a view cache.
func (c *ViewCacheInformer) GetIndexer() toolscache.Indexer {
	return c.cache
}

// GetController is deprecated, it does nothing useful.
func (c *ViewCacheInformer) GetController() toolscache.Controller {
	// We don't have a real controller, so we return nil
	return nil
}

// Run starts and runs the shared informer, returning after it stops.  The informer will be stopped
// when stopCh is closed.
func (c *ViewCacheInformer) Run(stopCh <-chan struct{}) {
	defer c.stopped.Store(true)

	// We don't need to run anything continuously, just wait for the stop signal
	<-stopCh
}

// RunWithContext starts and runs the shared informer, returning after it stops. The informer will
// be stopped when the context is canceled.
func (c *ViewCacheInformer) RunWithContext(ctx context.Context) {
	defer c.stopped.Store(true)

	// We don't need to run anything continuously, just wait for the stop signal
	<-ctx.Done()
}

// HasSynced returns true if the shared informer's store has been informed by at least one full
// LIST of the authoritative state of the informer's object collection.
func (c *ViewCacheInformer) HasSynced() bool {
	// Since we're not syncing with an API server, we can consider it always synced
	return true
}

// LastSyncResourceVersion is the resource version observed when last synced with the underlying
// store.
func (c *ViewCacheInformer) LastSyncResourceVersion() string {
	// We're not tracking resource versions, so we return an empty string
	return ""
}

// AddIndexers adds more indexers to this store. This supports adding indexes after the store
// already has items.
func (c *ViewCacheInformer) AddIndexers(indexers toolscache.Indexers) error {
	return c.cache.AddIndexers(indexers)
}

// The WatchErrorHandler is called whenever ListAndWatch drops the connection with an error. After
// calling this handler, the informer will backoff and retry.
func (c *ViewCacheInformer) SetWatchErrorHandler(_ toolscache.WatchErrorHandler) error {
	c.log.Info("SetWatchErrorHandler: not implemented")
	return nil
}

// SetWatchErrorHandlerWithContext is a variant of SetWatchErrorHandler where the handler is passed
// an additional context parameter.
func (c *ViewCacheInformer) SetWatchErrorHandlerWithContext(_ toolscache.WatchErrorHandlerWithContext) error {
	c.log.Info("SetWatchErrorHandlerWithContext: not implemented")
	return nil
}

// SetTransform create a transformer with a given TransformFunc that is called for each object
// which is about to be stored.
func (c *ViewCacheInformer) SetTransform(transform toolscache.TransformFunc) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.transform = transform
	return nil
}

// IsStopped reports whether the informer has already been stopped. An informer already stopped
// will never be started again.
func (c *ViewCacheInformer) IsStopped() bool {
	return c.stopped.Load()
}

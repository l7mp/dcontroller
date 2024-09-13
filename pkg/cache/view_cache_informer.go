package cache

import (
	"fmt"
	"hsnlab/dcontroller-runtime/pkg/object"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	toolscache "k8s.io/client-go/tools/cache"
)

var _ toolscache.SharedIndexInformer = &ViewCacheInformer{}

type ViewCacheInformer struct {
	indexer        toolscache.Indexer
	handlers       map[int64]handlerEntry
	handlerCounter int64
	mutex          sync.RWMutex
	transform      toolscache.TransformFunc
	stopped        atomic.Bool
	logger         logr.Logger
}

type handlerEntry struct {
	toolscache.ResourceEventHandler
	id int64
}

func (h *handlerEntry) HasSynced() bool {
	return true
}

func NewViewCacheInformer(indexer toolscache.Indexer, logger logr.Logger) *ViewCacheInformer {
	if logger.GetSink() == nil {
		logger = logr.Discard()
	}

	return &ViewCacheInformer{
		indexer:  indexer,
		handlers: make(map[int64]handlerEntry),
		logger:   logger,
	}
}

func (c *ViewCacheInformer) AddEventHandler(handler toolscache.ResourceEventHandler) (toolscache.ResourceEventHandlerRegistration, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	id := atomic.AddInt64(&c.handlerCounter, 1)
	he := handlerEntry{
		ResourceEventHandler: handler,
		id:                   id,
	}
	c.handlers[id] = he

	c.logger.V(2).Info("add-event-handler: ready", "handler-id", id)

	return &he, nil
}

func (c *ViewCacheInformer) AddEventHandlerWithResyncPeriod(handler toolscache.ResourceEventHandler, resyncPeriod time.Duration) (toolscache.ResourceEventHandlerRegistration, error) {
	// In this implementation, we ignore the custom resyncPeriod as we're not actually syncing with an API server
	return c.AddEventHandler(handler)
}

func (c *ViewCacheInformer) RemoveEventHandler(registration toolscache.ResourceEventHandlerRegistration) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if reg, ok := registration.(*handlerEntry); ok {
		delete(c.handlers, reg.id)
		return nil
	}

	return fmt.Errorf("unknown registration type")
}

// TriggerEvent will send an event on obj of eventType to all registered handlers. Set
// isInitialList to true if event is an Added as a part of the initial object list.
func (c *ViewCacheInformer) TriggerEvent(eventType toolscache.DeltaType, obj object.Object, isInitialList bool) {
	if len(c.handlers) == 0 {
		return
	}

	c.mutex.RLock()
	defer c.mutex.RUnlock()

	c.logger.V(2).Info("trigger-event", "event", eventType, "object", object.DumpObject(obj))

	if c.transform != nil {
		obj = object.DeepCopy(obj)

		item, err := c.transform(obj)
		if err != nil {
			c.logger.Error(err, "Failed to transform object")
			return
		}

		var ok bool
		obj, ok = item.(object.Object)
		if !ok {
			c.logger.Info("transform must produce an object.Object")
			return
		}

		c.logger.V(3).Info("trigger-event: transformer ready", "object", object.DumpObject(obj))
	}

	events := 0
	for _, handler := range c.handlers {

		c.logger.V(3).Info("trigger-event: sending event to informer",
			"object", object.DumpObject(obj), "handler-id", handler.id)

		switch eventType {
		case toolscache.Added:
			handler.OnAdd(object.DeepCopy(obj), false)
			events++
		case toolscache.Updated:
			handler.OnUpdate(nil, object.DeepCopy(obj))
			events++
		case toolscache.Deleted:
			handler.OnDelete(object.DeepCopy(obj))
			events++
		}
	}

	c.logger.V(3).Info("trigger-event: ready", "event", eventType, "object", object.DumpObject(obj),
		"events-sent", events)
}

func (c *ViewCacheInformer) GetStore() toolscache.Store {
	return c.indexer
}

func (c *ViewCacheInformer) GetIndexer() toolscache.Indexer {
	return c.indexer
}

func (c *ViewCacheInformer) GetController() toolscache.Controller {
	// We don't have a real controller, so we return nil
	return nil
}

func (c *ViewCacheInformer) Run(stopCh <-chan struct{}) {
	defer c.stopped.Store(true)

	// We don't need to run anything continuously, just wait for the stop signal
	<-stopCh
}

func (c *ViewCacheInformer) HasSynced() bool {
	// Since we're not syncing with an API server, we can consider it always synced
	return true
}

func (c *ViewCacheInformer) LastSyncResourceVersion() string {
	// We're not tracking resource versions, so we return an empty string
	return ""
}

func (c *ViewCacheInformer) AddIndexers(indexers toolscache.Indexers) error {
	return c.indexer.AddIndexers(indexers)
}

func (c *ViewCacheInformer) SetWatchErrorHandler(handler toolscache.WatchErrorHandler) error {
	c.logger.Info("SetWatchErrorHandler: not impllemented")
	return nil
}

func (c *ViewCacheInformer) SetTransform(transform toolscache.TransformFunc) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.transform = transform
	return nil
}

func (c *ViewCacheInformer) IsStopped() bool {
	return c.stopped.Load()
}

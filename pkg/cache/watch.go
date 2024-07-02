package cache

import (
	"context"
	"fmt"
	"hsnlab/dcontroller-runtime/pkg/object"
	"sync"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
)

const eventChannelBuffer = 64

type cacheWatcher struct {
	result  chan watch.Event
	cancel  context.CancelFunc
	stopped bool
	sync.Mutex
}

func newCacheWatcher(cancel context.CancelFunc) *cacheWatcher {
	return &cacheWatcher{
		result: make(chan watch.Event, eventChannelBuffer),
		cancel: cancel,
	}
}

func (w *cacheWatcher) ResultChan() <-chan watch.Event {
	return w.result
}

func (w *cacheWatcher) Stop() {
	w.Lock()
	defer w.Unlock()
	if !w.stopped {
		w.stopped = true
		w.cancel()
		close(w.result)
	}
}

func (c *Cache) watch(ctx context.Context, gvk schema.GroupVersionKind) (watch.Interface, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	indexer, exists := c.caches[gvk]
	if !exists {
		return nil, fmt.Errorf("no watcher registered for GVK %q", gvk)
	}

	wCtx, cancel := context.WithCancel(ctx)
	watcher := newCacheWatcher(cancel)
	if _, ok := c.watchers[gvk]; !ok {
		c.watchers[gvk] = []*cacheWatcher{}
	}
	c.watchers[gvk] = append(c.watchers[gvk], watcher)

	go func() {
		<-wCtx.Done()
		watcher.Stop()
		c.removeWatcher(gvk, watcher)
	}()

	// send initial list
	for _, item := range indexer.List() {
		event := watch.Event{Type: watch.Added, Object: item.(*object.Object).DeepCopy()}
		select {
		case watcher.result <- event:
			// Successfully sent the event to the watcher
		default:
			// Watcher's channel is full, skip this event for this watcher
		}
	}

	return watcher, nil
}

func (c *Cache) removeWatcher(gvk schema.GroupVersionKind, watcher *cacheWatcher) {
	c.mu.Lock()
	defer c.mu.Unlock()

	watchers := c.watchers[gvk]
	for i, w := range watchers {
		if w == watcher {
			c.watchers[gvk] = append(watchers[:i], watchers[i+1:]...)
			break
		}
	}
}

func (c *Cache) notifyWatchers(gvk schema.GroupVersionKind, event watch.Event) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if watchers, ok := c.watchers[gvk]; ok {
		for _, watcher := range watchers {
			watcher.Lock()
			if !watcher.stopped {
				event.Object = event.Object.(*object.Object).DeepCopy()
				select {
				case watcher.result <- event:
					// Successfully sent the event to the watcher
				default:
					// Watcher's channel is full, skip this event for this watcher
				}
			}
			watcher.Unlock()
		}
	}
}

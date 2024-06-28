package cache

import (
	"context"
	"fmt"
	"hsnlab/dcontroller-runtime/pkg/object"

	"k8s.io/apimachinery/pkg/watch"
)

// Client provides a read-only interface to the cache.
type Client interface {
	Get(id string) (*object.Object, error)
	List() ([]*object.Object, error)
	Watch(ctx context.Context) (watch.Interface, error)
}

// clientImpl implements read-only Client.
type clientImpl struct {
	cache *Cache
}

// NewClient creates a new read-only client
func (cc *Cache) NewClient() Client {
	return &clientImpl{cache: cc}
}

func (c *clientImpl) Get(id string) (*object.Object, error) {
	obj, exists, err := c.cache.indexer.GetByKey(id)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("not found: id %q", id)
	}
	return obj.(*object.Object), nil
}

func (c *clientImpl) List() ([]*object.Object, error) {
	objs := c.cache.indexer.List()
	result := make([]*object.Object, len(objs))
	for i, obj := range objs {
		result[i] = obj.(*object.Object)
	}
	return result, nil
}

type watcher struct {
	id       int
	ch       chan watch.Event
	cache    *Cache
	cancelCh chan any
}

func (c *clientImpl) Watch(ctx context.Context) (watch.Interface, error) {
	c.cache.watchMutex.Lock()
	defer c.cache.watchMutex.Unlock()

	id := c.cache.nextID
	c.cache.nextID++
	ch := make(chan watch.Event, 100)
	c.cache.watches[id] = ch

	cancelCh := make(chan any)
	w := &watcher{
		id:       id,
		ch:       ch,
		cache:    c.cache,
		cancelCh: cancelCh,
	}

	go func() {
		<-ctx.Done()
		c.cache.watchMutex.Lock()
		defer c.cache.watchMutex.Unlock()
		delete(c.cache.watches, w.id)
		close(w.ch)
		close(w.cancelCh)
	}()

	return w, nil
}

func (w *watcher) Stop() { w.cancelCh <- struct{}{} }

func (w *watcher) ResultChan() <-chan watch.Event {
	return w.ch
}

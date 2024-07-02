package cache

import (
	"fmt"
	"sync"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"

	"hsnlab/dcontroller-runtime/pkg/object"
)

type Cache struct {
	mu       sync.RWMutex
	caches   map[schema.GroupVersionKind]cache.Indexer
	watchers map[schema.GroupVersionKind][]*cacheWatcher
}

func New() *Cache {
	return &Cache{
		caches:   make(map[schema.GroupVersionKind]cache.Indexer),
		watchers: make(map[schema.GroupVersionKind][]*cacheWatcher),
	}
}

func (c *Cache) RegisterGVK(gvk schema.GroupVersionKind) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.caches[gvk]; exists {
		return fmt.Errorf("GVK %s is already registered", gvk)
	}

	indexer := cache.NewIndexer(
		cache.MetaNamespaceKeyFunc,
		cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
	)
	c.caches[gvk] = indexer
	return nil
}

func (c *Cache) Upsert(obj *object.Object) error {
	gvk := obj.GroupVersionKind()
	c.mu.RLock()
	indexer, exists := c.caches[gvk]
	c.mu.RUnlock()

	if !exists {
		return fmt.Errorf("GVK %s is not registered", gvk)
	}

	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err != nil {
		return err
	}

	oldObj, exists, err := indexer.GetByKey(key)
	if err != nil {
		return err
	}

	err = indexer.Update(obj)
	if err != nil {
		return err
	}

	eventType := watch.Added
	if exists && !obj.DeepEqual(oldObj.(*object.Object)) {
		eventType = watch.Modified
	}

	c.notifyWatchers(gvk, watch.Event{Type: eventType, Object: obj})

	return nil
}

func (c *Cache) Delete(obj *object.Object) error {
	gvk := obj.GroupVersionKind()
	c.mu.RLock()
	indexer, exists := c.caches[gvk]
	c.mu.RUnlock()

	if !exists {
		return fmt.Errorf("GVK %s is not registered", gvk)
	}

	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err != nil {
		return err
	}

	existingObj, exists, err := indexer.GetByKey(key)
	if err != nil {
		return err
	}
	if !exists {
		return nil // Object doesn't exist, nothing to delete
	}

	err = indexer.Delete(obj)
	if err != nil {
		return err
	}

	c.notifyWatchers(gvk, watch.Event{Type: watch.Deleted, Object: existingObj.(*object.Object)})
	return nil
}

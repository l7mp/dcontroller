package cache

import (
	"context"
	"fmt"
	"sync"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"hsnlab/dcontroller-runtime/pkg/object"
)

type Cache struct {
	mu       sync.RWMutex
	caches   map[schema.GroupVersionKind]cache.Indexer
	watchers map[schema.GroupVersionKind][]*cacheWatcher
}

// new c
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

func (c *Cache) Upsert(obj object.Object) error {
	gvk := obj.GetObjectKind().GroupVersionKind()

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
	if exists && !object.DeepEqual(obj, oldObj.(object.Object)) {
		eventType = watch.Modified
	}

	c.notifyWatchers(gvk, watch.Event{Type: eventType, Object: obj})

	return nil
}

func (c *Cache) Delete(obj object.Object) error {
	gvk := obj.GetObjectKind().GroupVersionKind()

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

	c.notifyWatchers(gvk, watch.Event{Type: watch.Deleted, Object: existingObj.(object.Object)})
	return nil
}

// // fulfill cache.Cache
// fulfill client.Reader
func (c *Cache) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	return c.NewClient().Get(ctx, key, obj, opts...)
}

func (c *Cache) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	return c.NewClient().List(ctx, list, opts...)
}

// // fulfill cache.Informer
// func (c *Cache) GetInformer(ctx context.Context, obj client.Object, opts ...ctrlCache.InformerGetOption) (ctrlCache.Informer, error) {
// 	gvk := obj.GetObjectKind().GroupVersionKind()
// 	if gvk.Group != viewapiv1.GroupVersion.Group {
// 		return nil, c.Cache.GetInformer(ctx, obj, opts...)
// 	}

// }
// func (c *Cache) GetInformerForKind(ctx context.Context, gvk schema.GroupVersionKind, opts ...ctrlCache.InformerGetOption) (ctrlCache.Informer, error) {
// }
// func (c *Cache) RemoveInformer(ctx context.Context, obj client.Object) error {
// }
// func (c *Cache) Start(ctx context.Context) error {
// }
// func (c *Cache) WaitForCacheSync(ctx context.Context) bool {
// }

// // fulfill client.FieldIndexer
// func (c *Cache) IndexField(ctx context.Context, obj client.Object, field string, extractValue client.IndexerFunc) error {
// }

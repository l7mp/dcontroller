package cache

import (
	"sync"

	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"

	"hsnlab/dcontroller-runtime/pkg/object"
)

// Cache manages our custom objects
type Cache struct {
	indexer    cache.Indexer
	watches    map[int]chan watch.Event
	nextID     int
	watchMutex sync.Mutex
}

// NewCache creates a new Cache.
func NewCache() *Cache {
	return &Cache{
		indexer: cache.NewIndexer(
			func(obj interface{}) (string, error) {
				return obj.(*object.Object).GetID(), nil
			},
			cache.Indexers{},
		),
		watches: make(map[int]chan watch.Event),
	}
}

// Add adds a new object to the cache
func (cc *Cache) Add(obj *object.Object) error {
	err := cc.indexer.Add(obj)
	if err != nil {
		return err
	}
	cc.notifyWatchers(watch.Added, obj)
	return nil
}

// Update updates an existing object in the cache
func (cc *Cache) Update(obj *object.Object) error {
	err := cc.indexer.Update(obj)
	if err != nil {
		return err
	}
	cc.notifyWatchers(watch.Modified, obj)
	return nil
}

func (cc *Cache) notifyWatchers(eventType watch.EventType, obj *object.Object) {
	event := watch.Event{Type: eventType, Object: obj}
	cc.watchMutex.Lock()
	defer cc.watchMutex.Unlock()
	for _, ch := range cc.watches {
		select {
		case ch <- event:
		default:
			// Channel is full, skip this watcher
		}
	}
}

package cache

import (
	toolscache "k8s.io/client-go/tools/cache"

	"github.com/l7mp/dcontroller/pkg/object"
)

// Store is like toolscache.Store but it also deep-copies objects
type Store struct {
	Store toolscache.Store
}

func NewStore() *Store {
	return &Store{Store: toolscache.NewStore(toolscache.MetaNamespaceKeyFunc)}
}

// Add adds the given object to the database associated with the given object's key
func (s *Store) Add(obj object.Object) error { return s.Store.Add(object.DeepCopy(obj)) }

// Update updates the given object in the database associated with the given object's key
func (s *Store) Update(obj object.Object) error { return s.Store.Update(object.DeepCopy(obj)) }

// Delete deletes the given object from the database associated with the given object's key
func (s *Store) Delete(obj object.Object) error { return s.Store.Delete(obj) }

// List returns a list of all the currently non-empty databases
func (s *Store) List() []object.Object {
	res := s.Store.List()
	ret := make([]object.Object, len(res))
	for i := range res {
		ret[i] = object.DeepCopy(res[i].(object.Object))
	}
	return ret
}

// ListKeys returns a list of all the keys currently associated with non-empty databases
func (s *Store) ListKeys() []string { return s.Store.ListKeys() }

// Get returns the database associated with the given object's key
func (s *Store) Get(obj object.Object) (object.Object, bool, error) {
	item, exists, err := s.Store.Get(obj)
	if err != nil || item == nil {
		return nil, exists, err
	}
	return object.DeepCopy(item.(object.Object)), exists, err
}

// GetByKey returns the database associated with the given key
func (s *Store) GetByKey(key string) (object.Object, bool, error) {
	item, exists, err := s.Store.GetByKey(key)
	if err != nil || item == nil {
		return nil, exists, err
	}
	return object.DeepCopy(item.(object.Object)), exists, err
}

// Replace will delete the contents of the store, using instead the
// given list. Store takes ownership of the list, you should not reference
// it after calling this function.
func (s *Store) Replace(objs []object.Object, arg string) error {
	as := make([]any, len(objs))
	for i := range objs {
		as[i] = objs[i]
	}
	return s.Store.Replace(as, arg)
}

// Resync is meaningless in the terms appearing here but has
// meaning in some implementations that have non-trivial
// additional behavior (e.g., DeltaFIFO).
func (s *Store) Resync() error {
	return s.Store.Resync()
}

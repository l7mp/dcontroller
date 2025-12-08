package cache

// Composite cache is a cache that serves views from the view cache and the rest from the default
// Kubernetes cache.

import (
	"context"

	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/l7mp/dcontroller/pkg/object"
)

// Re-export controller-runtime types for convenience.
// Users can use manager.Options and manager.Manager without importing controller-runtime.
type (
	Options      = cache.Options
	Cache        = cache.Cache
	NewCacheFunc = cache.NewCacheFunc
)

// ViewCacheInterface extends cache.Cache with view-specific operations.
// Both ViewCache and DelegatingViewCache implement this interface.
type ViewCacheInterface interface {
	Cache
	// GetClient returns a client for this view cache.
	GetClient() client.WithWatch
	// Add adds an object to the cache.
	Add(obj object.Object) error
	// Update updates an object in the cache.
	Update(oldObj, newObj object.Object) error
	// Delete removes an object from the cache.
	Delete(obj object.Object) error
	// Watch watches for changes to objects.
	Watch(ctx context.Context, list client.ObjectList, opts ...client.ListOption) (watch.Interface, error)
}

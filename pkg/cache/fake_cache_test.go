package cache

// composite cache is a cache that serves views from the view cache and the rest from the default
// Kubernetes cache

import (
	"context"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/scheme"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllertest"

	"hsnlab/dcontroller-runtime/pkg/object"
)

var _ cache.Cache = &fakeInformers{}

// fakeInformers is a fake implementation of Informers. Client can store only a single object.
// Source: sigs.k8s.io/controller-runtime/pkg/cache/informertest/fake_cache.go.
type fakeInformers struct {
	InformersByGVK map[schema.GroupVersionKind]toolscache.SharedIndexInformer
	Scheme         *runtime.Scheme
	Error          error
	Synced         *bool
	Store          object.Object
}

func newFakeInformers(s *runtime.Scheme) *fakeInformers {
	if s == nil {
		s = scheme.Scheme
	}

	return &fakeInformers{
		InformersByGVK: map[schema.GroupVersionKind]toolscache.SharedIndexInformer{},
		Scheme:         s,
	}
}

// GetInformerForKind implements Informers.
func (c *fakeInformers) GetInformerForKind(ctx context.Context, gvk schema.GroupVersionKind, opts ...cache.InformerGetOption) (cache.Informer, error) {
	obj, err := c.Scheme.New(gvk)
	if err != nil {
		return nil, err
	}
	return c.informerFor(gvk, obj)
}

// GetInformer implements Informers.
func (c *fakeInformers) GetInformer(ctx context.Context, obj client.Object, opts ...cache.InformerGetOption) (cache.Informer, error) {
	if c.Scheme == nil {
		c.Scheme = scheme.Scheme
	}
	gvks, _, err := c.Scheme.ObjectKinds(obj)
	if err != nil {
		return nil, err
	}
	gvk := gvks[0]
	return c.informerFor(gvk, obj)
}

// RemoveInformer implements Informers.
func (c *fakeInformers) RemoveInformer(ctx context.Context, obj client.Object) error {
	if c.Scheme == nil {
		c.Scheme = scheme.Scheme
	}
	gvks, _, err := c.Scheme.ObjectKinds(obj)
	if err != nil {
		return err
	}
	gvk := gvks[0]
	delete(c.InformersByGVK, gvk)
	return nil
}

// WaitForCacheSync implements Informers.
func (c *fakeInformers) WaitForCacheSync(ctx context.Context) bool {
	if c.Synced == nil {
		return true
	}
	return *c.Synced
}

func (c *fakeInformers) informerFor(gvk schema.GroupVersionKind, _ runtime.Object) (toolscache.SharedIndexInformer, error) {
	if c.Error != nil {
		return nil, c.Error
	}
	if c.InformersByGVK == nil {
		c.InformersByGVK = map[schema.GroupVersionKind]toolscache.SharedIndexInformer{}
	}
	informer, ok := c.InformersByGVK[gvk]
	if ok {
		return informer, nil
	}

	c.InformersByGVK[gvk] = &controllertest.FakeInformer{}
	return c.InformersByGVK[gvk], nil
}

// Start implements Informers.
func (c *fakeInformers) Start(ctx context.Context) error {
	return c.Error
}

// IndexField implements Cache.
func (c *fakeInformers) IndexField(ctx context.Context, obj client.Object, field string, extractValue client.IndexerFunc) error {
	return nil
}

func (c *fakeInformers) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if c.Store != nil {
		object.DeepCopyInto(c.Store, obj.(object.Object))
		return nil
	}
	return apierrors.NewNotFound(schema.GroupResource{
		Group:    obj.GetObjectKind().GroupVersionKind().Group,
		Resource: obj.GetObjectKind().GroupVersionKind().Kind,
	}, key.String())
}

func (c *fakeInformers) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if c.Store != nil {
		object.AppendToListItem(list, object.DeepCopy(c.Store))
	}
	return nil
}

func (c *fakeInformers) Upsert(obj object.Object) error {
	c.Store = object.DeepCopy(obj)
	return nil
}

func (c *fakeInformers) Delete(_ object.Object) error {
	c.Store = nil
	return nil
}

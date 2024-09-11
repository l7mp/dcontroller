package manager

import (
	"context"

	"hsnlab/dcontroller-runtime/pkg/cache"

	"sigs.k8s.io/controller-runtime/pkg/api"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// special client.Reader: Get and List are wrapped so that if API group is dcontroller.github.io,
// ask the view cache, otherwise fall back to the default manager client

type cclient struct {
	client.Client
	cacheClient cache.ReadOnlyClient
}

func NewClient(m *Manager) client.Client {
	return &cclient{
		Client:      m.Manager.GetClient(),
		cacheClient: m.cache.NewClient(),
	}
}

func (c *cclient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	gvk := obj.GetObjectKind().GroupVersionKind()
	if gvk.Group == api.GroupVersion.Group {
		return c.cacheClient.Get(ctx, key, obj, opts...)
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

func (c *cclient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	gvk := list.GetObjectKind().GroupVersionKind()
	if gvk.Group == api.GroupVersion.Group {
		return c.cacheClient.List(ctx, list, opts...)
	}
	return c.Client.List(ctx, list, opts...)
}

package manager

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"

	viewapiv1 "hsnlab/dcontroller-runtime/pkg/api/view/v1"
	"hsnlab/dcontroller-runtime/pkg/cache"
)

var errInvalidViewGroup = fmt.Errorf("invalid view group or version, expecting %s", viewapiv1.GroupVersion.String())

type viewClient struct {
	cacheClient cache.ReadOnlyClient
}

// GetClient obtains a client to the local view cache. Use manager.Manager.GetClient() to obtain a
// real Kubernetes API client.
func (m *Manager) GetClient() cache.ReadOnlyClient {
	return &viewClient{
		cacheClient: m.cache.NewClient(),
	}
}

// client.Reader
func (c *viewClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	gvk := obj.GetObjectKind().GroupVersionKind()
	if gvk.Group != viewapiv1.GroupVersion.Group {
		return errInvalidViewGroup
	}
	return c.cacheClient.Get(ctx, key, obj, opts...)
}

func (c *viewClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	gvk := list.GetObjectKind().GroupVersionKind()
	if gvk.Group != viewapiv1.GroupVersion.Group {
		return errInvalidViewGroup
	}
	return c.cacheClient.List(ctx, list, opts...)
}

func (c *viewClient) Watch(ctx context.Context, list client.ObjectList, opts ...client.ListOption) (watch.Interface, error) {
	gvk := list.GetObjectKind().GroupVersionKind()
	if gvk.Group != viewapiv1.GroupVersion.Group {
		return nil, errInvalidViewGroup
	}
	return c.cacheClient.Watch(ctx, list, opts...)
}

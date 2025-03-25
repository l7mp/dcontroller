package manager

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/client-go/rest"
	ctrlCache "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	viewv1a1 "github.com/hsnlab/dcontroller/pkg/api/view/v1alpha1"
	ccache "github.com/hsnlab/dcontroller/pkg/cache"
	"github.com/hsnlab/dcontroller/pkg/object"
)

var _ client.Client = &compositeClient{}

type compositeClient struct {
	compositeCache *ccache.CompositeCache // cache client: must be set up after the client has been created!
	log            logr.Logger
	client.Client
}

// NewCompositeClient creates a composite client: views are served through the viewcache, native
// Kubernetes resources served from a native client (can be split client).
func NewCompositeClient(config *rest.Config, options client.Options) (client.Client, error) {
	defaultClient, err := client.New(config, options)
	if err != nil {
		return nil, err
	}
	return &compositeClient{Client: defaultClient, log: logr.New(nil)}, nil
}

func (c *compositeClient) setCache(cache ctrlCache.Cache) error {
	ccache, ok := cache.(*ccache.CompositeCache)
	if !ok {
		return errors.New("cache must be a composite cache")
	}
	c.compositeCache = ccache
	c.log = ccache.GetLogger().WithName("composite-client")
	return nil
}

// split client:
// client.Reader: implemented by the cache.Reader in the native manager.client
// client.Writer: views are written to the viewcache, rest handled by the default client

func (c *compositeClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	gvk := obj.GetObjectKind().GroupVersionKind()
	if gvk.Group == viewv1a1.GroupVersion.Group {
		if c.compositeCache == nil {
			return errors.New("cache is not set")
		}

		o, ok := obj.(object.Object)
		if !ok {
			return errors.New("object must be an object.Object")
		}
		return c.compositeCache.GetViewCache().Add(o)
	}
	return c.Client.Create(ctx, obj, opts...)
}

func (c *compositeClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	gvk := obj.GetObjectKind().GroupVersionKind()
	if gvk.Group == viewv1a1.GroupVersion.Group {
		if c.compositeCache == nil {
			return errors.New("cache is not set")
		}

		o, ok := obj.(object.Object)
		if !ok {
			return errors.New("object must be an object.Object")
		}
		return c.compositeCache.GetViewCache().Delete(o)
	}
	return c.Client.Delete(ctx, obj, opts...)
}

func (c *compositeClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	gvk := obj.GetObjectKind().GroupVersionKind()
	if gvk.Group == viewv1a1.GroupVersion.Group {
		if c.compositeCache == nil {
			return errors.New("cache is not set")
		}

		newObj, ok := obj.(object.Object)
		if !ok {
			return errors.New("object must be an object.Object")
		}

		// get the old object
		oldObj := object.NewViewObject(newObj.GetKind())
		if err := c.compositeCache.GetViewCache().Get(ctx, client.ObjectKeyFromObject(newObj), oldObj); err != nil {
			return fmt.Errorf("cannot update object with key %s: not in cache",
				client.ObjectKeyFromObject(newObj))
		}

		return c.compositeCache.GetViewCache().Update(oldObj, newObj)
	}
	return c.Client.Update(ctx, obj, opts...)
}

func (c *compositeClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	gvk := obj.GetObjectKind().GroupVersionKind()
	if gvk.Group == viewv1a1.GroupVersion.Group {
		if c.compositeCache == nil {
			return errors.New("cache is not set")
		}

		o, ok := obj.(object.Object)
		if !ok {
			return errors.New("object must be an object.Object")
		}

		if patch.Type() != types.JSONPatchType && patch.Type() != types.MergePatchType {
			c.log.Info("strategic merge patch not supported in views, falling back to a merge-patch")
		}

		j, err := patch.Data(obj)
		if err != nil {
			return fmt.Errorf("cannot decode JSON patch: %w", err)
		}

		newContent := map[string]any{}
		if err := json.Unmarshal(j, &newContent); err != nil {
			return fmt.Errorf("cannot parse JSON patch: %w", err)
		}

		oldObj := object.NewViewObject(gvk.Kind)
		if err := c.compositeCache.GetViewCache().Get(ctx, client.ObjectKeyFromObject(o), oldObj); err != nil {
			return err
		}

		newObj := oldObj.DeepCopy()
		if err := object.Patch(newObj, newContent); err != nil {
			return err
		}

		// copy back into obj so that caller knows the new obj
		newObj.DeepCopyInto(o)

		return c.compositeCache.GetViewCache().Update(oldObj, newObj)
	}

	return c.Client.Patch(ctx, obj, patch, opts...)
}

func (c *compositeClient) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error {
	gvk := obj.GetObjectKind().GroupVersionKind()
	if gvk.Group != viewv1a1.GroupVersion.Group {
		if c.compositeCache == nil {
			return errors.New("cache is not set")
		}

		list := object.NewViewObjectList("view")
		if err := c.compositeCache.GetViewCache().List(ctx, list); err != nil {
			return err
		}

		for _, vo := range list.Items {
			return c.compositeCache.GetViewCache().Delete(&vo)
		}
	}
	return c.Client.DeleteAllOf(ctx, obj, opts...)
}

func (c *compositeClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	return c.compositeCache.Get(ctx, key, obj, opts...)
}

func (c *compositeClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	return c.compositeCache.List(ctx, list, opts...)
}

// implement StatusClient note that normally this would not be needed since the default view-object
// client already writes the status if requested, but still needed because native objects' status
// can only be updated via the status-writer
func (c *compositeClient) Status() client.SubResourceWriter {
	return &compositeStatusClient{
		compositeCache:    c.compositeCache,
		compositeClient:   c,
		SubResourceWriter: c.Client.Status(),
	}
}

type compositeStatusClient struct {
	compositeCache  *ccache.CompositeCache
	compositeClient *compositeClient
	client.SubResourceWriter
}

func (c *compositeStatusClient) Create(ctx context.Context, obj client.Object, subResource client.Object, opts ...client.SubResourceCreateOption) error {
	gvk := subResource.GetObjectKind().GroupVersionKind()
	if gvk.Group == viewv1a1.GroupVersion.Group {
		return c.updateViewStatus(ctx, obj, subResource)
	}
	return c.SubResourceWriter.Create(ctx, obj, subResource, opts...)
}

func (c *compositeStatusClient) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	gvk := obj.GetObjectKind().GroupVersionKind()
	if gvk.Group == viewv1a1.GroupVersion.Group {
		return c.updateViewStatus(ctx, obj, obj)
	}
	return c.SubResourceWriter.Update(ctx, obj, opts...)
}

func (c *compositeStatusClient) updateViewStatus(ctx context.Context, obj client.Object, subResource client.Object) error {
	if c.compositeCache == nil {
		return errors.New("cache is not set")
	}

	o, ok := obj.(object.Object)
	if !ok {
		return errors.New("object must be an object.Object")
	}
	so, ok := subResource.(object.Object)
	if !ok {
		return errors.New("sub-resource must be an object.Object")
	}

	// load status from new object
	status, ok, err := unstructured.NestedMap(so.UnstructuredContent(), "status")
	if err != nil {
		return fmt.Errorf("cannot load status sub-resource in object argument: %w", err)
	} else if !ok {
		return errors.New("no status sub-resource in object argument")
	}

	// get object from cache, overwrite status, and update
	oldObj := object.DeepCopy(o)
	if err := c.compositeCache.Get(ctx, client.ObjectKeyFromObject(oldObj), oldObj); err != nil {
		return err
	}

	oldObj.DeepCopyInto(o)
	if err := unstructured.SetNestedMap(o.UnstructuredContent(), status, "status"); err != nil {
		return fmt.Errorf("failed to set status to sub-resource: %w", err)
	}
	return c.compositeCache.GetViewCache().Update(oldObj, o)
}

func (c *compositeStatusClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	gvk := obj.GetObjectKind().GroupVersionKind()
	if gvk.Group == viewv1a1.GroupVersion.Group {
		// fallback to the composite-cache patch implementation
		return c.compositeClient.Patch(ctx, obj, patch)
	}

	return c.SubResourceWriter.Patch(ctx, obj, patch, opts...)
}

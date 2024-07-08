package view

import (
	"context"
	"errors"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	apiv1 "hsnlab/dcontroller-runtime/pkg/api/v1"
	"hsnlab/dcontroller-runtime/pkg/cache"
	"hsnlab/dcontroller-runtime/pkg/manager"
	"hsnlab/dcontroller-runtime/pkg/object"
	"hsnlab/dcontroller-runtime/pkg/pipeline"
)

type baseView struct {
	name        string
	scheme      *runtime.Scheme
	baseGVK     schema.GroupVersionKind
	cache       *cache.Cache
	aggregation pipeline.Aggregation
	ctrlClient  client.Client
	stop        bool
}

// NewBaseView registers a base view with the manager. A base view is a given by the view name, the
// native Kubernetes object to watch, and an aggregation pipeline to process it into a view.
func NewBaseView(name string, mgr *manager.Manager, aggregation pipeline.Aggregation, obj ...client.Object) (View, error) {
	if len(obj) != 1 {
		return nil, errors.New("base view must be called with a single object")
	}

	v := &baseView{
		name:        name,
		scheme:      mgr.GetScheme(),
		cache:       mgr.GetCache(),
		aggregation: aggregation,
		ctrlClient:  mgr.Manager.GetClient(),
		stop:        false,
	}

	baseGVK, err := v.getBaseGVK(obj[0])
	if err != nil {
		return nil, err
	}
	v.baseGVK = baseGVK

	c, err := controller.New(name, mgr.Manager, controller.Options{Reconciler: v})
	if err != nil {
		return nil, err
	}

	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(v.baseGVK)
	if err := c.Watch(source.Kind(mgr.Manager.GetCache(), u, // base manager cache, not ours
		&handler.TypedEnqueueRequestForObject[*unstructured.Unstructured]{},
	)); err != nil {
		return nil, err
	}

	mgr.GetCache().RegisterGVK(apiv1.NewGVK(name))

	return v, nil
}

func (v *baseView) GetName() string {
	return v.name
}

func (v *baseView) GetCache() *cache.Cache {
	return v.cache
}

func (v *baseView) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	log := log.FromContext(ctx).WithName("view-reconciler").WithValues("view", v.name, "req", req.String())
	log.Info("reconciling")

	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(v.baseGVK)
	obj := object.New(v.name).WithName(req.Namespace, req.Name)

	if err := v.ctrlClient.Get(ctx, req.NamespacedName, u); err != nil {
		if apierrors.IsNotFound(err) {
			log.V(4).Info("deleting object from cache", "object", obj.String())
			err = v.cache.Delete(obj)
		}
		return reconcile.Result{}, err
	}

	obj.SetUnstructuredContent(u.Object)
	log.V(2).Info("new object received", "object", obj.String())

	res, err := v.aggregation.Process(obj)
	if err != nil {
		return reconcile.Result{}, err
	}
	log.V(2).Info("object processed", "result", res.String())

	if err := v.cache.Upsert(res); err != nil {
		return reconcile.Result{}, err
	}

	return reconcile.Result{}, nil
}

// helpers
func (v *baseView) getBaseGVK(obj client.Object) (schema.GroupVersionKind, error) {
	gvks, _, err := v.scheme.ObjectKinds(obj)
	if err != nil {
		return schema.GroupVersionKind{}, err
	}

	if len(gvks) == 0 {
		return schema.GroupVersionKind{}, errors.New("no GVK found for object")
	}

	return gvks[0], nil
}

// NewFakeBaseView is a base-view that is useful for testing. The function takes a variable number
// of objects that are added to the initial cache.
func NewFakeBaseView(name string, scheme *runtime.Scheme, aggregation pipeline.Aggregation, obj ...client.Object) (View, error) {
	if len(obj) == 0 {
		return nil, errors.New("base view must be called with at least one object")
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(obj...).
		Build()

	v := &baseView{
		name:        "view",
		scheme:      scheme,
		cache:       cache.New(),
		aggregation: aggregation,
		ctrlClient:  fakeClient,
		stop:        false,
	}

	baseGVK, err := v.getBaseGVK(obj[0])
	if err != nil {
		return nil, err
	}
	v.baseGVK = baseGVK

	v.cache.RegisterGVK(apiv1.NewGVK("view"))

	return v, nil
}

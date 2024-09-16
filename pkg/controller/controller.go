package view

// import (
// 	"context"
// 	"errors"

// 	"github.com/go-logr/logr"
// 	apierrors "k8s.io/apimachinery/pkg/api/errors"
// 	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
// 	"k8s.io/apimachinery/pkg/runtime"
// 	"k8s.io/apimachinery/pkg/runtime/schema"
// 	"sigs.k8s.io/controller-runtime/pkg/client"
// 	"sigs.k8s.io/controller-runtime/pkg/controller"
// 	"sigs.k8s.io/controller-runtime/pkg/handler"
// 	"sigs.k8s.io/controller-runtime/pkg/log"
// 	"sigs.k8s.io/controller-runtime/pkg/reconcile"
// 	"sigs.k8s.io/controller-runtime/pkg/source"

// 	"hsnlab/dcontroller-runtime/pkg/cache"
// 	"hsnlab/dcontroller-runtime/pkg/manager"
// 	"hsnlab/dcontroller-runtime/pkg/object"
// 	"hsnlab/dcontroller-runtime/pkg/pipeline"
// 	viewv1a1 "hsnlab/dcontroller-runtime/pkg/api/view/v1alpha1"
// )

// type View interface {
// 	GetGroupVersionKind() schema.GroupVersionKind
// 	Start(context.Context) error
// 	GetClient()
// }

// // ViewConf is the basic view definition.
// type ViewConf struct {
// 	// Name is the unique name of the view.
// 	Name string `json:"name"`
// 	// Pipeline is an aggregation pipeline the view applies to the base object(s).
// 	Pipeline pipeline.Pipeline `json:"pipeline"`
// 	// The base API resource the base view watches.
// 	Resources []ResourceRef `json:"resources"`
// }

// // implementation
// type view struct {
// 	name        string
// 	manager     *manager.Manager
// 	scheme      *runtime.Scheme
// 	pipeline    pipeline.Pipeline
// 	baseGVKs    []schema.GroupVersionKind
// 	cache       *cache.ViewCache
// 	ctrlClient  client.Client
// 	cacheClient cache.ReadOnlyClient
// 	engine      pipeline.Engine
// 	log         logr.Logger
// }

// // NewView registers a view with the manager. A view is a given by a unique name, an aggregation
// // pipeline to process the base resources into view objects, and a set of Kubernetes base resources
// // to watch.
// func NewView(name string, mgr *manager.Manager, pipeline pipeline.Pipeline, resources []ResourceRef) (View, error) {
// 	v := &view{
// 		name:        name,
// 		pipeline:    pipeline,
// 		scheme:      mgr.GetScheme(),
// 		cache:       mgr.GetCache(),
// 		cacheClient: mgr.GetClient(),
// 		log:         mgr.GetLogger(),
// 	}

// 	// sanity check
// 	if len(resources) > 1 && pipeline.Join == nil {
// 		return nil, errors.New("multi-views (views on multiple base resources) must specify a Join in the pipeline")
// 	}

// 	for _, r := range resources {
// 		gvk, err := GetGVKByGroupKind(mgr, schema.GroupKind{Group: r.Group, Kind: r.Kind})
// 		if err != nil {
// 			return nil, err
// 		}

// 		if gvk.Group == viewapiv1.GroupVersion.Group {

// 		}

// 		baseGVK, err := v.getBaseGVK(obj)
// 		if err != nil {
// 			return nil, err
// 		}
// 		v.baseGVK = baseGVK

// 		c, err := controller.New(name, mgr.Manager, controller.Options{Reconciler: v})
// 		if err != nil {
// 			return nil, err
// 		}

// 		u := &unstructured.Unstructured{}
// 		u.SetGroupVersionKind(v.baseGVK)
// 		if err := c.Watch(source.Kind(mgr.Manager.GetCache(), u, // base manager cache, not ours
// 			&handler.TypedEnqueueRequestForObject[*unstructured.Unstructured]{},
// 			predicates...,
// 		)); err != nil {
// 			return nil, err
// 		}

// 		mgr.GetCache().RegisterGVK(apiv1.NewGVK(name))

// 		v.engine = pipeline.NewDefaultEngine(name, []string{v.baseGVK.String()}, view.log)

// 	}

// 	return v, nil
// }

// func (v *view) GetGroupVersionKind() schema.GroupVersionKind {
// 	return schema.GroupVersionKind{
// 		Group:   viewapiv1.GroupVersion.Group,
// 		Kind:    view.name,
// 		Version: viewapiv1.GroupVersion.Version,
// 	}
// }

// func (v *view) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
// 	log := log.FromContext(ctx).WithName("view-reconciler").WithValues("view", v.name, "req", req.String())
// 	log.Info("reconciling")

// 	u := &unstructured.Unstructured{}
// 	u.SetGroupVersionKind(v.baseGVK)
// 	obj := object.New(v.name).WithName(req.Namespace, req.Name)

// 	if err := v.ctrlClient.Get(ctx, req.NamespacedName, u); err != nil {
// 		if apierrors.IsNotFound(err) {
// 			// delete
// 			delta, err := v.aggregation.Evaluate(v.engine, cache.Delta{
// 				Type:   cache.Deleted,
// 				Object: obj,
// 			})
// 			if err == nil {
// 				err = v.cache.Delete(delta.Object)
// 				log.V(4).Info("object deleted", "object", delta.Object.String())
// 			}
// 		}
// 		return reconcile.Result{}, err
// 	}

// 	// upsert
// 	obj.SetUnstructuredContent(u.Object)

// 	delta, err := v.aggregation.Evaluate(v.engine, cache.Delta{
// 		Type:   cache.Upserted,
// 		Object: obj,
// 	})

// 	if err := v.cache.Upsert(delta.Object); err != nil {
// 		return reconcile.Result{}, err
// 	}

// 	log.V(2).Info("object upserted", "event", delta.Type, "object", delta.Object.String())

// 	return reconcile.Result{}, nil
// }

// // helpers
// // ignore possible cache client errors
// func (v *view) getUpsertEventType(obj client.Object) cache.DeltaType {
// 	eventType := cache.Added
// 	tmpObj := object.New(v.name).WithName(obj.GetNamespace(), obj.GetName())
// 	if err := v.cacheClient.Get(context.Background(), client.ObjectKeyFromObject(obj), tmpObj); err != nil {
// 		if apierrors.IsNotFound(err) {
// 			return cache.Added
// 		}
// 		return eventType
// 	}
// 	return cache.Updated
// }

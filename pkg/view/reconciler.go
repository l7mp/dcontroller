package view

import (
	"context"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type Request struct {
	reconcile.Request
	EventType string
	GVK       schema.GroupVersionKind
}

type Reconciler struct {
	client.Client
}

type EventHandler struct{}

func (h *EventHandler) Create(ctx context.Context, evt event.CreateEvent, q workqueue.RateLimitingInterface) {
	h.enqueue(evt.Object, "Create", q)
}

func (h *EventHandler) Update(ctx context.Context, evt event.UpdateEvent, q workqueue.RateLimitingInterface) {
	h.enqueue(evt.ObjectNew, "Update", q)
}

func (h *EventHandler) Delete(ctx context.Context, evt event.DeleteEvent, q workqueue.RateLimitingInterface) {
	h.enqueue(evt.Object, "Delete", q)
}

func (h *EventHandler) Generic(ctx context.Context, evt event.GenericEvent, q workqueue.RateLimitingInterface) {
	h.enqueue(evt.Object, "Generic", q)
}

func (h *EventHandler) enqueue(obj client.Object, eventType string, q workqueue.RateLimitingInterface) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}
	q.Add(Request{
		Request: reconcile.Request{
			NamespacedName: client.ObjectKey{
				Name:      u.GetName(),
				Namespace: u.GetNamespace(),
			},
		},
		EventType: eventType,
		GVK:       u.GroupVersionKind(),
	})
}

// func (r *Reconciler) Reconcile(ctx context.Context, req Request) (reconcile.Result, error) {
// 	fmt.Printf("Reconciling: EventType=%s, GVK=%v, Name=%s, Namespace=%s\n",
// 		req.EventType, req.GVK, req.Name, req.Namespace)
// 	// Your reconciliation logic here
// 	return reconcile.Result{}, nil
// }

// func main() {
//     mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{})
//     if err != nil {
//         panic(err)
//     }

//     err = ctrl.NewControllerManagedBy(mgr).
//         For(&unstructured.Unstructured{}).
//         WithEventFilter(predicate.NewPredicateFuncs(func(object client.Object) bool {
//             // Add your filtering logic here
//             return true
//         })).
//         WatchesRawSource(
//             source.Kind(mgr.GetCache(), &unstructured.Unstructured{}),
//             &CustomEventHandler{},
//         ).
//         Complete(&CustomReconciler{Client: mgr.GetClient()})
//     if err != nil {
//         panic(err)
//     }

//     if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
//         panic(err)
//     }
// }

// func main() {
//     mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{})
//     if err != nil {
//         panic(err)
//     }

//     // Create a new controller
//     c, err := controller.New("custom-controller", mgr, controller.Options{
//         Reconciler: &CustomReconciler{Client: mgr.GetClient()},
//     })
//     if err != nil {
//         panic(err)
//     }

//     // Create a source
//     src := source.Kind(mgr.GetCache(), &unstructured.Unstructured{})

//     // Create a predicate
//     pred := predicate.NewPredicateFuncs(func(object client.Object) bool {
//         // Add your filtering logic here
//         return true
//     })

//     // Watch the source
//     err = c.Watch(src, &CustomEventHandler{}, pred)
//     if err != nil {
//         panic(err)
//     }

//     if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
//         panic(err)
//     }
// }

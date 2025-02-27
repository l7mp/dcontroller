package reconciler

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/hsnlab/dcontroller/pkg/cache"
	"github.com/hsnlab/dcontroller/pkg/util"
)

type Request struct {
	Namespace, Name string
	EventType       cache.DeltaType
	GVK             schema.GroupVersionKind
}

func (r *Request) String() string {
	return fmt.Sprintf("req:{ns:%s/name:%s/type:%s/gvk:%s}", r.Namespace, r.Name, r.EventType, r.GVK)
}

type Reconciler = reconcile.TypedReconciler[Request]

var _ handler.TypedEventHandler[client.Object, Request] = &EventHandler[client.Object]{}

type EventHandler[object client.Object] struct{ log logr.Logger }

func (h EventHandler[O]) Create(ctx context.Context, evt event.TypedCreateEvent[O], q workqueue.TypedRateLimitingInterface[Request]) {
	h.log.Info("handling Create event", "event", util.Stringify(evt))
	h.enqueue(evt.Object, cache.Added, q)
}

func (h EventHandler[O]) Update(ctx context.Context, evt event.TypedUpdateEvent[O], q workqueue.TypedRateLimitingInterface[Request]) {
	h.log.Info("handling Update event", "event", util.Stringify(evt))
	h.enqueue(evt.ObjectNew, cache.Updated, q)
}

func (h EventHandler[O]) Delete(ctx context.Context, evt event.TypedDeleteEvent[O], q workqueue.TypedRateLimitingInterface[Request]) {
	h.log.Info("handling Delete event", "event", util.Stringify(evt))
	h.enqueue(evt.Object, cache.Deleted, q)
}

func (h EventHandler[O]) Generic(ctx context.Context, evt event.TypedGenericEvent[O], q workqueue.TypedRateLimitingInterface[Request]) {
	h.log.Info("ignoring Generic event", "event", util.Stringify(evt))
}

func (h EventHandler[O]) enqueue(obj O, eventType cache.DeltaType, q workqueue.TypedRateLimitingInterface[Request]) {
	q.Add(Request{
		Name:      obj.GetName(),
		Namespace: obj.GetNamespace(),
		EventType: eventType,
		GVK:       obj.GetObjectKind().GroupVersionKind(),
	})
}

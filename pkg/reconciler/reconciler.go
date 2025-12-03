// Package reconciler provides the core reconciliation abstractions for
// Δ-controller sources and targets, implementing the bridge between
// Kubernetes resources and the controller pipeline.
//
// This package handles the complexity of watching Kubernetes resources,
// converting them to controller requests, and writing results back to
// target resources. It supports both native Kubernetes resources and
// view objects with consistent semantics.
//
// Key components:
//   - Source: Configurable watch source with label/field selector support.
//   - Target: Configurable write target with Updater/Patcher semantics.
//   - Resource: Base abstraction for Kubernetes resource types.
//   - Request: Reconciliation request with event metadata.
//
// Sources support:
//   - Multiple resource types (native Kubernetes and views).
//   - Label and field selectors for filtering.
//   - Configurable predicates for change detection.
//   - Namespace-scoped and cluster-scoped resources.
//
// Targets support:
//   - Updater: Replaces target object content completely.
//   - Patcher: Applies strategic merge patches to target objects.
//
// Example usage:
//
//	source := reconciler.NewSource(mgr, "my-op", opv1a1.Source{
//	    Resource: opv1a1.Resource{Kind: "Pod"},
//	    LabelSelector: &metav1.LabelSelector{...},
//	})
//
//	target := reconciler.NewTarget(mgr, "my-op", opv1a1.Target{
//	    Resource: opv1a1.Resource{Kind: "PodView"},
//	    Type: "Patcher",
//	})
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

	"github.com/l7mp/dcontroller/pkg/object"
	"github.com/l7mp/dcontroller/pkg/util"
)

type Reconciler = reconcile.TypedReconciler[Request]
type EventHandler[object client.Object] struct{ log logr.Logger }

var _ handler.TypedEventHandler[client.Object, Request] = &EventHandler[client.Object]{}

// Definition of a reconciliation request.
type Request struct {
	Namespace, Name string
	EventType       object.DeltaType
	GVK             schema.GroupVersionKind
	Object          object.Object // Snapshot of the object at event time (fixes TOCTOU races)
}

// String stringifies a reconciliation request.
func (r *Request) String() string {
	return fmt.Sprintf("req:{ns:%s/name:%s/type:%s/gvk:%s}", r.Namespace, r.Name, r.EventType, r.GVK)
}

// GetNamespace returns the namespace of the request.
func (r Request) GetNamespace() string {
	return r.Namespace
}

// GetName returns the name of the request.
func (r Request) GetName() string {
	return r.Name
}

// GetEventType returns the event type of the request.
func (r Request) GetEventType() object.DeltaType {
	return r.EventType
}

// GetGVK returns the GroupVersionKind of the request.
func (r Request) GetGVK() schema.GroupVersionKind {
	return r.GVK
}

// GetObject returns the object snapshot from the request.
func (r Request) GetObject() object.Object {
	return r.Object
}

// Create createa a "create" event.
func (h EventHandler[O]) Create(ctx context.Context, evt event.TypedCreateEvent[O], q workqueue.TypedRateLimitingInterface[Request]) {
	h.log.Info("handling Create event", "event", util.Stringify(evt))
	h.enqueue(evt.Object, object.Added, q)
}

// Update createa an "update" event.
func (h EventHandler[O]) Update(ctx context.Context, evt event.TypedUpdateEvent[O], q workqueue.TypedRateLimitingInterface[Request]) {
	h.log.Info("handling Update event", "event", util.Stringify(evt))
	h.enqueue(evt.ObjectNew, object.Updated, q)
}

// Delete creates a "deletion" event.
func (h EventHandler[O]) Delete(ctx context.Context, evt event.TypedDeleteEvent[O], q workqueue.TypedRateLimitingInterface[Request]) {
	h.log.Info("handling Delete event", "event", util.Stringify(evt))
	h.enqueue(evt.Object, object.Deleted, q)
}

// Generic create a generic event/
func (h EventHandler[O]) Generic(ctx context.Context, evt event.TypedGenericEvent[O], q workqueue.TypedRateLimitingInterface[Request]) {
	h.log.Info("ignoring Generic event", "event", util.Stringify(evt))
}

func (h EventHandler[O]) enqueue(obj O, eventType object.DeltaType, q workqueue.TypedRateLimitingInterface[Request]) {
	// DeepCopy the object to create a snapshot at event time.
	// This prevents TOCTOU races where the object changes between predicate check and reconciliation.
	var snapshot object.Object
	if o, ok := any(obj).(object.Object); ok {
		snapshot = object.DeepCopy(o)
	}

	q.Add(Request{
		Name:      obj.GetName(),
		Namespace: obj.GetNamespace(),
		EventType: eventType,
		GVK:       obj.GetObjectKind().GroupVersionKind(),
		Object:    snapshot,
	})
}

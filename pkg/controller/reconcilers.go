package controller

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	"github.com/l7mp/dcontroller/pkg/object"
	"github.com/l7mp/dcontroller/pkg/reconciler"
	"github.com/l7mp/dcontroller/pkg/util"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	runtimeManager "sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// IncrementalReconciler is a reconciler for incremental updates from watcher and oneshot sources.
// It processes requests directly through the pipeline.
type IncrementalReconciler struct {
	manager    runtimeManager.Manager
	controller *DeclarativeController
	log        logr.Logger
}

// NewIncrementalReconciler creates a new incremental reconciler.
func NewIncrementalReconciler(mgr runtimeManager.Manager, c *DeclarativeController) *IncrementalReconciler {
	return &IncrementalReconciler{
		manager:    mgr,
		controller: c,
		log:        mgr.GetLogger().WithName("incremental-reconciler").WithValues("name", c.name),
	}
}

// Reconcile implements the reconciler for incremental updates.
// It processes the request directly (pipeline has internal locking for thread safety).
func (r *IncrementalReconciler) Reconcile(ctx context.Context, req reconciler.Request) (reconcile.Result, error) {
	r.log.V(2).Info("processing request", "request", util.Stringify(req))

	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(req.GVK)
	obj.SetNamespace(req.Namespace)
	obj.SetName(req.Name)

	switch req.EventType {
	case object.Added, object.Updated, object.Replaced:
		if err := r.controller.mgr.GetClient().Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
			err = fmt.Errorf("object %s/%s disappeared from client for Add/Update event: %w",
				req.GVK, client.ObjectKeyFromObject(obj).String(), err)
			r.log.Error(r.controller.Push(err), "error", "request", req)
			return reconcile.Result{}, err
		}

	case object.Deleted:
		// Try to get the deleted object from the pipeline's source cache.
		// This is needed to generate the proper negative delta/zset.
		sourceCache := r.controller.pipeline.GetSourceCache(req.GVK)
		if sourceCache == nil {
			// Very weird: we got a delete event for a GVK we don't have a cache for.
			r.log.Info("ignoring Delete event for unknown source GVK", "gvk", req.GVK,
				"object", client.ObjectKeyFromObject(obj).String())
			return reconcile.Result{}, nil
		}

		d, ok, err := sourceCache.Get(obj)
		if err != nil {
			err = fmt.Errorf("failed to get object %s/%s from pipeline cache in Delete event: %w",
				req.GVK, client.ObjectKeyFromObject(obj).String(), err)
			r.log.Error(r.controller.Push(err), "error", "request", req)
			return reconcile.Result{}, err
		}
		if !ok {
			// Weird but happens: we got a delete for an object we never saw added.
			r.log.Info("ignoring Delete event for unknown object", "gvk", req.GVK,
				"object", client.ObjectKeyFromObject(obj).String())
			return reconcile.Result{}, nil
		}

		obj = d

	default:
		r.log.Info("ignoring event", "event-type", req.EventType)
		return reconcile.Result{}, nil
	}

	delta := object.Delta{
		Type:   req.EventType,
		Object: obj,
	}

	// Process the delta through the pipeline (pipeline has internal locking).
	deltas, err := r.controller.pipeline.Evaluate(delta)
	if err != nil {
		err = fmt.Errorf("error evaluating pipeline for object %s/%s: %w", req.GVK,
			client.ObjectKeyFromObject(obj), err)
		r.log.Error(r.controller.Push(err), "error", "request", req)
		return reconcile.Result{}, err
	}

	// Apply the resultant deltas.
	for _, d := range deltas {
		r.log.V(4).Info("writing delta to target", "target", r.controller.target.String(),
			"delta-type", d.Type, "object", object.Dump(d.Object))

		if err := r.controller.target.Write(ctx, d); err != nil {
			err = fmt.Errorf("cannot update target %s for delta %s: %w", req.GVK,
				d.String(), err)
			r.log.Error(r.controller.Push(err), "error", "request", req)
			return reconcile.Result{}, err
		}
	}

	return reconcile.Result{}, nil
}

// StateOfTheWorldReconciler is a reconciler for periodic sources that triggers full reconciliation.
type StateOfTheWorldReconciler struct {
	manager    runtimeManager.Manager
	controller *DeclarativeController
	log        logr.Logger
}

// NewStateOfTheWorldReconciler creates a new state-of-the-world reconciler.
func NewStateOfTheWorldReconciler(mgr runtimeManager.Manager, c *DeclarativeController) *StateOfTheWorldReconciler {
	return &StateOfTheWorldReconciler{
		manager:    mgr,
		controller: c,
		log:        mgr.GetLogger().WithName("sow-reconciler").WithValues("name", c.name),
	}
}

// Reconcile implements state-of-the-world reconciliation using pipeline.Sync().
// This is triggered by periodic sources and computes the delta between the required
// target state (based on current sources) and the actual target state, then applies
// the delta to bring the target up to date.
func (r *StateOfTheWorldReconciler) Reconcile(ctx context.Context, req reconciler.Request) (reconcile.Result, error) {
	// Call pipeline.Sync() to compute the delta needed to reconcile target state.
	deltas, err := r.controller.pipeline.Sync()
	if err != nil {
		err = fmt.Errorf("error during state-of-the-world reconciliation: %w", err)
		r.log.Error(r.controller.Push(err), "error", "request", req)
		return reconcile.Result{}, err
	}

	r.log.V(2).Info("state-of-the-world reconciliation computed deltas", "num-deltas", len(deltas))

	// Apply the deltas to the target.
	for _, d := range deltas {
		r.log.V(4).Info("writing delta to target", "target", r.controller.target.String(),
			"delta-type", d.Type, "object", object.Dump(d.Object))

		if err := r.controller.target.Write(ctx, d); err != nil {
			err = fmt.Errorf("cannot update target %s for delta %s: %w",
				r.controller.target.String(), d.String(), err)
			r.log.Error(r.controller.Push(err), "error", "request", req)
			return reconcile.Result{}, err
		}
	}

	r.log.V(1).Info("reconciliation complete", "num-deltas", len(deltas))

	return reconcile.Result{}, nil
}

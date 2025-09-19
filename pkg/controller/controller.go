// Package controller implements the core Δ-controller controller runtime that processes
// declarative pipeline specifications and manages incremental view reconciliation.
//
// This package provides the main controller abstraction that bridges declarative YAML
// specifications with imperative Go reconciliation logic. Controllers watch source Kubernetes
// resources, process them through declarative pipelines to generate view objects, and manage
// target resource updates.
//
// Key components:
//   - Controller: Main controller struct that manages the reconciliation lifecycle.
//   - StatusReporter: Handles controller status reporting and error management.
//   - ProcessorFunc: Interface for pluggable request processing logic.
//
// Controllers support:
//   - multiple source resource types with optional label selectors,
//   - declarative pipeline processing with joins and aggregations,
//   - incremental reconciliation using DBSP (Database Stream Processing),
//   - configurable error handling and status reporting, and
//   - integration with the composite client system.
//
// Example usage:
//
//	ctrl, _ := controller.New("my-controller", mgr, controller.Config{
//	    Sources: []opv1a1.Source{{Kind: "Pod"}},
//	    Target: opv1a1.Target{Kind: "PodView"},
//	    Pipeline: opv1a1.Pipeline{...},
//	})
package controller

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	runtimeManager "sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	opv1a1 "github.com/l7mp/dcontroller/pkg/api/operator/v1alpha1"
	"github.com/l7mp/dcontroller/pkg/composite"
	"github.com/l7mp/dcontroller/pkg/object"
	"github.com/l7mp/dcontroller/pkg/pipeline"
	"github.com/l7mp/dcontroller/pkg/reconciler"
	"github.com/l7mp/dcontroller/pkg/util"
)

var _ runtimeManager.Runnable = &Controller{}

// WatcherBufferSize is the default buffer size for the watch event handler.
const WatcherBufferSize int = 1024

// ProcessorFunc is the request processor type for the controller.
type ProcessorFunc func(ctx context.Context, c *Controller, req reconciler.Request) error

// Options defines the controller configuration.
type Options struct {
	// Processor allows to override the default request processor of the controller.
	Processor ProcessorFunc

	// ErrorChannel is a channel to receive errors from the controller.
	ErrorChan chan error
}

// Controller is a dcontroller reconciler.
type Controller struct {
	*errorReporter
	name, kind, op string
	config         opv1a1.Controller
	sources        []reconciler.Source
	cache          map[schema.GroupVersionKind]*composite.Store // needed to recover deleted objects
	target         reconciler.Target
	mgr            runtimeManager.Manager
	watcher        chan reconciler.Request
	pipeline       pipeline.Evaluator
	processor      ProcessorFunc
	logger, log    logr.Logger
}

// New registers a new controller for an operator, given by the source resource(s) the controller
// watches, a target resource the controller sends its output, and a processing pipeline to process
// the base resources into target resources.
func New(mgr runtimeManager.Manager, operator string, config opv1a1.Controller, opts Options) (*Controller, error) {
	logger := mgr.GetLogger()
	if logger.GetSink() == nil {
		logger = logr.Discard()
	}

	c := &Controller{
		mgr:     mgr,
		op:      operator,
		sources: []reconciler.Source{},
		cache:   make(map[schema.GroupVersionKind]*composite.Store),
		config:  config,
		watcher: make(chan reconciler.Request, WatcherBufferSize),
		logger:  logger,
	}
	c.errorReporter = NewErrorReporter(c, opts.ErrorChan)

	name := config.Name
	if name == "" {
		return c, c.PushCriticalError("invalid controller configuration: empty name")
	}
	c.name = name
	c.log = logger.WithName("controller").WithValues("name", name)

	if len(config.Sources) == 0 {
		return c, c.PushCriticalError("invalid controller configuration: no source")
	}

	emptyTarget := opv1a1.Target{}
	if config.Target == emptyTarget {
		return c, c.PushCriticalError("invalid controller configuration: no target")
	}

	processor := processRequest
	if opts.Processor != nil {
		processor = opts.Processor
	}
	c.processor = processor

	// Create the target.
	c.kind = config.Target.Kind // the kind of the target
	c.target = reconciler.NewTarget(mgr, c.op, config.Target)

	// Create the reconciler.
	controllerReconciler := NewControllerReconciler(mgr, c)

	// Create the sources and the cache.
	srcs := []string{}
	for _, s := range config.Sources {
		source := reconciler.NewSource(mgr, c.op, s)
		c.sources = append(c.sources, source)
		srcs = append(srcs, source.String())
	}

	c.log.V(4).Info("creating", "sources", fmt.Sprintf("[%s]", strings.Join(srcs, ",")))

	on := true
	baseviews := []schema.GroupVersionKind{}
	for _, s := range c.sources {
		gvk, err := s.GetGVK()
		if err != nil {
			return c, c.PushCriticalErrorf("failed to obtain GVK for source %s: %w",
				util.Stringify(s), err)
		}

		// Init the cache.
		c.cache[gvk] = composite.NewStore()

		// Create the controller.
		ctrl, err := controller.NewTyped(name, mgr, controller.TypedOptions[reconciler.Request]{
			SkipNameValidation: &on,
			Reconciler:         controllerReconciler,
		})
		if err != nil {
			return c, c.PushCriticalErrorf("failed to create runtime controller "+
				"for resource %s: %w", gvk.String(), err)
		}

		// Set up the watch.
		src, err := s.GetSource()
		if err != nil {
			return c, c.PushCriticalErrorf("failed to create runtime source for "+
				"resource %s: %w", gvk.String(), err)
		}

		// Create the watch for the source.
		if err := ctrl.Watch(src); err != nil {
			return c, c.PushCriticalErrorf("failed to watch resource %s: %w",
				gvk.String(), err)
		}

		c.log.V(4).Info("watching resource", "GVK", s.String())

		baseviews = append(baseviews, gvk)
	}

	// Create the pipeline.
	pipeline, err := pipeline.New(c.op, c.kind, baseviews, c.config.Pipeline,
		logger.WithName("pipeline").WithValues("controller", c.name, "target-kind", c.kind))
	if err != nil {
		return c, c.PushCriticalErrorf("failed to create pipleline for controller %s: %w",
			c.name, err)
	}
	c.pipeline = pipeline

	// Add the controller to the manager (this will automatically start it when Start is called
	// on the manager, but the reconciler must still be explicitly started).
	if err := mgr.Add(c); err != nil {
		return c, c.PushCriticalErrorf("failed to schedule controller %s: %w",
			c.name, err)
	}

	c.log.Info("controller ready", "sources", fmt.Sprintf("[%s]", strings.Join(srcs, ",")),
		"pipeline", c.pipeline.String(), "target", c.target.String(),
		"errors", strings.Join(c.ReportErrors(), ","))

	return c, nil
}

// GetName returns the name of the controller.
func (c *Controller) GetName() string { return c.name }

// GetWatcher returns the channel that multiplexes the requests coming from the base resources.
func (c *Controller) GetWatcher() chan reconciler.Request { return c.watcher }

// SetPipeline overrides the pipeline of the controller. Useful for adding a custom pipeline to a controller.
func (c *Controller) SetPipeline(pipeline pipeline.Evaluator) { c.pipeline = pipeline }

// ReportErrors returns a short report on the error stack of the controller.
func (c *Controller) ReportErrors() []string { return c.Report() }

// GetGVKs returns the GVKs of the views registered with the controller.
func (c *Controller) GetGVKs() []schema.GroupVersionKind {
	gvks := []schema.GroupVersionKind{}
	if c.target != nil {
		gvk, err := c.target.GetGVK()
		if err == nil {
			gvks = append(gvks, gvk)
		}
	}
	for _, src := range c.sources {
		gvk, err := src.GetGVK()
		if err == nil {
			gvks = append(gvks, gvk)
		}
	}

	return gvks
}

// Start starts running the controller. The Start function blocks until the context is closed or an
// error occurs, and it will stop running when the context is closed.
func (c *Controller) Start(ctx context.Context) error {
	c.log.Info("starting")

	defer close(c.watcher)
	for {
		select {
		case req := <-c.watcher:
			c.log.V(2).Info("processing request", "request", util.Stringify(req))

			if err := c.processor(ctx, c, req); err != nil {
				err = fmt.Errorf("error processing watch event: %w", err)
				c.log.Error(c.Push(err), "error", "request", req)
			}
		case <-ctx.Done():
			c.log.V(2).Info("controller terminating")
			return nil
		}
	}
}

// GetStatus returns the status of the controller.
func (c *Controller) GetStatus(gen int64) opv1a1.ControllerStatus {
	status := opv1a1.ControllerStatus{Name: c.name}

	var condition metav1.Condition
	switch {
	case c.IsEmpty():
		condition = metav1.Condition{
			Type:               string(opv1a1.ControllerConditionReady),
			Status:             metav1.ConditionTrue,
			ObservedGeneration: gen,
			LastTransitionTime: metav1.Now(),
			Reason:             string(opv1a1.ControllerReasonReady),
			Message:            "Controller is up and running",
		}
	case c.HasCritical():
		condition = metav1.Condition{
			Type:               string(opv1a1.ControllerConditionReady),
			Status:             metav1.ConditionFalse,
			ObservedGeneration: gen,
			LastTransitionTime: metav1.Now(),
			Reason:             string(opv1a1.ControllerReasonNotReady),
			Message:            "Controller failed to start due to a critcal error",
		}
	default:
		condition = metav1.Condition{
			Type:               string(opv1a1.ControllerConditionReady),
			Status:             metav1.ConditionUnknown,
			ObservedGeneration: gen,
			LastTransitionTime: metav1.Now(),
			Reason:             string(opv1a1.ControllerReasonReconciliationFailed),
			Message:            "Controller seems functional but there were reconciliation errors",
		}
	}

	conditions := []metav1.Condition{condition}
	status.Conditions = conditions

	status.LastErrors = c.Report()

	return status
}

// processRequest processes a reconcile request.
func processRequest(ctx context.Context, c *Controller, req reconciler.Request) error {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(req.GVK)
	obj.SetNamespace(req.Namespace)
	obj.SetName(req.Name)

	switch req.EventType {
	case object.Added, object.Updated, object.Replaced:
		if err := c.mgr.GetClient().Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
			return fmt.Errorf("object %s/%s disappeared from client for Add/Update event: %w",
				req.GVK, client.ObjectKeyFromObject(obj).String(), err)
		}
		if err := c.cache[req.GVK].Add(obj); err != nil {
			return fmt.Errorf("failed to add object %s/%s to cache: %w",
				req.GVK, client.ObjectKeyFromObject(obj).String(), err)
		}
	case object.Deleted:
		d, ok, err := c.cache[req.GVK].Get(obj)
		if err != nil {
			return fmt.Errorf("failed to get object %s/%s to cache in Delete event: %w",
				req.GVK, client.ObjectKeyFromObject(obj).String(), err)
		}
		if !ok {
			c.log.Info("ignoring Delete event for unknown object %s/%s",
				req.GVK, client.ObjectKeyFromObject(obj).String())
		}
		if err := c.cache[req.GVK].Delete(obj); err != nil {
			return fmt.Errorf("failed to delete object %s/%s from cache: %w",
				req.GVK, client.ObjectKeyFromObject(obj).String(), err)
		}

		obj = d
	default:
		c.log.Info("ignoring event %s", req.EventType)
		return nil
	}

	delta := object.Delta{
		Type:   req.EventType,
		Object: obj,
	}

	// Process the delta through the pipeline.
	deltas, err := c.pipeline.Evaluate(delta)
	if err != nil {
		return fmt.Errorf("error evaluating pipeline for object %s/%s: %w", req.GVK,
			client.ObjectKeyFromObject(obj), err)
	}

	// Apply the resultant deltas.
	for _, d := range deltas {
		c.log.V(4).Info("writing delta to target", "target", c.target.String(),
			"delta-type", d.Type, "object", object.Dump(d.Object))

		if err := c.target.Write(ctx, d); err != nil {
			return fmt.Errorf("cannot update target %s for delta %s: %w", req.GVK,
				d.String(), err)
		}
	}

	return nil
}

// ControllerReconciler is a generic reconciler that feeds the requests received from any of the
// sources into the controller processor pipeline.
type ControllerReconciler struct {
	manager runtimeManager.Manager
	watcher chan reconciler.Request
	log     logr.Logger
}

// NewControllerReconciler creates a new generic reconciler.
func NewControllerReconciler(mgr runtimeManager.Manager, c *Controller) *ControllerReconciler {
	return &ControllerReconciler{
		manager: mgr,
		watcher: c.watcher,
		log:     mgr.GetLogger().WithName("reconciler").WithValues("name", c.name),
	}
}

// Reconcile implements the reconciler.
func (r *ControllerReconciler) Reconcile(ctx context.Context, req reconciler.Request) (reconcile.Result, error) {
	r.log.V(4).Info("reconcile", "request", req)
	r.watcher <- req
	return reconcile.Result{}, nil
}

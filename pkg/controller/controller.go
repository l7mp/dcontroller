// Package controller implements the core Δ-controller controller runtime that processes
// declarative pipeline specifications and manages incremental view reconciliation.
//
// This package provides the main controller abstraction that bridges declarative YAML
// specifications with imperative Go reconciliation logic. Controllers watch source Kubernetes
// resources, process them through declarative pipelines to generate view objects, and manage
// target resource updates.
//
// Controllers support:
//   - multiple source resource types with optional label selectors,
//   - declarative pipeline processing with joins and aggregations,
//   - incremental reconciliation using DBSP,
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
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	runtimeManager "sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	opv1a1 "github.com/l7mp/dcontroller/pkg/api/operator/v1alpha1"
	"github.com/l7mp/dcontroller/pkg/pipeline"
	"github.com/l7mp/dcontroller/pkg/reconciler"
	"github.com/l7mp/dcontroller/pkg/util"
)

// Options defines the controller configuration.
type Options struct {
	// ErrorChannel is a channel to receive errors from the controller.
	ErrorChan chan error
}

// Controller is a dcontroller reconciler.
type Controller struct {
	*errorReporter
	name, op    string
	config      opv1a1.Controller
	sources     []reconciler.Source
	target      reconciler.Target
	mgr         runtimeManager.Manager
	pipeline    pipeline.Evaluator
	logger, log logr.Logger
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
		config:  config,
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

	// Create the target.
	c.target = reconciler.NewTarget(mgr, c.op, config.Target)
	targetGVK, err := c.target.GetGVK()
	if err != nil {
		return c, c.PushCriticalErrorf("invalid target: %w", err)
	}

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

		// Choose reconciler based on source type
		var rec reconcile.TypedReconciler[reconciler.Request]
		if s.Type() == opv1a1.Periodic {
			// State-of-the-world reconciler for periodic sources.
			rec = NewStateOfTheWorldReconciler(mgr, c)
		} else {
			// Incremental reconciler for watcher and oneshot sources.
			rec = NewIncrementalReconciler(mgr, c)
			// Only incremental sources flow through the pipeline.
			baseviews = append(baseviews, gvk)
		}

		// Create the controller.
		ctrl, err := controller.NewTyped(name, mgr, controller.TypedOptions[reconciler.Request]{
			SkipNameValidation: &on,
			Reconciler:         rec,
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
	}

	// Create the pipeline.
	pipeline, err := pipeline.New(c.op, targetGVK, baseviews, c.config.Pipeline,
		logger.WithName("pipeline").WithValues("controller", c.name, "target", targetGVK.String()))
	if err != nil {
		return c, c.PushCriticalErrorf("failed to create pipleline for controller %s: %w",
			c.name, err)
	}
	c.pipeline = pipeline

	c.log.Info("controller ready", "sources", fmt.Sprintf("[%s]", strings.Join(srcs, ",")),
		"pipeline", c.pipeline.String(), "target", c.target.String(),
		"errors", strings.Join(c.ReportErrors(), ","))

	return c, nil
}

// GetName returns the name of the controller.
func (c *Controller) GetName() string { return c.name }

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

package controller

import (
	"context"
	"errors"
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

	opv1a1 "hsnlab/dcontroller/pkg/api/operator/v1alpha1"
	"hsnlab/dcontroller/pkg/cache"
	"hsnlab/dcontroller/pkg/object"
	"hsnlab/dcontroller/pkg/pipeline"
	"hsnlab/dcontroller/pkg/util"
)

const WatcherBufferSize int = 1024

type ProcessorFunc func(ctx context.Context, c *Controller, req Request) error
type Options struct {
	// Processor allows to override the default request processor of the controller.
	Processor ProcessorFunc
}

var _ runtimeManager.Runnable = &Controller{}

// implementation
type Controller struct {
	name, kind       string
	config           opv1a1.Controller
	sources          []Source
	target           Target
	mgr              runtimeManager.Manager
	watcher          chan Request
	engine           pipeline.Engine
	processor        ProcessorFunc
	invalidR, readyR ErrorReporter
	logger, log      logr.Logger
}

// New registers a new controller given by the source resource(s) the controller watches, a target
// resource the controller sends its output, and a processing pipeline to process the base
// resources into target resources.
func New(mgr runtimeManager.Manager, config opv1a1.Controller, opts Options) (*Controller, error) {
	invalidR, readyR := NewErrorReporter(), NewErrorReporter()

	// sanity check
	if len(config.Sources) == 0 {
		return nil, invalidR.Push(errors.New("no source"))
	}

	emptyTarget := opv1a1.Target{}
	if config.Target == emptyTarget {
		return nil, invalidR.Push(errors.New("no target"))
	}

	if len(config.Sources) > 1 && config.Pipeline.Join == nil {
		return nil, invalidR.Push(errors.New("controllers defined on multiple base resources " +
			"must specify a Join in the pipeline"))
	}

	// opts
	processor := processRequest
	if opts.Processor != nil {
		processor = opts.Processor
	}

	name := config.Name
	if name == "" {
		return nil, invalidR.Push(errors.New("empty name in controller config"))
	}

	logger := mgr.GetLogger()
	if logger.GetSink() == nil {
		logger = logr.Discard()
	}

	c := &Controller{
		name:      name,
		kind:      config.Target.Resource.Kind, // the kind of the target
		mgr:       mgr,
		target:    NewTarget(mgr, config.Target),
		sources:   []Source{},
		config:    config,
		watcher:   make(chan Request, WatcherBufferSize),
		processor: processor,
		invalidR:  invalidR,
		readyR:    readyR,
		logger:    logger,
		log:       logger.WithName("controller").WithValues("name", name),
	}

	// Create the reconciler
	reconciler := NewControllerReconciler(mgr, c)

	srcs := []string{}
	for _, s := range config.Sources {
		source := NewSource(mgr, s)
		c.sources = append(c.sources, source)
		srcs = append(srcs, source.String())
	}
	c.log.Info("creating", "sources", fmt.Sprintf("[%s]", strings.Join(srcs, ",")))

	on := true
	baseviews := []schema.GroupVersionKind{}
	for _, s := range c.sources {
		gvk, err := s.GetGVK()
		if err != nil {
			return nil, readyR.Push(fmt.Errorf("failed to obtain GVK for source %s: %w",
				util.Stringify(s), err))
		}

		// Create the controller
		ctrl, err := controller.NewTyped(name, mgr, controller.TypedOptions[Request]{
			SkipNameValidation: &on,
			Reconciler:         reconciler,
		})
		if err != nil {
			return nil, readyR.Push(fmt.Errorf("failed to create runtime controller for resource %s: %w",
				gvk.String(), err))
		}

		// Set up the watch
		src, err := s.GetSource()
		if err != nil {
			return nil, readyR.Push(fmt.Errorf("failed to create runtime source for resource %s: %w",
				gvk.String(), err))
		}

		if err := ctrl.Watch(src); err != nil {
			return nil, readyR.Push(fmt.Errorf("failed to watch resource %s: %w",
				gvk.String(), err))
		}

		c.log.V(2).Info("watching resource", "GVK", s.String())

		baseviews = append(baseviews, gvk)
	}

	c.engine = pipeline.NewDefaultEngine(c.kind, baseviews,
		logger.WithName("pipeline").WithValues("controller", c.name, "kind/view", c.kind))

	if err := mgr.Add(c); err != nil {
		return nil, readyR.Push(fmt.Errorf("failed to schedule controller %s: %w",
			c.name, err))
	}

	return c, nil
}

func (c *Controller) GetName() string { return c.name }

// GetWatcher returns the channel that multiplexes the requests coming from the base resources.
func (c *Controller) GetWatcher() chan Request { return c.watcher }

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
				c.log.Error(c.readyR.Push(err), "error", "request", req)
			}
		case <-ctx.Done():
			c.log.V(2).Info("controller terminating")
			return nil
		}
	}
}

func (c *Controller) GetStatus(gen int64) opv1a1.ControllerStatus {
	status := opv1a1.ControllerStatus{Name: c.name}

	acceptedCondition := metav1.Condition{}
	if c.invalidR.IsEmpty() {
		acceptedCondition = metav1.Condition{
			Type:               string(opv1a1.ControllerConditionAccepted),
			Status:             metav1.ConditionTrue,
			ObservedGeneration: gen,
			LastTransitionTime: metav1.Now(),
			Reason:             string(opv1a1.ControllerReasonAccepted),
			Message:            fmt.Sprintf("Controller accpepted"),
		}
	} else {
		acceptedCondition = metav1.Condition{
			Type:               string(opv1a1.ControllerConditionAccepted),
			Status:             metav1.ConditionFalse,
			ObservedGeneration: gen,
			LastTransitionTime: metav1.Now(),
			Reason:             string(opv1a1.ControllerReasonInvalid),
			Message: fmt.Sprintf(fmt.Sprintf("Controller rejected: %s",
				c.invalidR.Top().Error())),
		}
	}

	readyCondition := metav1.Condition{}
	if c.readyR.IsEmpty() {
		readyCondition = metav1.Condition{
			Type:               string(opv1a1.ControllerConditionReady),
			Status:             metav1.ConditionTrue,
			ObservedGeneration: gen,
			LastTransitionTime: metav1.Now(),
			Reason:             string(opv1a1.ControllerReasonReady),
			Message:            fmt.Sprintf("Controller is up and running"),
		}
	} else {
		readyCondition = metav1.Condition{
			Type:               string(opv1a1.ControllerConditionReady),
			Status:             metav1.ConditionFalse,
			ObservedGeneration: gen,
			LastTransitionTime: metav1.Now(),
			Reason:             string(opv1a1.ControllerReasonNotReady),
			Message: fmt.Sprintf(fmt.Sprintf("Reconciliation error: %s",
				c.readyR.Top().Error())),
		}
	}

	conditions := []metav1.Condition{acceptedCondition, readyCondition}
	status.Conditions = conditions

	return status
}

func processRequest(ctx context.Context, c *Controller, req Request) error {
	// Obtain the requested object
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(req.GVK)
	obj.SetNamespace(req.Namespace)
	obj.SetName(req.Name)

	if req.EventType == cache.Added || req.EventType == cache.Updated || req.EventType == cache.Replaced {
		if err := c.mgr.GetClient().Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
			return c.readyR.Push(fmt.Errorf("object %s/%s disappeared for Add/Update event: %w",
				req.GVK, client.ObjectKeyFromObject(obj), err))
		}
	}
	delta := cache.Delta{
		Type:   req.EventType,
		Object: obj,
	}

	// Process the delta through the pipeline
	deltas, err := c.config.Pipeline.Evaluate(c.engine, delta)
	if err != nil {
		return c.readyR.Push(fmt.Errorf("error evaluating pipeline for object %s/%s: %w",
			req.GVK, client.ObjectKeyFromObject(obj), err))
	}

	// Apply the resultant deltas
	for _, d := range deltas {
		c.log.V(4).Info("writing delta to target", "target", c.target.String(),
			"delta-type", d.Type, "object", object.Dump(delta.Object))

		if err := c.target.Write(ctx, d); err != nil {
			return c.readyR.Push(fmt.Errorf("cannot update target %s for delta %s: %w",
				req.GVK, d.String(), err))
		}
	}

	return nil
}

// ControllerReconciler is a generic reconciler that feeds the requests received from any of the
// sources into the controller processor pipeline.
type ControllerReconciler struct {
	manager runtimeManager.Manager
	watcher chan Request
	log     logr.Logger
}

func NewControllerReconciler(mgr runtimeManager.Manager, c *Controller) *ControllerReconciler {
	return &ControllerReconciler{
		manager: mgr,
		watcher: c.watcher,
		log:     mgr.GetLogger().WithName("reconciler").WithValues("name", c.name),
	}
}

func (r *ControllerReconciler) Reconcile(ctx context.Context, req Request) (reconcile.Result, error) {
	r.log.V(4).Info("reconcile", "request", req)
	r.watcher <- req
	return reconcile.Result{}, nil
}

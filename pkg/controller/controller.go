package view

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	runtimeManager "sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"hsnlab/dcontroller-runtime/pkg/cache"
	"hsnlab/dcontroller-runtime/pkg/pipeline"
	"hsnlab/dcontroller-runtime/pkg/util"
)

const WatcherBufferSize int = 1024

// ViewConf is the basic view definition.
type Config struct {
	// The base resource(s) the controller watches.
	Sources []Source `json:"sources"`
	// Pipeline is an aggregation pipeline applied to base objects.
	Pipeline pipeline.Pipeline `json:"pipeline"`
	// The target resource the results are to be added.
	Target Target `json:"target"`
}

type ProcessorFunc func(ctx context.Context, c *Controller, req Request) error
type Options struct {
	// Processor allows to override the default request processor of the controller.
	Processor ProcessorFunc
}

var _ runtimeManager.Runnable = &Controller{}

// implementation
type Controller struct {
	name        string
	config      Config
	manager     runtimeManager.Manager
	watcher     chan Request
	engine      pipeline.Engine
	processor   ProcessorFunc
	logger, log logr.Logger
}

// New registers a new controller with the manager. A controller is given by the source resource(s) the controller watches, an
// processing pipeline to process the base resources, and a target resource the processing result is applied to.
func New(mgr runtimeManager.Manager, config Config, opts Options) (*Controller, error) {
	name := config.Target.Resource.String(mgr)

	processor := processRequest
	if opts.Processor != nil {
		processor = opts.Processor
	}
	// sanity check
	if len(config.Sources) > 1 && config.Pipeline.Join == nil {
		return nil, errors.New("controllers defined on multiple base resources must specify a Join in the pipeline")
	}

	logger := mgr.GetLogger()
	if logger.GetSink() == nil {
		logger = logr.Discard()
	}

	c := &Controller{
		name:      name,
		manager:   mgr,
		config:    config,
		watcher:   make(chan Request, WatcherBufferSize),
		processor: processor,
		logger:    logger,
		log:       logger.WithName("controller").WithValues("name", name),
	}

	srcs := []string{}
	for _, s := range config.Sources {
		srcs = append(srcs, s.Resource.String(mgr))
	}
	c.log.Info("creating", "sources", fmt.Sprintf("[%s]", strings.Join(srcs, ",")))

	on := true
	baseviews := []schema.GroupVersionKind{}
	for i := range config.Sources {
		s := &config.Sources[i]
		gvk, err := s.GetGVK(mgr)
		if err != nil {
			return nil, fmt.Errorf("controller: cannot obtain GVK for source %s: %w",
				util.Stringify(s), err)
		}

		// Create the reconciler
		reconciler := NewControllerReconciler(mgr, s, c.watcher)

		// Create the controller
		// c, err := controller.NewTyped(name, mgr.Manager, controller.TypedOptions[Request]{
		ctrl, err := controller.NewTyped(name, mgr, controller.TypedOptions[Request]{
			SkipNameValidation: &on,
			Reconciler:         reconciler,
		})
		if err != nil {
			return nil, fmt.Errorf("controller: cannot create runtime controller for resource %s: %w",
				gvk.String(), err)
		}

		// Set up the watch
		src, err := NewSource(mgr, s).GetSource()
		if err != nil {
			return nil, fmt.Errorf("controller: cannot create runtime source for resource %s: %w",
				gvk.String(), err)
		}

		if err := ctrl.Watch(src); err != nil {
			return nil, fmt.Errorf("controller: cannot watch resource %s: %w",
				gvk.String(), err)
		}

		c.log.V(2).Info("watching resource", "GVK", s.String(mgr))

		baseviews = append(baseviews, gvk)
	}

	c.engine = pipeline.NewDefaultEngine(name, baseviews, logger)

	if err := mgr.Add(c); err != nil {
		return nil, fmt.Errorf("controller: cannot schedule controller %s: %w",
			c.name, err)
	}

	return c, nil
}

func (c *Controller) GetName() string { return c.name }

// GetWatcher returns the channel that multiplexes the requests coming from the base resources.
func (c *Controller) GetWatcher() chan Request { return c.watcher }

func (c *Controller) Start(ctx context.Context) error {
	c.log.Info("starting")

	// set up the watcher
	go func() {
		defer close(c.watcher)
		for {
			select {
			case req := <-c.watcher:
				c.log.V(2).Info("processing request", "request", util.Stringify(req))

				if err := c.processor(ctx, c, req); err != nil {
					c.log.Info("error processing watch event", "request", req,
						"error", err.Error())
				}
			case <-ctx.Done():
				c.log.V(2).Info("controller terminating")
				return
			}
		}
	}()

	return nil
}

func processRequest(ctx context.Context, c *Controller, req Request) error {
	// Obtain the requested object
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(req.GVK)
	obj.SetNamespace(req.Namespace)
	obj.SetName(req.Name)

	if req.EventType == cache.Added || req.EventType == cache.Updated || req.EventType == cache.Replaced {
		if err := c.manager.GetClient().Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
			return fmt.Errorf("controller: object %s/%s disappeared for Add/Update event: %w",
				req.GVK, client.ObjectKeyFromObject(obj), err)
		}
	}
	delta := cache.Delta{
		Type:   req.EventType,
		Object: obj,
	}

	// Process the delta through the pipeline
	deltas, err := c.config.Pipeline.Evaluate(c.engine, delta)
	if err != nil {
		return fmt.Errorf("controller: error evaluating pipeline for object %s/%s: %w",
			req.GVK, client.ObjectKeyFromObject(obj), err)
	}

	// Apply the resultant deltas
	target := NewTarget(c.manager, &c.config.Target)
	for _, d := range deltas {
		if err := target.Write(ctx, d); err != nil {
			return fmt.Errorf("controller: cannot update target %s for delta %s: %w",
				req.GVK, d.String(), err)
		}
	}

	return nil
}

type ContrllerReconciler struct {
	name    string
	manager runtimeManager.Manager
	watcher chan Request
	log     logr.Logger
}

func NewControllerReconciler(mgr runtimeManager.Manager, s *Source, watcher chan Request) *ContrllerReconciler {
	name := s.String(mgr)
	return &ContrllerReconciler{
		manager: mgr,
		name:    name,
		watcher: watcher,
		log:     mgr.GetLogger().WithName("reconciler").WithValues("object", name),
	}
}

func (r *ContrllerReconciler) Reconcile(ctx context.Context, req Request) (reconcile.Result, error) {
	r.log.V(4).Info("reconcile", "request", req)
	r.watcher <- req
	return reconcile.Result{}, nil
}

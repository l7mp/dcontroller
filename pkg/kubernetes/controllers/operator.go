// Package controllers implements the Kubernetes controllers used to reconcile the CRD resources.
package controllers

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/retry"
	runtimeCache "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	runtimeCtrl "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	opv1a1 "github.com/l7mp/dcontroller/pkg/api/operator/v1alpha1"
	"github.com/l7mp/dcontroller/pkg/apiserver"
	"github.com/l7mp/dcontroller/pkg/composite"
	"github.com/l7mp/dcontroller/pkg/controller"
	"github.com/l7mp/dcontroller/pkg/manager"
	"github.com/l7mp/dcontroller/pkg/operator"
	"github.com/l7mp/dcontroller/pkg/util"
)

// StatusChannelBufferSize determines the number if errors that can be buffered in the error
// channel of an operator.
const StatusChannelBufferSize = 64

type opEntry struct {
	op        *operator.Operator
	errorChan chan error
	ctx       context.Context    // Operator-specific context
	cancel    context.CancelFunc // For stopping the operator
}

type OpController struct {
	// Base context, only valid after started==true
	ctx context.Context
	// Manager for watching Operator CRDs from Kubernetes
	crdMgr manager.Manager
	// Client for accessing Kubernetes API (Operator CRDs)
	k8sClient client.Client

	// Shared cache for all operators (each operator has its own manager using this cache)
	sharedCache runtimeCache.Cache
	// Map of operator name to operator and error channel
	operators map[string]*opEntry
	// Mutex for thread-safe operator map access
	mu sync.Mutex
	// API server for exposing views
	apiServer *apiserver.APIServer
	// Error channel for operator errors
	errorChan chan error
	// REST config for creating operator managers
	config *rest.Config
	// Whether the controller has been started
	started bool

	logger, log logr.Logger
}

// NewOpController creates a new Kubernetes controller that handles the Operator CRDs. OpController
// creates and manages:
//  1. CRD manager: watches Operator CRDs from Kubernetes
//  2. Shared composite cache: used by all operators to enable cross-operator watches
//  3. Per-operator managers: each operator gets its own manager with the shared cache injected
//
// The controller has to be started using Start(ctx) that will start the CRD manager and shared cache.
func NewOpController(config *rest.Config, sharedCache runtimeCache.Cache, opts manager.Options) (*OpController, error) {
	logger := opts.Logger
	if logger.GetSink() == nil {
		logger = logr.Discard()
		opts.Logger = logger
	}

	// Create the CRD manager for watching Operator CRDs from Kubernetes
	crdMgr, err := manager.New(config, opts)
	if err != nil {
		return nil, err
	}

	if opts.HealthProbeBindAddress != "0" {
		if err := crdMgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
			return nil, err
		}

		if err := crdMgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
			return nil, err
		}
	}

	ctrl := &OpController{
		crdMgr:      crdMgr,
		k8sClient:   crdMgr.GetClient(),
		sharedCache: sharedCache,
		operators:   make(map[string]*opEntry),
		errorChan:   make(chan error),
		config:      config,
		logger:      logger,
		log:         logger.WithName("op-ctrl"),
	}

	// Create a controller to watch and reconcile the Operator CRD.
	c, err := runtimeCtrl.New("operator-ctrl", crdMgr, runtimeCtrl.Options{
		Reconciler: ctrl,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to set up operator OpController: %w", err)
	}

	if err := c.Watch(
		source.Kind[client.Object](
			crdMgr.GetCache(),
			&opv1a1.Operator{},
			&handler.EnqueueRequestForObject{},
			predicate.GenerationChangedPredicate{},
		),
	); err != nil {
		return nil, err
	}

	ctrl.log.Info("watching operator objects")

	return ctrl, nil
}

// GetSharedCache returns the shared cache used by all operators.
func (c *OpController) GetCache() runtimeCache.Cache {
	return c.sharedCache
}

// GetK8sClient returns the native Kubernetes client for the CRD manager.
func (c *OpController) GetK8sClient() client.Client {
	return c.k8sClient
}

// GetClient returns a client that can access views from all operators via the shared cache.
func (c *OpController) GetClient() client.Client {
	// Return the view cache client
	if cc, ok := c.sharedCache.(*composite.CompositeCache); ok {
		return cc.GetViewCache().GetClient()
	}
	// Fallback (should not happen in normal operation)
	return nil
}

// SetAPIServer sets the embedded API server shared by all operators.
// The API server lifecycle is not managed by the controller; make sure to run
// apiServer.Start before calling Start on the controller.
func (c *OpController) SetAPIServer(apiServer *apiserver.APIServer) {
	c.apiServer = apiServer
	c.mu.Lock()
	for _, e := range c.operators {
		e.op.SetAPIServer(apiServer)
	}
	c.mu.Unlock()
}

// GetErrorChannel returns the error channel used to surface operator errors.
func (c *OpController) GetErrorChannel() chan error {
	return c.errorChan
}

// GetOperator returns the operator with the given name.
func (c *OpController) GetOperator(name string) *operator.Operator {
	c.mu.Lock()
	op, ok := c.operators[name]
	c.mu.Unlock()
	if !ok || op == nil {
		return nil
	}
	return op.op
}

// AddOperator adds an operator to the controller.
func (c *OpController) AddOperator(op *operator.Operator) {
	name := op.GetName()
	e := &opEntry{op: op, errorChan: op.GetErrorChannel()}
	c.mu.Lock()
	c.operators[name] = e
	c.mu.Unlock()

	// Start the new operator if we are already running
	if c.started {
		c.startOp(e)
	}
}

// AddOperatorFromSpec registers an operator with the controller using an OperatorSpec.
// The operator will use the shared operator manager, enabling cross-operator watches.
func (c *OpController) AddOperatorFromSpec(name string, spec *opv1a1.OperatorSpec) (*operator.Operator, error) {
	c.log.V(4).Info("adding operator", "name", name)

	// Use the shared operator manager for this operator
	errorChan := make(chan error, StatusChannelBufferSize)
	op, err := operator.New(name, c.config, operator.Options{
		Cache:        c.sharedCache,
		APIServer:    c.apiServer,
		ErrorChannel: errorChan,
		Logger:       c.logger,
	})
	if err != nil {
		return nil, err
	}
	op.AddSpec(spec)
	c.AddOperator(op)

	return op, nil
}

// UpsertOperator creates or updates an operator.
func (c *OpController) UpsertOperator(name string, spec *opv1a1.OperatorSpec) (*operator.Operator, error) {
	c.log.V(4).Info("upserting operator", "name", name)

	// If this is a modification event, first remove old operator and create a new one
	if c.GetOperator(name) != nil {
		c.DeleteOperator(name)
	}

	return c.AddOperatorFromSpec(name, spec)
}

// DeleteOperator removes an operator from the controller.
func (c *OpController) DeleteOperator(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	e, ok := c.operators[name]
	if !ok {
		return
	}

	c.log.V(4).Info("deleting operator", "name", name)

	e.cancel()
	delete(c.operators, name)
}

// startOp starts error channel aggregation for an operator.
func (c *OpController) startOp(e *opEntry) {
	// Create a context for the operator and start it
	ctx, cancel := context.WithCancel(c.ctx)
	c.mu.Lock()
	e.ctx = ctx
	e.cancel = cancel
	c.mu.Unlock()

	go func() {
		if err := e.op.Start(ctx); err != nil {
			c.errorChan <- err
			return
		}
	}()

	// Pass the errors on to our caller
	go func() {
		for {
			select {
			case err := <-e.errorChan:
				// Use non-blocking send to avoid writing to closed channel
				select {
				case c.errorChan <- err:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}

// Reconcile runs the reconciliation logic for the OpController
func (c *OpController) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	log := c.log.WithValues("request", req.String())

	log.Info("reconciling")

	opName := req.Name
	spec := opv1a1.Operator{}
	err := c.k8sClient.Get(ctx, req.NamespacedName, &spec)
	if err != nil {
		if apierrors.IsNotFound(err) {
			c.DeleteOperator(opName)
			return reconcile.Result{}, nil
		}
		log.Error(err, "failed to get Operator")
		return reconcile.Result{}, err
	}

	op, err := c.UpsertOperator(opName, &spec.Spec)
	if err != nil {
		log.Error(err, "failed to upsert Operator")
		return reconcile.Result{}, err
	}
	c.updateStatus(ctx, op)

	return reconcile.Result{}, nil
}

// Start starts the controller. It blocks.
// Starts both the CRD manager (for watching Operator CRDs) and the operator manager
// (for running operators and storing views).
func (c *OpController) Start(ctx context.Context) error {
	c.log.V(2).Info("starting operator controller", "num-operators", len(c.operators),
		"APIServer", fmt.Sprintf("%t", c.apiServer != nil))

	// Mark as started and collect operators to start
	c.mu.Lock()
	es := []*opEntry{}
	for _, e := range c.operators {
		es = append(es, e)
	}
	c.started = true
	c.ctx = ctx
	c.mu.Unlock()

	// Start the operator manager in the background
	go func() {
		c.log.V(2).Info("starting operator manager")

		if err := c.crdMgr.Start(ctx); err != nil {
			c.log.Error(err, "operator manager error")
		}
	}()

	// Start all operators
	for _, e := range es {
		c.startOp(e)
	}

	// Surface errors from operators and generate CRD statuses.
	go func() {
		defer close(c.errorChan)
		for {
			select {
			case err := <-c.errorChan:
				var operr controller.Error
				if errors.As(err, &operr) {
					c.log.Error(err, "controller error", "operator", operr.Operator,
						"controller", operr.Controller)

					if op := c.GetOperator(operr.Operator); op == nil {
						c.log.Error(err, "spurious controller error: operator no longer available",
							"operator", operr.Operator, "controller", operr.Controller)
					} else {
						c.updateStatus(ctx, op)
					}
				} else {
					c.log.Error(err, "unknown error")
				}

			case <-ctx.Done():
				return
			}
		}
	}()

	// Start the CRD manager that watches Operator CRDs (blocks until context is done)
	c.log.V(2).Info("starting CRD manager")
	return c.crdMgr.Start(ctx)
}

func (c *OpController) updateStatus(ctx context.Context, op *operator.Operator) {
	key := types.NamespacedName{Name: op.GetName()}

	// Fetch the Operator CRD once to get the generation, then capture the status snapshot.
	// This ensures we're applying the same status on all retry attempts, preventing conflicts
	// caused by the status changing between retries (e.g., new errors being pushed).
	spec := opv1a1.Operator{}
	if err := c.k8sClient.Get(ctx, key, &spec); err != nil {
		c.log.Error(err, "failed to get operator for status update")
		return
	}

	newStatus := op.GetStatus(spec.GetGeneration())

	attempt := 0
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		attempt++

		// Refetch the Operator to get the latest resourceVersion
		spec := opv1a1.Operator{}
		if err := c.k8sClient.Get(ctx, key, &spec); err != nil {
			return err
		}

		// Use the status snapshot captured before the retry loop
		spec.Status = newStatus

		c.log.V(2).Info("updating status", "attempt", attempt, "status", util.Stringify(spec.Status))

		if err := c.k8sClient.Status().Update(ctx, &spec); err != nil {
			c.log.Error(err, "failed to update status", "attempt", attempt)
			return err
		}
		return nil
	}); err != nil {
		c.log.Error(err, "failed to update opearator status", "name", op.GetName())
		return
	}
}

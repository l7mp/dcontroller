// Package operator implements a Kubernetes controller to reconcile Operator CRD resources.
package operator

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	runtimeController "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	runtimeManager "sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	opv1a1 "hsnlab/dcontroller-runtime/pkg/api/operator/v1alpha1"
	"hsnlab/dcontroller-runtime/pkg/manager"
)

var _ Controller = &controller{}

type Controller interface {
	runtimeManager.Runnable
	reconcile.Reconciler
	GetManager() runtimeManager.Manager
}

type opEntry struct {
	op     *operator
	cancel context.CancelFunc
}

type controller struct {
	client.Client
	mgr         runtimeManager.Manager
	operators   map[types.NamespacedName]*opEntry
	mu          sync.Mutex
	options     runtimeManager.Options
	ctx         context.Context
	started     bool
	logger, log logr.Logger
}

// NewController creates a new Kubernetes controller for the Operator CRDs.
func NewController(config *rest.Config, options runtimeManager.Options) (Controller, error) {
	logger := options.Logger
	if logger.GetSink() == nil {
		logger = logr.Discard()
		options.Logger = logger
	}

	// Create a manager for this operator
	mgr, err := runtimeManager.New(config, runtimeManager.Options{Logger: logger})
	if err != nil {
		return nil, fmt.Errorf("failed to to set up manager: %w", err)
	}
	controller := &controller{
		Client:    mgr.GetClient(),
		mgr:       mgr,
		options:   options,
		operators: make(map[types.NamespacedName]*opEntry),
		logger:    logger,
		log:       logger.WithName("opcontroller"),
	}

	// Create a controller to watch and reconcile the Operator CRD.
	c, err := runtimeController.New("operator-controller", mgr, runtimeController.Options{
		Reconciler: controller,
	})

	if err := c.Watch(
		source.Kind[client.Object](
			mgr.GetCache(),
			&opv1a1.Operator{},
			&handler.EnqueueRequestForObject{},
			predicate.GenerationChangedPredicate{},
		),
	); err != nil {
		return nil, err
	}

	controller.log.Info("watching operaror objects")

	return controller, nil
}

func (c *controller) GetManager() runtimeManager.Manager { return c.mgr }

func (c *controller) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	log := c.log.WithValues("operator", req.String())

	log.Info("Reconciling")

	op := opv1a1.Operator{}
	err := c.Get(ctx, req.NamespacedName, &op)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			log.Error(err, "Failed to get Operator")
			return reconcile.Result{}, err
		}
		c.deleteOperator(req.NamespacedName)
	}

	operator := c.upsertOperator(&op)

	// set status
	op.Status = operator.GetStatus(op.GetGeneration())
	if err := c.Status().Update(ctx, &op); err != nil {
		log.Error(err, "Failed to update Operator status")
		return reconcile.Result{}, err
	}

	return reconcile.Result{}, nil
}

// Start starts the operator controller and starts each operator registered with the controller. It blocks

func (c *controller) Start(ctx context.Context) error {
	c.log.Info("starting operators")

	c.mu.Lock()
	c.ctx = ctx
	c.started = true
	for k := range c.operators {
		c.startOp(k)
	}
	c.mu.Unlock()

	return c.mgr.Start(ctx)
}

func (c *controller) getOperatorEntry(key types.NamespacedName) *opEntry {
	c.mu.Lock()
	op := c.operators[key]
	c.mu.Unlock()
	return op
}

func (c *controller) upsertOperator(op *opv1a1.Operator) *operator {
	key := client.ObjectKeyFromObject(op)
	e := c.getOperatorEntry(key)

	// if this is a modification event we first remove old operator and create a new one
	if e == nil {
		c.deleteOperator(key)
	}
	return c.addOperator(op)
}

func (c *controller) addOperator(op *opv1a1.Operator) *operator {
	// First create a manager for this operator
	mgr, err := manager.New(nil, c.mgr.GetConfig(), c.options)
	if err != nil {
		c.log.Error(err, "failed to create manager for operator", "operator", op.Name)
		return nil
	}

	key := client.ObjectKeyFromObject(op)
	operator := New(mgr, &op.Spec, c.logger)

	c.mu.Lock()
	c.operators[key] = &opEntry{op: operator}
	c.mu.Unlock()

	// start the new operator if we are already running
	if c.started {
		c.startOp(key)
	}

	return operator
}

func (c *controller) deleteOperator(k types.NamespacedName) {
	e := c.getOperatorEntry(k)
	if e == nil {
		return
	}

	if e.cancel != nil {
		e.cancel()
	}

	delete(c.operators, k)
}

func (c *controller) startOp(k types.NamespacedName) {
	c.mu.Lock()
	e, ok := c.operators[k]
	c.mu.Unlock()

	if !ok {
		return
	}

	ctx, cancel := context.WithCancel(c.ctx)
	e.cancel = cancel

	c.mu.Lock()
	c.operators[k] = e
	c.mu.Unlock()

	go e.op.Start(ctx)
}

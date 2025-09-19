// Package controllers implements the Kubernetes controllers used to reconcile the CRD resources.
package controllers

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	runtimeCtrl "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	runtimeMgr "sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	opv1a1 "github.com/l7mp/dcontroller/pkg/api/operator/v1alpha1"
	"github.com/l7mp/dcontroller/pkg/apiserver"
	"github.com/l7mp/dcontroller/pkg/controller"
	"github.com/l7mp/dcontroller/pkg/operator"
	"github.com/l7mp/dcontroller/pkg/util"
)

type OpController struct {
	client.Client
	mgr     runtimeMgr.Manager
	opgroup *operator.Group
	log     logr.Logger
}

// NewOpController creates a new Kubernetes controller that handles the Operator CRDs. OpController
// creates and manages its own controller-runtime manager based on the passed options. The
// controller has to be started using Start(ctx) that will automatically start the underlyng
// controller-runtime manager.
func NewOpController(config *rest.Config, opts runtimeMgr.Options) (*OpController, error) {
	logger := opts.Logger
	if logger.GetSink() == nil {
		logger = logr.Discard()
		opts.Logger = logger
	}

	mgr, err := runtimeMgr.New(config, opts)
	if err != nil {
		return nil, err
	}

	if opts.HealthProbeBindAddress != "0" {
		if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
			return nil, err
		}

		if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
			return nil, err
		}
	}

	ctrl := &OpController{
		mgr:     mgr,
		Client:  mgr.GetClient(),
		opgroup: operator.NewGroup(config, logger),
		log:     logger.WithName("op-ctrl"),
	}

	// Create a OpController to watch and reconcile the Operator CRD.
	c, err := runtimeCtrl.New("operator-ctrl", mgr, runtimeCtrl.Options{
		Reconciler: ctrl,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to to set up operator OpController: %w", err)
	}

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

	ctrl.log.Info("watching operaror objects")

	return ctrl, nil
}

// GetOperatorGroup returns the operator group that handles the reconciled operators.
func (c *OpController) GetOperatorGroup() *operator.Group { return c.opgroup }

// SetAPIServer allows to set the embedded API server uf the underlying operator group. The API
// server lifecycle is not managed by the operator; make sure to run apiServer.Start before calling
// Start on the oparator.
func (c *OpController) SetAPIServer(apiServer *apiserver.APIServer) {
	c.opgroup.SetAPIServer(apiServer)
}

// Reconcile runs the reconciliation logic for the OpController
func (c *OpController) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	log := c.log.WithValues("request", req.String())

	log.Info("reconciling")

	opName := req.Name
	spec := opv1a1.Operator{}
	err := c.Get(ctx, req.NamespacedName, &spec)
	if err != nil {
		if apierrors.IsNotFound(err) {
			c.opgroup.DeleteOperator(opName)
			return reconcile.Result{}, nil
		}
		log.Error(err, "failed to get Operator")
		return reconcile.Result{}, err
	}

	op, err := c.opgroup.UpsertOperator(opName, &spec.Spec)
	if err != nil {
		return reconcile.Result{}, err
	}
	c.updateStatus(ctx, op)

	return reconcile.Result{}, nil
}

// Start starts runtimeCtrl. It blocks.
func (c *OpController) Start(ctx context.Context) error {
	// Start the operator group.
	go func() {
		if err := c.opgroup.Start(ctx); err != nil {
			c.log.Error(err, "operator exited with an error")
		}
	}()

	// Start the controller runtime manager that will start our watches.
	go func() {
		if err := c.mgr.Start(ctx); err != nil {
			c.log.Error(err, "controller runtime manager exited with an error")
		}
	}()

	// Surface errors from the operator group and generate CRD statuses.
	for {
		select {
		case err := <-c.opgroup.GetErrorChannel():
			var operr controller.Error
			if errors.As(err, &operr) {
				c.log.Error(err, "controller error", "operator", operr.Operator,
					"controller", operr.Controller)

				if op := c.opgroup.GetOperator(operr.Operator); op == nil {
					c.log.Error(err, "spurious controller error: operator no longer avalialble",
						"operator", operr.Operator, "controller", operr.Controller)
				} else {
					c.updateStatus(ctx, op)
				}
			} else {
				c.log.Error(err, "unknown error")
			}

		case <-ctx.Done():
			return nil
		}
	}
}

func (c *OpController) updateStatus(ctx context.Context, op *operator.Operator) {
	key := types.NamespacedName{Name: op.GetName()}
	attempt := 0
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		attempt++

		spec := opv1a1.Operator{}
		if err := c.Get(ctx, key, &spec); err != nil {
			return err
		}

		spec.Status = op.GetStatus(spec.GetGeneration())

		c.log.V(2).Info("updating status", "attempt", attempt, "status", util.Stringify(spec.Status))

		if err := c.Status().Update(ctx, &spec); err != nil {
			c.log.Error(err, "failed to update status", "attempt", attempt)
			return err
		}
		return nil
	}); err != nil {
		c.log.Error(err, "failed to update opearator status", "name", op.GetName())
		return
	}
}

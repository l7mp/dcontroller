// Package operator implements a Kubernetes controller to reconcile Operator CRD resources.
package operator

import (
	"context"

	"github.com/go-logr/logr"
	runtimeManager "sigs.k8s.io/controller-runtime/pkg/manager"

	opv1a1 "hsnlab/dcontroller-runtime/pkg/api/operator/v1alpha1"
	dcontroller "hsnlab/dcontroller-runtime/pkg/controller"
)

var _ Operator = &operator{}

type Operator interface {
	runtimeManager.Runnable
	GetStatus(int64) opv1a1.OperatorStatus
}

type operator struct {
	name        string
	mgr         runtimeManager.Manager
	op          *opv1a1.OperatorSpec
	controllers []dcontroller.Controller
	logger, log logr.Logger
}

// NewOperator creates a new operator.
func New(mgr runtimeManager.Manager, op *opv1a1.OperatorSpec, logger logr.Logger) *operator {
	return &operator{
		name:        op.Name,
		mgr:         mgr,
		op:          op,
		controllers: []dcontroller.Controller{},
		logger:      logger,
		log:         logger.WithName("operator").WithValues("name", op.Name),
	}
}

// Start starts the operator. It blocks
func (op *operator) Start(ctx context.Context) error {
	op.log.Info("starting")

	return op.mgr.Start(ctx)

}

// GetStatus populates the operator status with the controller statuses.
func (op *operator) GetStatus(gen int64) opv1a1.OperatorStatus {
	cs := []opv1a1.ControllerStatus{}
	for _, c := range op.controllers {
		cs = append(cs, c.GetStatus(gen))
	}
	return opv1a1.OperatorStatus{
		Controllers: cs,
	}
}

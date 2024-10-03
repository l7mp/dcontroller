// Package operator implements a Kubernetes controller to reconcile Operator CRD resources.
package operator

import (
	"context"

	"github.com/go-logr/logr"
	runtimeManager "sigs.k8s.io/controller-runtime/pkg/manager"

	opv1a1 "hsnlab/dcontroller/pkg/api/operator/v1alpha1"
	dcontroller "hsnlab/dcontroller/pkg/controller"
)

var _ Operator = &operator{}

type Operator interface {
	runtimeManager.Runnable
	GetStatus(int64) opv1a1.OperatorStatus
}

type operator struct {
	name          string
	mgr           runtimeManager.Manager
	spec          *opv1a1.OperatorSpec
	controllers   []*dcontroller.Controller // maybe nil
	statusUpdater StatusUpdater
	logger, log   logr.Logger
}

// NewOperator creates a new operator.
func New(mgr runtimeManager.Manager, name string, spec *opv1a1.OperatorSpec, statusUpdater StatusUpdater, logger logr.Logger) *operator {
	op := &operator{
		name:          name,
		mgr:           mgr,
		spec:          spec,
		controllers:   []*dcontroller.Controller{},
		logger:        logger,
		statusUpdater: statusUpdater,
		log:           logger.WithName("operator").WithValues("name", name),
	}

	// Create the controllers for the operator (manager.Start() will automatically start them)
	for _, config := range spec.Controllers {
		c, err := dcontroller.New(op.mgr, config, dcontroller.Options{
			StatusTrigger: op,
		})
		if err != nil {
			// report errors but otherwise move on: controller erros will be reported
			// in the operator's controller statuses
			op.log.Error(err, "failed to create controller", "controller", config.Name)
		}

		op.controllers = append(op.controllers, c)
	}

	return op
}

// Start starts the operator. It blocks
func (op *operator) Start(ctx context.Context) error {
	op.log.Info("starting")

	return op.mgr.Start(ctx)

}

// GetManager returns the controller runtime manager associated with the operator.
func (op *operator) GetManager() runtimeManager.Manager {
	return op.mgr
}

// Trigger can be used to ask a status update trigger on the operator.
func (op *operator) Trigger() {
	if err := op.statusUpdater.UpdateStatus(context.TODO(), op); err != nil {
		op.log.Error(err, "failed to update status")
	}
}

// GetStatus populates the operator status with the controller statuses.
func (op *operator) GetStatus(gen int64) opv1a1.OperatorStatus {
	cs := []opv1a1.ControllerStatus{}
	for _, c := range op.controllers {
		if c != nil {
			cs = append(cs, c.GetStatus(gen))
		}
	}
	return opv1a1.OperatorStatus{
		Controllers: cs,
	}
}

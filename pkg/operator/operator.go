// Package operator implements a Kubernetes controller to reconcile Operator CRD resources.
package operator

import (
	"context"
	"fmt"
	"os"

	"github.com/go-logr/logr"
	runtimeManager "sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/yaml"

	opv1a1 "hsnlab/dcontroller/pkg/api/operator/v1alpha1"
	dcontroller "hsnlab/dcontroller/pkg/controller"
)

var _ runtimeManager.Runnable = &Operator{}

// Options can be used to customize the Operator's behavior.
type Options struct {
	// ErrorChannel is a channel to receive errors from the operator.
	ErrorChannel chan error

	// Logger is a standard logger.
	Logger logr.Logger
}

type Operator struct {
	name        string
	mgr         runtimeManager.Manager
	spec        *opv1a1.OperatorSpec
	controllers []*dcontroller.Controller // maybe nil
	ctx         context.Context
	errorChan   chan error
	logger, log logr.Logger
}

// New creates a new operator.
func New(name string, mgr runtimeManager.Manager, spec *opv1a1.OperatorSpec, opts Options) *Operator {
	logger := opts.Logger
	if logger.GetSink() == nil {
		logger = logr.Discard()
	}

	op := &Operator{
		name:        name,
		mgr:         mgr,
		spec:        spec,
		controllers: []*dcontroller.Controller{},
		errorChan:   opts.ErrorChannel,
		logger:      logger,
		log:         logger.WithName("operator").WithValues("name", name),
	}

	// Create the controllers for the operator (manager.Start() will automatically start them)
	for _, config := range spec.Controllers {
		c, err := dcontroller.New(op.mgr, config, dcontroller.Options{
			ErrorHandler: op,
		})
		if err != nil {
			// report errors but otherwise move on: controller erros will be reported
			// in the operator's controller statuses
			op.log.Error(err, "failed to create controller", "controller", config.Name)
		}

		// the controller returned is always valid: this makes sure we will receive the
		// status update triggers to show the controller errors to the user
		op.controllers = append(op.controllers, c)
	}

	return op
}

// Load creates a new operator from a serialized operator spec.
func Load(name string, mgr runtimeManager.Manager, file string, opts Options) (*Operator, error) {
	b, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}
	var spec opv1a1.OperatorSpec
	if err := yaml.Unmarshal(b, &spec); err != nil {
		return nil, fmt.Errorf("failed to parse operator spec: %w", err)
	}

	return New(name, mgr, &spec, opts), nil
}

// Start starts the operator. It blocks
func (op *Operator) Start(ctx context.Context) error {
	op.log.Info("starting")
	op.ctx = ctx
	return op.mgr.Start(ctx)
}

// GetManager returns the controller runtime manager associated with the operator.
func (op *Operator) GetManager() runtimeManager.Manager {
	return op.mgr
}

// GetName returns the name of the operator.
func (op *Operator) GetName() string {
	return op.name
}

// Trigger can be used to ask a status update trigger on the operator.
func (op *Operator) Trigger(err error) {
	ctx := op.ctx
	if ctx == nil {
		// we are not started yet:
		ctx = context.TODO()
	}

	// make this async so that we won't block the operator
	go func() {
		defer close(op.errorChan)

		select {
		case op.errorChan <- err:
			op.log.V(4).Info("operator status update triggered")
			return
		case <-ctx.Done():
			return
		}
	}()
}

// GetStatus populates the operator status with the controller statuses.
func (op *Operator) GetStatus(gen int64) opv1a1.OperatorStatus {
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

// Package operator implements a Kubernetes controller to reconcile Operator CRD resources.
package operator

import (
	"context"
	"fmt"
	"os"

	"github.com/go-logr/logr"
	runtimeManager "sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/yaml"

	opv1a1 "github.com/hsnlab/dcontroller/pkg/api/operator/v1alpha1"
	dcontroller "github.com/hsnlab/dcontroller/pkg/controller"
)

var _ runtimeManager.Runnable = &Operator{}

// Options can be used to customize the Operator's behavior.
type Options struct {
	// ErrorChannel is a channel to receive errors from the operator. Note that the error
	// channel is rate limited to at most 3 errors per every 2 seconds. Use ReportErrors on the
	// individual controllers to get the errors that might have been supporessed by the rate
	// limiter.
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
		if err := op.AddController(config); err != nil {
			// error already pushed to the error channel: move on and let parent decide what to do
			op.log.V(5).Info("failed to create controller", "controller", config.Name,
				"error", err)
		}
	}

	return op
}

// NewFromFile creates a new operator from a serialized operator spec.
func NewFromFile(name string, mgr runtimeManager.Manager, file string, opts Options) (*Operator, error) {
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

// GetController returns the controller with the given name or nil if no controller with that name
// exists.
func (op *Operator) GetController(name string) *dcontroller.Controller {
	for _, c := range op.controllers {
		if c.GetName() == name {
			return c
		}
	}

	return nil
}

// AddController adds a new controller to the operator.
func (op *Operator) AddController(config opv1a1.Controller) error {
	c, err := dcontroller.New(op.mgr, config, dcontroller.Options{
		ErrorChan: op.errorChan,
	})

	// the controller returned is always valid: this makes sure we will receive the
	// status update triggers to show the controller errors to the user
	op.controllers = append(op.controllers, c)

	return err
}

// Start starts the operator. It blocks
func (op *Operator) Start(ctx context.Context) error {
	op.log.Info("starting")
	op.ctx = ctx

	// close the error channel
	if op.errorChan != nil {
		go func() {
			defer close(op.errorChan)
			<-ctx.Done()
		}()
	}

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
	if op.errorChan != nil {
		// this should be async so that we won't block the controller - if someone passed
		// an errorchannel to the constructor we expect them to actually read what we write
		// there
		op.errorChan <- err
	}
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

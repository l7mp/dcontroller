// Package operator is the top-level Operator abstraction that implement the declarative controller
// specifications.
//
// The operator creates/manages the corresponding Δ-controllers based on their specifications. It
// provides the bridge between declarative YAML specifications and the imperative controller
// runtime.
//
// Example usage:
//
//	op, _ := operator.NewFromFile("my-op", mgr, "operator.yaml", operator.Options{
//	    APIServer: server,
//	    Logger: logger,
//	})
package operator

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime/schema"
	runtimeMgr "sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/yaml"

	opv1a1 "github.com/l7mp/dcontroller/pkg/api/operator/v1alpha1"
	viewv1a1 "github.com/l7mp/dcontroller/pkg/api/view/v1alpha1"
	"github.com/l7mp/dcontroller/pkg/apiserver"
	dcontroller "github.com/l7mp/dcontroller/pkg/controller"
)

var _ runtimeMgr.Runnable = &Operator{}

// Options can be used to customize the Operator's behavior.
type Options struct {
	// ErrorChannel is a channel to receive errors from the operator. Note that the error
	// channel is rate limited to at most 3 errors per every 2 seconds. Use ReportErrors on the
	// individual controllers to get the errors that might have been supporessed by the rate
	// limiter.
	ErrorChannel chan error

	// API server is an optional extension server that can be used to interact with the view
	// objects stored in the operator cacache.
	APIServer *apiserver.APIServer

	// Logger is a standard logger.
	Logger logr.Logger
}

// Operator definition.
type Operator struct {
	name        string
	mgr         runtimeMgr.Manager
	apiServer   *apiserver.APIServer
	spec        *opv1a1.OperatorSpec
	controllers []*dcontroller.Controller // maybe nil
	ctx         context.Context
	gvks        []schema.GroupVersionKind
	errorChan   chan error
	started     bool
	logger, log logr.Logger
}

// New creates a new operator.
func New(name string, mgr runtimeMgr.Manager, spec *opv1a1.OperatorSpec, opts Options) *Operator {
	logger := opts.Logger
	if logger.GetSink() == nil {
		logger = logr.Discard()
	}

	op := &Operator{
		name:        name,
		mgr:         mgr,
		spec:        spec,
		controllers: []*dcontroller.Controller{},
		apiServer:   opts.APIServer,
		gvks:        []schema.GroupVersionKind{},
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

	// Update the API server
	if err := op.RegisterGVKs(); err != nil {
		// this is not fatal
		op.log.Error(err, "failed to register GKVs with the API server")
	}

	return op
}

// NewFromFile creates a new operator from a serialized operator spec.
func NewFromFile(name string, mgr runtimeMgr.Manager, file string, opts Options) (*Operator, error) {
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

// SetAPIServer allows to set the embedded API server. The API server lifecycle is not managed by
// the operator; make sure to run apiServer.Start before calling Start on the oparator.
func (op *Operator) SetAPIServer(apiServer *apiserver.APIServer) {
	op.apiServer = apiServer
}

// ListControllers lists the controllers for the operator.
func (op *Operator) ListControllers() []*dcontroller.Controller {
	ret := make([]*dcontroller.Controller, len(op.controllers))
	copy(ret, op.controllers)
	return ret
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
	c, err := dcontroller.New(op.mgr, op.name, config, dcontroller.Options{ErrorChan: op.errorChan})
	if err != nil {
		op.log.Error(err, "failed to create controller", "name", config.Name)
	}

	// the controller returned is always valid: this makes sure we will receive the
	// status update triggers to show the controller errors to the user
	op.controllers = append(op.controllers, c)

	return err
}

// Start starts the operator. It blocks
func (op *Operator) Start(ctx context.Context) error {
	if op.started {
		return errors.New("operator already started")
	}

	op.log.Info("starting up")
	op.ctx = ctx
	op.started = true

	defer func() {
		if op.errorChan != nil {
			close(op.errorChan)
		}
	}()

	return op.mgr.Start(ctx)
}

// GetManager returns the controller runtime manager associated with the operator.
func (op *Operator) GetManager() runtimeMgr.Manager {
	return op.mgr
}

// GetName returns the name of the operator.
func (op *Operator) GetName() string {
	return op.name
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

// RegisterGVKs registers the view resources associated with the conbtrollers run by operator in
// the extension API server.
func (op *Operator) RegisterGVKs() error {
	if op.apiServer == nil {
		return nil
	}

	op.log.V(2).Info("registering GVKs", "API group", viewv1a1.Group(op.name))

	gvks := []schema.GroupVersionKind{}
	for _, c := range op.controllers {
		gvks = append(gvks, c.GetGVKs()...)
	}

	op.gvks = uniq(gvks)
	return op.apiServer.RegisterGVKs(op.gvks)
}

func uniq(gvks []schema.GroupVersionKind) []schema.GroupVersionKind {
	var ret []schema.GroupVersionKind
	set := make(map[schema.GroupVersionKind]bool)
	for _, item := range gvks {
		if !set[item] {
			set[item] = true
			ret = append(ret, item)
		}
	}

	return ret
}

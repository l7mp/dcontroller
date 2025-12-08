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
	"fmt"
	"os"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	runtimeConfig "sigs.k8s.io/controller-runtime/pkg/config"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/yaml"

	opv1a1 "github.com/l7mp/dcontroller/pkg/api/operator/v1alpha1"
	viewv1a1 "github.com/l7mp/dcontroller/pkg/api/view/v1alpha1"
	"github.com/l7mp/dcontroller/pkg/apiserver"
	"github.com/l7mp/dcontroller/pkg/cache"
	dcontroller "github.com/l7mp/dcontroller/pkg/controller"
	"github.com/l7mp/dcontroller/pkg/manager"
)

// Options can be used to customize the Operator's behavior.
type Options struct {
	// Cache can be used to redirect the storage used by the operator into an external
	// cache. This allows multiple operators to share cache state while maintaining independent
	// lifecycles.
	Cache cache.Cache

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
	mgr         manager.Manager
	apiServer   *apiserver.APIServer
	controllers []dcontroller.Controller // maybe nil
	errorChan   chan error
	logger, log logr.Logger
}

// New creates a new operator with its own dedicated manager.
func New(name string, config *rest.Config, opts Options) (*Operator, error) {
	logger := opts.Logger
	if logger.GetSink() == nil {
		logger = logr.Discard()
	}

	var cacheInjector cache.NewCacheFunc
	if opts.Cache != nil {
		cacheInjector = manager.CacheInjector(opts.Cache)
	}

	// Create dedicated manager for this operator.
	off := true
	mgr, err := manager.New(config, manager.Options{
		NewCache:               cacheInjector,
		LeaderElection:         false,
		HealthProbeBindAddress: "0",
		Controller: runtimeConfig.Controller{
			SkipNameValidation: &off,
		},
		Metrics: metricsserver.Options{
			BindAddress: "0",
		},
		Logger: logger.WithName("operator").WithValues("operator", name),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create manager for operator %s: %w", name, err)
	}

	return &Operator{
		name:        name,
		mgr:         mgr,
		controllers: []dcontroller.Controller{},
		apiServer:   opts.APIServer,
		errorChan:   opts.ErrorChannel,
		logger:      logger,
		log:         logger.WithName("operator").WithValues("name", name),
	}, nil
}

// AddSpec adds a declarative controller spec to the operator.
func (op *Operator) AddSpec(spec *opv1a1.OperatorSpec) {
	// Create the controllers for the operator (manager.Start() will automatically start them)
	for _, config := range spec.Controllers {
		if err := op.AddDeclarativeController(config); err != nil {
			// error already pushed to the error channel: move on and let parent decide what to do
			op.log.V(5).Info("failed to create controller", "controller", config.Name,
				"error", err)
		}
	}
}

// NewFromFile creates a new operator from a serialized operator spec. Note that once this call
// finishes there is no way to add new controllers to the operator.
func NewFromFile(name string, config *rest.Config, file string, opts Options) (*Operator, error) {
	b, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}
	var spec opv1a1.OperatorSpec
	if err := yaml.Unmarshal(b, &spec); err != nil {
		return nil, fmt.Errorf("failed to parse operator spec: %w", err)
	}

	op, err := New(name, config, opts)
	if err != nil {
		return nil, err
	}
	op.AddSpec(&spec)

	return op, nil
}

// SetAPIServer allows to set the embedded API server. The API server lifecycle is not managed by
// the operator; make sure to run apiServer.Start before calling Start on the oparator.
func (op *Operator) SetAPIServer(apiServer *apiserver.APIServer) {
	op.apiServer = apiServer
}

// ListControllers lists the controllers for the operator.
func (op *Operator) ListControllers() []dcontroller.Controller {
	ret := make([]dcontroller.Controller, len(op.controllers))
	copy(ret, op.controllers)
	return ret
}

// GetController returns the controller with the given name or nil if no controller with that name
// exists.
func (op *Operator) GetController(name string) dcontroller.Controller {
	for _, c := range op.controllers {
		if c.GetName() == name {
			return c
		}
	}
	return nil
}

// AddDeclarativeController adds a new declarative controller spec to the operator.
func (op *Operator) AddDeclarativeController(config opv1a1.Controller) error {
	c, err := dcontroller.NewDeclarative(op.mgr, op.name, config, dcontroller.Options{ErrorChan: op.errorChan})
	if err != nil {
		op.log.Error(err, "failed to create controller", "name", config.Name)
	}

	// the controller returned is always valid: this makes sure we will receive the
	// status update triggers to show the controller errors to the user
	op.controllers = append(op.controllers, c)

	if err := op.RegisterGVKs(); err != nil {
		// this is not fatal
		op.log.Error(err, "failed to register GKVs with the API server")
	}

	return err
}

// AddNativeController adds a controller-runtime controller to the operator.
func (op *Operator) AddNativeController(name string, ctrl dcontroller.RuntimeController, gvks []schema.GroupVersionKind) error {
	c, err := dcontroller.NewNative(name, ctrl, gvks)
	if err != nil {
		op.log.Error(err, "failed to create controller", "name", name)
	}

	// the controller returned is always valid: this makes sure we will receive the
	// status update triggers to show the controller errors to the user
	op.controllers = append(op.controllers, c)

	if err := op.RegisterGVKs(); err != nil {
		// this is not fatal
		op.log.Error(err, "failed to register GKVs with the API server")
	}

	return err
}

// GetManager returns the controller runtime manager associated with the operator.
func (op *Operator) GetManager() manager.Manager {
	return op.mgr
}

// GetClient returns a client.Client for the storage underlying the operator.
func (op *Operator) GetClient() client.Client {
	return op.mgr.GetClient()
}

// Start starts the operator's manager. This blocks until the context is cancelled.
// Controllers registered with the operator will be started automatically by the manager.
func (op *Operator) Start(ctx context.Context) error {
	op.log.V(2).Info("starting operator")
	err := op.mgr.Start(ctx)
	op.log.V(2).Info("operator has stopped", "error", err == nil)
	return err
}

// GetName returns the name of the operator.
func (op *Operator) GetName() string {
	return op.name
}

// GetErrorChannel returns the channel that can be used to retrieve the runtime errors from the
// operator.
func (op *Operator) GetErrorChannel() chan error {
	return op.errorChan
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

// RegisterGVKs registers the view resources associated with the operator' controllers in the
// extension API server.
func (op *Operator) RegisterGVKs() error {
	if op.apiServer == nil {
		return nil
	}

	gvks := op.GetGVKs()

	op.log.V(2).Info("registering GVKs", "API group", viewv1a1.Group(op.name),
		"GVKs", gvks)

	return op.apiServer.RegisterGVKs(gvks)
}

// // UnregisterGVKs unregisters the view resources associated with the controllers.
func (op *Operator) UnregisterGVKs() {
	if op.apiServer == nil {
		return
	}

	op.log.V(2).Info("unregistering GVKs", "API group", viewv1a1.Group(op.name))

	op.apiServer.UnregisterGVKs(op.GetGVKs())
}

// GetGVKs returns the GVKs of this operator group.
func (op *Operator) GetGVKs() []schema.GroupVersionKind {
	gvks := []schema.GroupVersionKind{}
	for _, c := range op.controllers {
		gvks = append(gvks, c.GetGVKs()...)
	}

	var ret []schema.GroupVersionKind
	set := make(map[schema.GroupVersionKind]bool)
	for _, item := range gvks {
		if item.Group != viewv1a1.Group(op.name) {
			continue
		}
		if !set[item] {
			set[item] = true
			ret = append(ret, item)
		}
	}

	return ret
}

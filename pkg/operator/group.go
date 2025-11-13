package operator

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/go-logr/logr"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	runtimeConfig "sigs.k8s.io/controller-runtime/pkg/config"
	runtimeManager "sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/yaml"

	opv1a1 "github.com/l7mp/dcontroller/pkg/api/operator/v1alpha1"
	"github.com/l7mp/dcontroller/pkg/apiserver"
	"github.com/l7mp/dcontroller/pkg/manager"
)

// StatusChannelBufferSize defines the longest backlog on the status channel.
const StatusChannelBufferSize = 64

type opEntry struct {
	op        *Operator
	errorChan chan error
}

// Group is a set of operators that share the same lifecycle, a common manager, and a common APIServer.
// All operators in the group share a single manager with a composite cache, enabling cross-operator watches.
type Group struct {
	operators   map[string]*opEntry
	config      *rest.Config
	mgr         *manager.Manager // Shared manager for all operators
	mu          sync.Mutex
	apiServer   *apiserver.APIServer
	errorChan   chan error
	ctx         context.Context
	started     bool
	logger, log logr.Logger
}

// NewGroup creates a new operator group with a shared manager.
// The shared manager uses a composite cache that can store views from all operators,
// enabling cross-operator watches.
func NewGroup(config *rest.Config, logger logr.Logger) *Group {
	if logger.GetSink() == nil {
		logger = logr.Discard()
	}

	// Create shared manager with composite cache that accepts all view groups
	off := true
	mgr, err := manager.New(config, manager.Options{
		Options: runtimeManager.Options{
			LeaderElection:         false,
			HealthProbeBindAddress: "0",
			// Disable global controller name uniquness test
			Controller: runtimeConfig.Controller{
				SkipNameValidation: &off,
			},
			Metrics: metricsserver.Options{
				BindAddress: "0",
			},
			Logger: logger,
		},
	})
	if err != nil {
		// This should not happen with valid config, but handle gracefully
		logger.Error(err, "failed to create shared manager for operator group")
		return nil
	}

	return &Group{
		mgr:       mgr,
		config:    config,
		operators: make(map[string]*opEntry),
		errorChan: make(chan error),
		logger:    logger,
		log:       logger.WithName("op-group"),
	}
}

// GetClient returns the shared manager's client that can access views from all operators.
func (g *Group) GetManager() runtimeManager.Manager {
	return g.mgr
}

// GetClient returns the shared manager's client that can access views from all operators.
func (g *Group) GetClient() client.Client {
	return g.GetManager().GetClient()
}

// SetAPIServer allows to set the embedded API server shared by the operatos for this group. The
// API server lifecycle is not managed by the operator; make sure to run apiServer.Start before
// calling Start on the oparator group.
func (g *Group) SetAPIServer(apiServer *apiserver.APIServer) {
	g.apiServer = apiServer
	for _, e := range g.operators {
		e.op.SetAPIServer(apiServer)
	}
}

// GetErrorChannel returns the error channel the group uses to surface errors.
func (g *Group) GetErrorChannel() chan error {
	return g.errorChan
}

// GetOperator returns the operator with the given name.
func (g *Group) GetOperator(name string) *Operator {
	g.mu.Lock()
	op, ok := g.operators[name]
	g.mu.Unlock()
	if !ok || op == nil {
		return nil
	}
	return op.op
}

// AddOperatorFromFile loads an operator from file.
func (g *Group) AddOperatorFromFile(name string, file string) (*Operator, error) {
	b, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}
	var spec opv1a1.OperatorSpec
	if err := yaml.Unmarshal(b, &spec); err != nil {
		return nil, fmt.Errorf("failed to parse operator spec: %w", err)
	}
	return g.AddOperatorFromSpec(name, &spec)
}

// AddOperatorFromSpec registers an operator with the group.
// The operator will use the group's shared manager, enabling cross-operator watches.
func (g *Group) AddOperatorFromSpec(name string, spec *opv1a1.OperatorSpec) (*Operator, error) {
	g.log.V(4).Info("adding operator to group", "name", name)

	// Use the shared manager for this operator
	errorChan := make(chan error, StatusChannelBufferSize)
	op := New(name, g.mgr, Options{
		APIServer:    g.apiServer,
		ErrorChannel: errorChan,
		Logger:       g.logger,
	})
	op.AddSpec(spec)
	g.AddOperator(op)

	return op, nil
}

func (g *Group) AddOperator(op *Operator) {
	name := op.GetName()
	e := &opEntry{op: op, errorChan: op.GetErrorChannel()}
	g.mu.Lock()
	g.operators[name] = e
	g.mu.Unlock()

	// Start the new operator if we are already running
	if g.started {
		g.startOp(e)
	}
}

func (g *Group) UpsertOperator(name string, spec *opv1a1.OperatorSpec) (*Operator, error) {
	g.log.V(4).Info("upserting operator", "name", name)

	// if this is a modification event we first remove old operator and create a new one
	if e := g.getOperatorEntry(name); e != nil {
		g.DeleteOperator(name)
	}

	return g.AddOperatorFromSpec(name, spec)
}

func (g *Group) DeleteOperator(name string) {
	g.log.V(4).Info("deleting operator", "name", name)

	g.mu.Lock()
	delete(g.operators, name)
	g.mu.Unlock()
}

// Start starts the shared manager and all operators registered with the group. It blocks.
func (g *Group) Start(ctx context.Context) error {
	defer close(g.errorChan)
	g.log.V(2).Info("starting operator group", "num-operators", len(g.operators),
		"APIServer", fmt.Sprintf("%t", g.apiServer == nil))

	g.mu.Lock()
	es := []*opEntry{}
	for _, e := range g.operators {
		es = append(es, e)
	}
	g.ctx = ctx
	g.started = true
	g.mu.Unlock()

	// Start all operators
	for _, e := range es {
		g.startOp(e)
	}

	// Start the shared manager (blocks until context is done)
	g.log.V(2).Info("starting manager")
	if err := g.mgr.Start(ctx); err != nil {
		return fmt.Errorf("shared manager error: %w", err)
	}

	return nil
}

func (g *Group) startOp(e *opEntry) {
	// pass the errors on to our caller
	go func() {
		for err := range e.errorChan {
			g.errorChan <- err
		}
	}()
}

func (g *Group) getOperatorEntry(name string) *opEntry {
	g.mu.Lock()
	op := g.operators[name]
	g.mu.Unlock()
	return op
}

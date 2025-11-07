package operator

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/go-logr/logr"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	runtimeManager "sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/yaml"

	opv1a1 "github.com/l7mp/dcontroller/pkg/api/operator/v1alpha1"
	viewv1a1 "github.com/l7mp/dcontroller/pkg/api/view/v1alpha1"
	"github.com/l7mp/dcontroller/pkg/apiserver"
	"github.com/l7mp/dcontroller/pkg/composite"
	"github.com/l7mp/dcontroller/pkg/manager"
)

// StatusChannelBufferSize defines the longest backlog on the status channel.
const StatusChannelBufferSize = 64

type opEntry struct {
	op        *Operator
	errorChan chan error
	cancel    context.CancelFunc
}

// Group is a set of operators that share the same lifecycle and a common APIServer
type Group struct {
	operators   map[string]*opEntry
	config      *rest.Config
	clientMpx   composite.ClientMultiplexer
	mu          sync.Mutex
	apiServer   *apiserver.APIServer
	errorChan   chan error
	ctx         context.Context
	started     bool
	logger, log logr.Logger
}

// NewController creates a new Kubernetes controller that handles the Operator CRDs.
func NewGroup(config *rest.Config, logger logr.Logger) *Group {
	if logger.GetSink() == nil {
		logger = logr.Discard()
	}

	return &Group{
		clientMpx: composite.NewClientMultiplexer(logger),
		config:    config,
		operators: make(map[string]*opEntry),
		errorChan: make(chan error),
		logger:    logger,
		log:       logger.WithName("op-group"),
	}
}

// GetClient returns a controller runtime client that multiplexes all registered operator clients.
func (g *Group) GetClient() client.Client {
	return g.clientMpx
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
func (g *Group) AddOperatorFromSpec(name string, spec *opv1a1.OperatorSpec) (*Operator, error) {
	g.log.V(4).Info("adding operator", "name", name)

	// First create a manager for this operator
	mgr, err := manager.New(g.config, name, manager.Options{Options: g.newDefaultOpts()})
	if err != nil {
		return nil, fmt.Errorf("failed to create manager for operator %s: %w",
			name, err)
	}

	errorChan := make(chan error, StatusChannelBufferSize)
	op := New(name, mgr, Options{
		APIServer:    g.apiServer,
		ErrorChannel: errorChan,
		Logger:       g.logger,
	})
	op.AddSpec(spec)
	op.Commit()

	g.AddOperator(op)

	return op, nil
}

func (g *Group) AddOperator(op *Operator) {
	name := op.GetName()
	e := &opEntry{op: op, errorChan: op.GetErrorChannel()}
	g.mu.Lock()
	g.operators[name] = e
	g.mu.Unlock()

	// register the operator in the client multiplexer
	if err := g.clientMpx.RegisterClient(viewv1a1.Group(name), op.GetManager().GetClient()); err != nil {
		g.log.Error(err, "failed to register operator in the multiplex client", "operator", name)
	}

	// start the new operator if we are already running
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

	e := g.getOperatorEntry(name)
	if e == nil {
		return
	}

	if e.cancel != nil {
		e.cancel()
	}

	// unregister the operator in the client multiplexer
	if err := g.clientMpx.UnregisterClient(viewv1a1.Group(e.op.name)); err != nil {
		g.log.Error(err, "failed to unregister operator in the multiplex client", "name", e.op.name)
	}

	g.mu.Lock()
	delete(g.operators, name)
	g.mu.Unlock()
}

// Start starts the operators registered with the group. It blocks
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

	for _, e := range es {
		g.startOp(e)
	}

	<-ctx.Done()

	return nil
}

func (g *Group) startOp(e *opEntry) {
	// Store the cancel function into the operator entry
	ctx, cancel := context.WithCancel(g.ctx)
	e.cancel = cancel

	// pass the errors on to our caller
	go func() {
		for err := range e.errorChan {
			g.errorChan <- err
		}
	}()

	go e.op.Start(ctx) //nolint:errcheck
}

func (g *Group) getOperatorEntry(name string) *opEntry {
	g.mu.Lock()
	op := g.operators[name]
	g.mu.Unlock()
	return op
}

func (g *Group) newDefaultOpts() runtimeManager.Options {
	// disable leader-election, health-check and the metrics server on the embedded manager
	off := true
	return runtimeManager.Options{
		LeaderElection:         false,
		HealthProbeBindAddress: "0",
		// Disable global controller name uniquness test
		Controller: config.Controller{
			SkipNameValidation: &off,
		},
		Metrics: metricsserver.Options{
			BindAddress: "0",
		},
		Logger: g.logger,
	}
}

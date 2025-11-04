package reconciler

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	runtimeManager "sigs.k8s.io/controller-runtime/pkg/manager"
	runtimePredicate "sigs.k8s.io/controller-runtime/pkg/predicate"
	runtimeSource "sigs.k8s.io/controller-runtime/pkg/source"

	opv1a1 "github.com/l7mp/dcontroller/pkg/api/operator/v1alpha1"
	"github.com/l7mp/dcontroller/pkg/object"
	"github.com/l7mp/dcontroller/pkg/predicate"
)

var _ runtimeSource.TypedSource[Request] = &periodicRuntimeSource{}

const (
	// OneShotSourceObjectName is the name of the trigger object.
	OneShotSourceObjectName = "one-shot-trigger"

	// PeriodicSourceObjectName is the name of the trigger object.
	PeriodicSourceObjectName = "periodic-trigger"

	// VirtualSourceTriggeredLabel defines the name of the last-triggered label.
	VirtualSourceTriggeredLabel = "dcontroller.io/last-triggered"
)

// Source is a generic watch source that knows how to create controller runtime sources.
type Source interface {
	Resource
	GetSource() (runtimeSource.TypedSource[Request], error)
	Type() opv1a1.SourceType
	fmt.Stringer
}

// NewSource creates a new source resource. It dispatches to the appropriate implementation based
// on the source's Type field.
func NewSource(mgr runtimeManager.Manager, operator string, s opv1a1.Source) Source {
	sourceType := s.Type
	if sourceType == "" {
		sourceType = opv1a1.Watcher // default
	}

	switch sourceType {
	case opv1a1.OneShot:
		return NewOneShotSource(mgr, operator, s)
	case opv1a1.Periodic:
		return NewPeriodicSource(mgr, operator, s)
	case opv1a1.Watcher:
		return NewWatchSource(mgr, operator, s)
	default:
		// Fallback to watcher for unknown types
		return NewWatchSource(mgr, operator, s)
	}
}

// watchSource is a source that watches Kubernetes API resources
type watchSource struct {
	Resource
	mgr    runtimeManager.Manager
	source opv1a1.Source
	log    logr.Logger
}

// NewWatchSource creates a new Kubernetes resource watch source.
func NewWatchSource(mgr runtimeManager.Manager, operator string, s opv1a1.Source) Source {
	src := &watchSource{
		mgr:      mgr,
		source:   s,
		Resource: NewResource(mgr, operator, s.Resource),
	}

	log := mgr.GetLogger().WithName("watch-source").WithValues("name", src.Resource.String())
	src.log = log

	return src
}

// String stringifies a watch source.
func (s *watchSource) String() string { return s.Resource.String() }

// Type returns the source type.
func (s *watchSource) Type() opv1a1.SourceType { return opv1a1.Watcher }

// GetSource generates a controller-runtime source for watching Kubernetes resources.
func (s *watchSource) GetSource() (runtimeSource.TypedSource[Request], error) {
	// gvk to watch
	gvk, err := s.GetGVK()
	if err != nil {
		return nil, err
	}

	var obj client.Object = &unstructured.Unstructured{}
	obj.GetObjectKind().SetGroupVersionKind(gvk)

	// prepare the predicate
	ps := []runtimePredicate.TypedPredicate[client.Object]{}
	if s.source.Predicate != nil {
		p, err := predicate.FromPredicate(*s.source.Predicate)
		if err != nil {
			return nil, err
		}
		ps = append(ps, p)
	}

	if s.source.LabelSelector != nil {
		lp, err := predicate.FromLabelSelector(*s.source.LabelSelector)
		if err != nil {
			return nil, err
		}
		ps = append(ps, lp)
	}

	if s.source.Namespace != nil {
		ps = append(ps, predicate.FromNamespace(*s.source.Namespace))
	}

	// generic handler
	src := runtimeSource.TypedKind(s.mgr.GetCache(), obj, EventHandler[client.Object]{}, ps...)

	s.log.V(4).Info("watch source: ready", "GVK", gvk.String(), "predicate-num", len(ps))

	return src, nil
}

// oneShotSource triggers the controller exactly once when it starts with an empty object.
type oneShotSource struct {
	Resource
	mgr      runtimeManager.Manager
	client   client.Client
	operator string
	source   opv1a1.Source
	log      logr.Logger
}

// NewOneShotSource creates a new one-shot source.
func NewOneShotSource(mgr runtimeManager.Manager, operator string, s opv1a1.Source) Source {
	src := &oneShotSource{
		mgr:      mgr,
		client:   mgr.GetClient(),
		source:   s,
		operator: operator,
		Resource: NewResource(mgr, operator, s.Resource),
	}

	log := mgr.GetLogger().WithName("oneshot-source").WithValues("resource", src.Resource.String())
	src.log = log

	return src
}

// String stringifies a one-shot source.
func (s *oneShotSource) String() string { return s.Resource.String() }

// Type returns the source type.
func (s *oneShotSource) Type() opv1a1.SourceType { return opv1a1.OneShot }

// GetSource generates a controller-runtime source that triggers once.
func (s *oneShotSource) GetSource() (runtimeSource.TypedSource[Request], error) {
	s.log.V(4).Info("one-shot source: ready")

	// Create an empty object with the source's GVK
	gvk, err := s.GetGVK()
	if err != nil {
		return nil, err
	}

	// Create empty object (views are typically cluster-scoped, so no namespace)
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)
	obj.SetName(OneShotSourceObjectName)
	obj.SetLabels(map[string]string{VirtualSourceTriggeredLabel: time.Now().String()})

	// Inject into the cache so the pipeline has something to process
	if err := s.client.Create(context.TODO(), obj); err != nil {
		return nil, fmt.Errorf("failed to inject one-shot trigger: %w", err)
	}

	return runtimeSource.TypedKind[client.Object](s.mgr.GetCache(), obj,
		EventHandler[client.Object]{}), nil
}

// periodicSource is a simple timer that triggers periodic state-of-the-world reconciliation
// events.  The actual state-of-the-world reconciliation logic is handled by the controller.
type periodicSource struct {
	Resource
	mgr      runtimeManager.Manager
	source   opv1a1.Source
	operator string
	period   time.Duration
	log      logr.Logger
}

// NewPeriodicSource creates a new periodic source.
func NewPeriodicSource(mgr runtimeManager.Manager, operator string, s opv1a1.Source) Source {
	src := &periodicSource{
		mgr:      mgr,
		source:   s,
		operator: operator,
		// Periodic sources don't have a real GVK - they just trigger reconciliation
		Resource: NewResource(mgr, operator, s.Resource),
		period:   5 * time.Minute, // default period
	}

	// Extract period from parameters
	if s.Parameters != nil && s.Parameters.Raw != nil {
		var params map[string]interface{}
		if err := json.Unmarshal(s.Parameters.Raw, &params); err == nil {
			if periodStr, ok := params["period"].(string); ok {
				if d, err := time.ParseDuration(periodStr); err == nil {
					src.period = d
				}
			}
		}
	}

	log := mgr.GetLogger().WithName("periodic-source").WithValues("period", src.period)
	src.log = log

	return src
}

// String stringifies a periodic source.
func (s *periodicSource) String() string {
	return fmt.Sprintf("%s(period=%s)", s.Resource.String(), s.period.String())
}

// Type returns the source type.
func (s *periodicSource) Type() opv1a1.SourceType { return opv1a1.Periodic }

// GetSource generates a controller-runtime source that triggers periodic reconciliation.
func (s *periodicSource) GetSource() (runtimeSource.TypedSource[Request], error) {
	s.log.V(4).Info("periodic source: ready")
	return &periodicRuntimeSource{src: s, log: s.log}, nil
}

// periodicRuntimeSource is a controller-runtime source that emits periodic trigger events.
type periodicRuntimeSource struct {
	src *periodicSource
	log logr.Logger
}

// Start implements source.TypedSource and emits periodic trigger events.
// The actual SoW reconciliation logic is handled by the controller's reconciler.
func (s *periodicRuntimeSource) Start(ctx context.Context, queue workqueue.TypedRateLimitingInterface[Request]) error {
	if s.src.period <= 0 {
		return fmt.Errorf("periodic source requires positive period, got: %v", s.src.period)
	}

	// Get the GVK for the periodic trigger (just for identification)
	gvk, err := s.src.GetGVK()
	if err != nil {
		return err
	}

	ticker := time.NewTicker(s.src.period)
	go func() {
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.log.V(5).Info("triggering periodic reconciliation", "queue-len", queue.Len())

				req := Request{
					GVK:       gvk,
					Name:      PeriodicSourceObjectName,
					EventType: object.Updated,
				}
				queue.Add(req)
			}
		}
	}()

	return nil
}

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

const (
	// VirtualSourceGroup is the API group for virtual (non-Kubernetes) sources
	VirtualSourceGroup = "source.dcontroller.io"

	// VirtualSourceVersion is the version for virtual sources
	VirtualSourceVersion = "v1alpha1"
)

// Virtual source kinds
const (
	// OneShotKind is a source that triggers exactly once when the controller starts
	OneShotKind = "OneShot"

	// PeriodicKind is a source that triggers periodically based on a timer
	PeriodicKind = "Periodic"
)

// IsVirtualSource returns true if the given resource is a virtual source
func IsVirtualSource(apiGroup string) bool {
	return apiGroup == VirtualSourceGroup
}

// Source is a generic watch source that knows how to create controller runtime sources.
type Source interface {
	Resource
	GetSource() (runtimeSource.TypedSource[Request], error)
	fmt.Stringer
}

// watchSource is a source that watches Kubernetes API resources
type watchSource struct {
	Resource
	mgr    runtimeManager.Manager
	source opv1a1.Source
	log    logr.Logger
}

// newWatchSource creates a new Kubernetes resource watch source.
func newWatchSource(mgr runtimeManager.Manager, operator string, s opv1a1.Source) Source {
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

// NewSource creates a new source resource. It dispatches to the appropriate implementation
// based on the source's API group (virtual sources vs Kubernetes resource watches).
func NewSource(mgr runtimeManager.Manager, operator string, s opv1a1.Source) Source {
	// Check if this is a virtual source
	apiGroup := s.Group
	if apiGroup == nil {
		apiGroup = new(string)
		*apiGroup = ""
	}

	if IsVirtualSource(*apiGroup) {
		// Dispatch to virtual source implementation
		kind := s.Kind
		switch kind {
		case OneShotKind:
			return newOneShotSource(mgr, operator, s)
		case PeriodicKind:
			return newPeriodicSource(mgr, operator, s)
		default:
			// Unknown virtual source kind - fall back to watch source
			// (will likely fail during GetSource())
			return newWatchSource(mgr, operator, s)
		}
	}

	// Regular Kubernetes resource watch
	return newWatchSource(mgr, operator, s)
}

// oneShotSource is a virtual source that triggers exactly once when the controller starts.
type oneShotSource struct {
	Resource
	mgr    runtimeManager.Manager
	source opv1a1.Source
	log    logr.Logger
}

// newOneShotSource creates a new one-shot source.
func newOneShotSource(mgr runtimeManager.Manager, operator string, s opv1a1.Source) Source {
	src := &oneShotSource{
		mgr:      mgr,
		source:   s,
		Resource: NewResource(mgr, operator, s.Resource),
	}

	log := mgr.GetLogger().WithName("oneshot-source").WithValues("name", src.Resource.String())
	src.log = log

	return src
}

// String stringifies a one-shot source.
func (s *oneShotSource) String() string { return s.Resource.String() }

// GetSource generates a controller-runtime source that triggers once.
func (s *oneShotSource) GetSource() (runtimeSource.TypedSource[Request], error) {
	s.log.V(4).Info("one-shot source: ready")
	return &oneShotRuntimeSource{}, nil
}

// oneShotRuntimeSource is a controller-runtime source that triggers exactly once.
type oneShotRuntimeSource struct{}

// Start implements source.TypedSource and triggers a single reconciliation event.
func (s *oneShotRuntimeSource) Start(ctx context.Context, queue workqueue.TypedRateLimitingInterface[Request]) error {
	go func() {
		// Virtual sources don't have a specific object, so we use empty values
		queue.Add(Request{
			Name:      "OneShotEvent",
			Namespace: "",
			EventType: object.Added,
		})
	}()

	return nil
}

// periodicSource is a virtual source that triggers periodically based on a timer.
type periodicSource struct {
	Resource
	mgr    runtimeManager.Manager
	source opv1a1.Source
	period time.Duration
	log    logr.Logger
}

// newPeriodicSource creates a new periodic source.
func newPeriodicSource(mgr runtimeManager.Manager, operator string, s opv1a1.Source) Source {
	src := &periodicSource{
		mgr:      mgr,
		source:   s,
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

	log := mgr.GetLogger().WithName("periodic-source").
		WithValues("name", src.Resource.String(), "period", src.period)
	src.log = log

	return src
}

// String stringifies a periodic source.
func (s *periodicSource) String() string { return s.Resource.String() }

// GetSource generates a controller-runtime source that triggers periodically.
func (s *periodicSource) GetSource() (runtimeSource.TypedSource[Request], error) {
	s.log.V(4).Info("periodic source: ready")
	return &periodicRuntimeSource{period: s.period}, nil
}

// periodicRuntimeSource is a controller-runtime source that triggers on a timer.
type periodicRuntimeSource struct {
	period time.Duration
}

// Start implements source.TypedSource and triggers periodic reconciliation events.
func (s *periodicRuntimeSource) Start(ctx context.Context, queue workqueue.TypedRateLimitingInterface[Request]) error {
	if s.period <= 0 {
		return fmt.Errorf("periodic source requires positive period, got: %v", s.period)
	}

	go func() {
		// Create ticker
		ticker := time.NewTicker(s.period)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Enqueue a reconciliation request directly
				// Virtual sources don't have a specific object, so we use empty values
				queue.Add(Request{
					Name:      "PeriodicEvent",
					Namespace: "",
					EventType: object.Added,
				})
			}
		}
	}()

	return nil
}

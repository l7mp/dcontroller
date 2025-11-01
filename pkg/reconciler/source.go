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
	viewv1a1 "github.com/l7mp/dcontroller/pkg/api/view/v1alpha1"
	"github.com/l7mp/dcontroller/pkg/object"
	"github.com/l7mp/dcontroller/pkg/predicate"
)

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
	fmt.Stringer
}

// NewSource creates a new source resource. It dispatches to the appropriate implementation based
// on the source's API group (virtual sources vs Kubernetes resource watches).
func NewSource(mgr runtimeManager.Manager, operator string, s opv1a1.Source) Source {
	apiGroup := ""
	if s.Group != nil {
		apiGroup = *s.Group
	}

	switch apiGroup {
	case opv1a1.OneShotSourceGroupVersion.Group:
		return newOneShotSource(mgr, operator, s)
	case opv1a1.PeriodicSourceGroupVersion.Group:
		return newPeriodicSource(mgr, operator, s)
	default:
		return newWatchSource(mgr, operator, s)
	}
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

// oneShotSource is a virtual operator that triggers the controller exactly once when the
// controller starts with an empty resource.
type oneShotSource struct {
	Resource
	mgr      runtimeManager.Manager
	client   client.Client
	operator string
	source   opv1a1.Source
	log      logr.Logger
}

// newOneShotSource creates a new one-shot source.
func newOneShotSource(mgr runtimeManager.Manager, operator string, s opv1a1.Source) Source {
	objGVK := viewv1a1.GroupVersionKind(operator, s.Kind)
	src := &oneShotSource{
		mgr:      mgr,
		client:   mgr.GetClient(),
		source:   s,
		operator: operator,
		Resource: NewResource(mgr, operator, opv1a1.Resource{
			Group: &objGVK.Group,
			Kind:  objGVK.Kind,
		}),
	}

	log := mgr.GetLogger().WithName("oneshot-source").WithValues("kind", s.Kind)
	src.log = log

	return src
}

// String stringifies a one-shot source.
func (s *oneShotSource) String() string { return s.Resource.String() }

// GetSource generates a controller-runtime source that triggers once.
func (s *oneShotSource) GetSource() (runtimeSource.TypedSource[Request], error) {
	s.log.V(4).Info("one-shot source: ready")

	// Create an empty object in the operator's view space, with the requested Kind.
	gvk, err := s.GetGVK()
	if err != nil {
		return nil, err
	}

	// Inject into the view cache
	obj := object.NewViewObject(s.operator, gvk.Kind)
	obj.SetName(OneShotSourceObjectName)
	obj.SetLabels(map[string]string{VirtualSourceTriggeredLabel: time.Now().String()})
	if err := s.client.Create(context.TODO(), obj); err != nil {
		return nil, fmt.Errorf("failed to inject initial trigger")
	}

	return runtimeSource.TypedKind[client.Object](s.mgr.GetCache(), obj,
		EventHandler[client.Object]{}), nil
}

// periodicSource is a virtual operator that triggers periodically based on a timer with inhecting
// an empty object into the parent operator's view cache.
type periodicSource struct {
	Resource
	mgr      runtimeManager.Manager
	client   client.Client
	source   opv1a1.Source
	operator string
	period   time.Duration
	log      logr.Logger
}

// newPeriodicSource creates a new periodic source.
func newPeriodicSource(mgr runtimeManager.Manager, operator string, s opv1a1.Source) Source {
	objGVK := viewv1a1.GroupVersionKind(operator, s.Kind)
	src := &periodicSource{
		mgr:      mgr,
		client:   mgr.GetClient(),
		source:   s,
		operator: operator,
		Resource: NewResource(mgr, operator, opv1a1.Resource{
			Group: &objGVK.Group,
			Kind:  objGVK.Kind,
		}),
		period: 5 * time.Minute, // default period
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

	log := mgr.GetLogger().WithName("periodic-source").WithValues("kind", s.Kind)
	src.log = log

	return src
}

// String stringifies a periodic source.
func (s *periodicSource) String() string {
	return fmt.Sprintf("%s(period=%s)", s.Resource.String(), s.period.String())
}

// GetSource generates a controller-runtime source that triggers periodically.
func (s *periodicSource) GetSource() (runtimeSource.TypedSource[Request], error) {
	s.log.V(4).Info("periodic source: ready")
	return &periodicRuntimeSource{src: s, client: s.mgr.GetClient(), log: s.log}, nil
}

// periodicRuntimeSource is a controller-runtime source that triggers on a timer.
type periodicRuntimeSource struct {
	src    *periodicSource
	client client.Client
	log    logr.Logger
}

// Start implements source.TypedSource and triggers periodic reconciliation events.
func (s *periodicRuntimeSource) Start(ctx context.Context, queue workqueue.TypedRateLimitingInterface[Request]) error {
	if s.src.period <= 0 {
		return fmt.Errorf("periodic source requires positive period, got: %v", s.src.period)
	}

	// Create an empty object in the operator's view space, with the requested Kind.
	gvk, err := s.src.GetGVK()
	if err != nil {
		return err
	}

	// Triggering source
	obj := object.NewViewObject(s.src.operator, gvk.Kind)
	obj.SetName(PeriodicSourceObjectName)
	src := runtimeSource.TypedKind[client.Object](s.src.mgr.GetCache(), obj,
		EventHandler[client.Object]{})

	ticker := time.NewTicker(s.src.period)
	go func() {
		defer ticker.Stop()

		started := false
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				obj.SetLabels(map[string]string{VirtualSourceTriggeredLabel: time.Now().String()})
				if started {
					started = true
					if err := s.client.Update(ctx, obj); err != nil {
						s.log.Error(err, "failed to update trigger")
					}
				} else {
					if err := s.client.Create(ctx, obj); err != nil {
						s.log.Error(err, "failed to inject initial trigger")
					}
				}
			}
		}
	}()

	return src.Start(ctx, queue)
}

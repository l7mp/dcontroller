package reconciler

import (
	"fmt"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	runtimeManager "sigs.k8s.io/controller-runtime/pkg/manager"
	runtimePredicate "sigs.k8s.io/controller-runtime/pkg/predicate"
	runtimeSource "sigs.k8s.io/controller-runtime/pkg/source"

	opv1a1 "github.com/l7mp/dcontroller/pkg/api/operator/v1alpha1"
	"github.com/l7mp/dcontroller/pkg/predicate"
)

// Source is a generic watch source that knows how to create controller runtime sources.
type Source interface {
	Resource
	GetSource() (runtimeSource.TypedSource[Request], error)
	fmt.Stringer
}

type source struct {
	Resource
	mgr    runtimeManager.Manager
	source opv1a1.Source
	log    logr.Logger
}

// NewSource creates a new source resource.
func NewSource(mgr runtimeManager.Manager, operator string, s opv1a1.Source) Source {
	src := &source{
		mgr:      mgr,
		source:   s,
		Resource: NewResource(mgr, operator, s.Resource),
	}

	log := mgr.GetLogger().WithName("source").WithValues("name", src.Resource.String())
	src.log = log

	return src
}

// String stringifies a source.
func (s *source) String() string { return s.Resource.String() }

// GetSource generates a controller-runtime source.
func (s *source) GetSource() (runtimeSource.TypedSource[Request], error) {
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

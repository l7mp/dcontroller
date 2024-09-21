package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"hsnlab/dcontroller-runtime/pkg/pipeline"
	"hsnlab/dcontroller-runtime/pkg/predicate"
)

// Operator is a set of related controllers with a unique name.
type Operator struct {
	// Name is the unique name of the operator.
	Name string `json:"name"`
	// The set of controllers run by this opeator.
	Controllers []Controller `json:"conterollers"`
}

// Controller is a translator that processes a set of base resources via a declarative pipeline
// into a delta on the target resource. A controller is defined by a name, a set of sources, a
// processing pipeline and a target.
type Controller struct {
	// Name is the unique name of the controller.
	Name string `json:"name"`
	// The base resource(s) the controller watches.
	Sources []Source `json:"sources"`
	// Pipeline is an aggregation pipeline applied to base objects.
	Pipeline pipeline.Pipeline `json:"pipeline"`
	// The target resource the results are to be added.
	Target Target `json:"target"`
}

// Resource specifies a resource by the GVK.
type Resource struct {
	Group   *string `json:"apiGroup,omitempty"`
	Version *string `json:"version,omitempty"`
	Kind    string  `json:"kind"`
}

// Source is a watch source that feeds deltas into the controller.
type Source struct {
	Resource
	Namespace     *string               `json:"namespace,omitempty"`
	LabelSelector *metav1.LabelSelector `json:"labelSelector,omitempty"`
	Predicate     *predicate.Predicate  `json:"predicate,omitempty"`
}

// Target is the target reource type in which the controller writes.
type Target struct {
	Resource
	Type TargetType `json:"type,omitempty"`
}

type TargetType string

const (
	// Updates defines a target type that will fully overwrite the target resource with the
	// controller ouutput.
	Updater TargetType = "Updater"
	// Updates defines a target type that will applies the fully overwrite the target resource.
	Patcher TargetType = "Patcher"
)

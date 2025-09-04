package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/l7mp/dcontroller/pkg/expression"
	"github.com/l7mp/dcontroller/pkg/predicate"
)

// Controller is a translator that processes a set of base resources via a declarative pipeline
// into a delta on the target resource. A controller is defined by a name, a set of sources, a
// processing pipeline and a target.
type Controller struct {
	// Name is the unique name of the controller.
	Name string `json:"name"`
	// The base resource(s) the controller watches.
	Sources []Source `json:"sources"`
	// Pipeline is an aggregation pipeline applied to base objects.
	//
	// +kubebuilder:validation:Schemaless
	// +kubebuilder:pruning:PreserveUnknownFields
	Pipeline Pipeline `json:"pipeline"`
	// The target resource the results are to be added.
	Target Target `json:"target"`
}

// Pipeline is an optional join followed by an aggregation.
type Pipeline struct {
	*Join        `json:",inline"`
	*Aggregation `json:",inline"`
}

// Join is an operation that can be used to perform an inner join on a list of views.
type Join struct {
	Expression expression.Expression `json:"@join"`
}

// Aggregation is an operation that can be used to process, objects, or alter the shape of a list
// of objects in a view.
type Aggregation struct {
	Expressions []expression.Expression `json:"@aggregate"`
}

// Resource specifies a resource by the GVK.
type Resource struct {
	// Group is the API group. Default is "<operator-name>.view.dcontroller.io", where
	// <operator-name> is the name of the operator that manages the object.
	Group *string `json:"apiGroup,omitempty"`
	// Version is the version of the resource. Optional.
	Version *string `json:"version,omitempty"`
	// Kind is the type of the resource. Mandatory.
	Kind string `json:"kind"`
}

// Source is a watch source that feeds deltas into the controller.
type Source struct {
	Resource `json:",inline"`
	// Namespace, if given, restricts the source to generate events only from the given namespace.
	Namespace *string `json:"namespace,omitempty"`
	// LabelSelector is an optional label selector to filter events on this source.
	LabelSelector *metav1.LabelSelector `json:"labelSelector,omitempty"`
	// Predicate is a controller runtime predicate for filtering events on this source.
	//
	// +kubebuilder:validation:Schemaless
	// +kubebuilder:pruning:PreserveUnknownFields
	Predicate *predicate.Predicate `json:"predicate,omitempty"`
}

// Target is the target reource type in which the controller writes.
type Target struct {
	Resource `json:",inline"`
	// Type is the type of the target.
	Type TargetType `json:"type,omitempty"`
}

// TargetType represents the type of a target.
type TargetType string

const (
	// Updater is a target that will fully overwrite the target resource with the update.
	Updater TargetType = "Updater"
	// Patcher is a target that applies the update as a patch to the target resource.
	Patcher TargetType = "Patcher"
)

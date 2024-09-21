package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"hsnlab/dcontroller-runtime/pkg/pipeline"
	"hsnlab/dcontroller-runtime/pkg/predicate"
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
	Pipeline pipeline.Pipeline `json:"pipeline"`
	// The target resource the results are to be added.
	Target Target `json:"target"`
}

// Resource specifies a resource by the GVK.
type Resource struct {
	// Group is the API group. Default is "view.dcontroller.io".
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
	// Predicate is a controller runtime predicate for filtering events on this source..
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

type TargetType string

const (
	// Updates defines a target type that will fully overwrite the target resource with the
	// controller ouutput.
	Updater TargetType = "Updater"
	// Updates defines a target type that will applies the fully overwrite the target resource.
	Patcher TargetType = "Patcher"
)

package v1alpha1

import (
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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
	// Pipeline is an processing pipeline applied to base objects.
	//
	// +kubebuilder:validation:Schemaless
	// +kubebuilder:pruning:PreserveUnknownFields
	Pipeline Pipeline `json:"pipeline"`
	// The target resource the results are to be added.
	Target Target `json:"target"`
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
	// Type specifies the behavior of the source. Default is Watcher.
	Type SourceType `json:"type,omitempty"`
	// Namespace, if given, restricts the source to generate events only from the given namespace.
	Namespace *string `json:"namespace,omitempty"`
	// LabelSelector is an optional label selector to filter events on this source.
	LabelSelector *metav1.LabelSelector `json:"labelSelector,omitempty"`
	// Predicate is a controller runtime predicate for filtering events on this source.
	//
	// +kubebuilder:validation:Schemaless
	// +kubebuilder:pruning:PreserveUnknownFields
	Predicate *predicate.Predicate `json:"predicate,omitempty"`
	// Parameters contains arbitrary source-specific parameters for virtual sources.
	// For example, Periodic sources use {"period": "5m"}.
	//
	// +kubebuilder:validation:Schemaless
	// +kubebuilder:pruning:PreserveUnknownFields
	// +optional
	Parameters *apiextensionsv1.JSON `json:"parameters,omitempty"`
}

// SourceType represents the type of a source.
type SourceType string

const (
	// Watcher is a source that watches Kubernetes resources and performs incremental reconciliation.
	Watcher SourceType = "Watcher"
	// Periodic is a source that triggers state-of-the-world reconciliation for all other sources.
	Periodic SourceType = "Periodic"
	// OneShot is a source that emits a single empty object for initialization.
	OneShot SourceType = "OneShot"
)

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

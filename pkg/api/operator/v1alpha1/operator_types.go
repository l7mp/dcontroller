package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func init() {
	SchemeBuilder.Register(&Operator{}, &OperatorList{})
}

// Operator is an abstraction of a basic unit of automation, a set of related controllers working
// on a single shared view of system resources.
//
// +genclient:nonNamespaced
// +kubebuilder:object:root=true
// +kubebuilder:resource:categories=dcontroller,scope=Cluster,shortName=operators
// +kubebuilder:subresource:status
// +kubebuilder:storageversion
// // +kubebuilder:printcolumn:name="ControllerNum",type=integer,JSONPath=`length(.spec.controllers)`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type Operator struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Spec defines the desired state of an operator.
	Spec OperatorSpec `json:"spec"`

	// Status defines the current state of the operator.
	Status OperatorStatus `json:"status,omitempty"`
}

// OperatorSpec defines the desired state of an operator.
type OperatorSpec struct {
	// Controllers is a list of controllers that collectively implement the operator.
	//
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=255
	Controllers []Controller `json:"controllers"`
}

// +kubebuilder:object:root=true

// OperatorList contains a list of operators.
type OperatorList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Operator `json:"items"`
}

// OperatorStatus specifies the status of an operator.
type OperatorStatus struct {
	Controllers []ControllerStatus `json:"controllers"`
}

// ControllerStatus specifies the status of a controller.
type ControllerStatus struct {
	Name       string             `json:"name"`
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	LastErrors []string           `json:"lastErrors,omitempty"`
}

// ControllerConditionType is a type of condition associated with a Controller. This type should be
// used with the ControllerStatus.Conditions field.
type ControllerConditionType string

// ControllerConditionReason defines the set of reasons that explain why a particular Controller
// condition type has been raised.
type ControllerConditionReason string

// const (
// 	// This condition is true when Controller is syntactically and semantically valid enough to
// 	// be able to start.
// 	//
// 	// Possible reasons for this condition to be True are:
// 	//
// 	// * "Accepted"
// 	//
// 	// Possible reasons for this condition to be False are:
// 	//
// 	// * "Invalid"
// 	//
// 	// Controllers may raise this condition with other reasons, but should prefer to use the
// 	// reasons listed above to improve interoperability.
// 	ControllerConditionAccepted ControllerConditionType = "Accepted"

// 	// This reason is used with the "Accepted" condition when the condition is True.
// 	ControllerReasonAccepted ControllerConditionReason = "Accepted"

// 	// This reason is used with the "Accepted" condition when the Controller specification is
// 	// not valid.
// 	ControllerReasonInvalid ControllerConditionReason = "Invalid"
// )

const (
	// The Ready condition is set if the Controller is running and actively reconciles
	// resources.
	//
	// Possible reasons for this condition to be true are:
	//
	// * "Ready"
	//
	// Possible reasons for this condition to be False are:
	//
	// * "ReconcileError"
	//
	// Controllers may raise this condition with other reasons, but should prefer to use the
	// reasons listed above to improve interoperability.

	// ControllerConditionReady represents the Ready condition.
	ControllerConditionReady ControllerConditionType = "Ready"

	// ControllerReasonReady is used with the "Ready" condition when the condition is true.
	ControllerReasonReady ControllerConditionReason = "Ready"

	// ControllerReasonReconciliationFailed is used with the "Ready" condition when
	// reconciliation has failed for some input resources.
	ControllerReasonReconciliationFailed ControllerConditionReason = "ReconciliationFailed"

	// ControllerReasonNotReady is used with the "Ready" condition when the controller is not
	// ready for processing events.
	ControllerReasonNotReady ControllerConditionReason = "NotReady"
)

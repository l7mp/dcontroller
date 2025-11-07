package controller

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	runtimeCtrl "sigs.k8s.io/controller-runtime/pkg/controller"

	opv1a1 "github.com/l7mp/dcontroller/pkg/api/operator/v1alpha1"
	dreconciler "github.com/l7mp/dcontroller/pkg/reconciler"
)

type RuntimeController = runtimeCtrl.TypedController[dreconciler.Request]

// NativeController wraps a traditional controller-runtime controller to integrate with the
// Δ-controller operator framework. This allows Go-based controllers to coexist with declarative
// controllers in the same operator.
type NativeController struct {
	name string
	ctrl RuntimeController
	gvks []schema.GroupVersionKind
}

var _ Controller = &NativeController{}

// NewNative creates a new native controller wrapper around a controller-runtime controller.
func NewNative(name string, ctrl RuntimeController, gvks []schema.GroupVersionKind) (Controller, error) {
	c := &NativeController{
		name: name,
		ctrl: ctrl,
		gvks: gvks,
	}
	return c, nil
}

// GetName returns the name of the controller.
func (c *NativeController) GetName() string {
	return c.name
}

// GetGVKs returns the GVKs of the resources managed by this controller.
func (c *NativeController) GetGVKs() []schema.GroupVersionKind {
	return c.gvks
}

// GetStatus returns the status of the controller.
// For native controllers, we provide basic status reporting.
func (c *NativeController) GetStatus(gen int64) opv1a1.ControllerStatus {
	status := opv1a1.ControllerStatus{Name: c.name}

	// Native controllers are assumed to be ready if they were successfully created.
	condition := metav1.Condition{
		Type:               string(opv1a1.ControllerConditionReady),
		Status:             metav1.ConditionTrue,
		ObservedGeneration: gen,
		LastTransitionTime: metav1.Now(),
		Reason:             string(opv1a1.ControllerReasonReady),
		Message:            "Native controller is running",
	}

	status.Conditions = []metav1.Condition{condition}
	status.LastErrors = []string{}

	return status
}

// Package controller implements the core Δ-controller controller runtime that processes
// declarative pipeline specifications and manages incremental view reconciliation.
//
// This package provides the main controller abstraction that bridges declarative YAML
// specifications with imperative Go reconciliation logic. Controllers watch source Kubernetes
// resources, process them through declarative pipelines to generate view objects, and manage
// target resource updates.
//
// Controllers support:
//   - multiple source resource types with optional label selectors,
//   - declarative pipeline processing with joins and aggregations,
//   - incremental reconciliation using DBSP,
//   - configurable error handling and status reporting, and
//   - integration with the composite client system.
//
// Example usage:
//
//	ctrl, _ := controller.New("my-controller", mgr, controller.Config{
//	    Sources: []opv1a1.Source{{Kind: "Pod"}},
//	    Target: opv1a1.Target{Kind: "PodView"},
//	    Pipeline: opv1a1.Pipeline{...},
//	})
package controller

import (
	"k8s.io/apimachinery/pkg/runtime/schema"

	opv1a1 "github.com/l7mp/dcontroller/pkg/api/operator/v1alpha1"
)

// Options defines the controller configuration.
type Options struct {
	// ErrorChannel is a channel to receive errors from the controller.
	ErrorChan chan error
}

// Controller is the interface that all controller implementations must satisfy.
// It provides the core API for managing controller lifecycle, status reporting,
// and GVK registration.
type Controller interface {
	// GetName returns the name of the controller.
	GetName() string

	// GetGVKs returns the GVKs of the views registered with the controller.
	GetGVKs() []schema.GroupVersionKind

	// GetStatus returns the status of the controller.
	GetStatus(gen int64) opv1a1.ControllerStatus
}

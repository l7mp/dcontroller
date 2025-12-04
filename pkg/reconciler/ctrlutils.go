package reconciler

import (
	"context"
	"fmt"

	"github.com/l7mp/dcontroller/pkg/object"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// CreateOrUpdate creates or updates the given object in the Kubernetes cluster. The object's
// desired state must be reconciled with the existing state inside the passed in callback MutateFn.
//
// The MutateFn is called regardless of creating or updating an object.
//
// It returns the executed operation and an error.
//
// Note: this version differs from default controllerutil.CreateOrUpdate in two ways:
//   - it uses the unstructured API via object.Object
//   - errors produced by the `Create` branch (after a failed `Get`) will be ignored
//
// Status handling: For resources with status as a subresource (native Kubernetes objects like
// Pods), client.Update() does NOT update status. This function saves the status before Update(),
// then restores and updates it via Status().Update(). For resources without status subresource
// (like view objects), this redundantly updates status twice but is harmless.
func CreateOrUpdate(ctx context.Context, c client.Client, obj object.Object, f controllerutil.MutateFn) (controllerutil.OperationResult, error) {
	key := client.ObjectKeyFromObject(obj)
	if err := c.Get(ctx, key, obj); err != nil {
		if !apierrors.IsNotFound(err) {
			return controllerutil.OperationResultNone, err
		}
		if err := mutate(f, key, obj); err != nil {
			return controllerutil.OperationResultNone, err
		}
		if err := c.Create(ctx, obj); err != nil {
			// this is not an error: default to the update branch
			goto update
		}
		return controllerutil.OperationResultCreated, nil
	}

update:
	if err := mutate(f, key, obj); err != nil {
		return controllerutil.OperationResultNone, err
	}

	// Update may rewrite out status
	newStatus, hasStatus, _ := unstructured.NestedMap(obj.UnstructuredContent(), "status")

	if err := c.Update(ctx, obj); err != nil {
		return controllerutil.OperationResultNone, err
	}

	// take care of the status just now
	if hasStatus {
		if err := unstructured.SetNestedMap(obj.UnstructuredContent(), newStatus, "status"); err == nil {
			if err := c.Status().Update(ctx, obj); err != nil {
				return controllerutil.OperationResultNone, err
			}
		}
	}

	return controllerutil.OperationResultUpdated, nil
}

// Update updates the given object in the Kubernetes cluster, including status if present.
//
// This function properly handles status updates for both:
//   - Native Kubernetes objects (Pods, etc.) where status is a subresource
//   - View objects where status is not a subresource
//
// Status handling: For resources with status as a subresource (native Kubernetes objects),
// client.Update() does NOT update status. This function saves the status before Update(),
// then restores and updates it via Status().Update(). For resources without status subresource
// (like view objects), this redundantly updates status twice but is harmless.
func Update(ctx context.Context, c client.Client, obj object.Object) error {
	// Save status before Update() because for resources with status as a subresource
	// (native Kubernetes objects), client.Update() will clear the status field.
	savedStatus, hasStatus, _ := unstructured.NestedMap(obj.UnstructuredContent(), "status")

	if err := c.Update(ctx, obj); err != nil {
		return err
	}

	// Restore and update status if it was present.
	// For resources with status as a subresource: Update() cleared it, so we restore and update.
	// For resources without status subresource: This redundantly updates status again (harmless).
	if hasStatus {
		if err := unstructured.SetNestedMap(obj.UnstructuredContent(), savedStatus, "status"); err == nil {
			if err := c.Status().Update(ctx, obj); err != nil {
				return err
			}
		}
	}

	return nil
}

func mutate(f controllerutil.MutateFn, key client.ObjectKey, obj client.Object) error {
	if err := f(); err != nil {
		return err
	}
	if newKey := client.ObjectKeyFromObject(obj); key != newKey {
		return fmt.Errorf("MutateFn cannot mutate object name and/or object namespace")
	}
	return nil
}

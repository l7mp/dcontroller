package reconciler

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/l7mp/dcontroller/pkg/object"
)

// CreateOrUpdate creates or updates the given object in the Kubernetes cluster. The object's
// desired state must be reconciled with the existing state inside the passed in callback MutateFn.
//
// The MutateFn is called regardless of creating or updating an object.
//
// It returns the executed operation and an error.
//
// Note: this version differs from default controllerutil.CreateOrUpdate in two subtle ways
//   - it uses the unstructured API via object.Object,
//   - changes made by MutateFn to the status subresource will be handled, changes to any other
//     sub-resource will be discarded,
//   - errors produced by the `Create` branch (after a failed `Get`) will be ignored.
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

func mutate(f controllerutil.MutateFn, key client.ObjectKey, obj client.Object) error {
	if err := f(); err != nil {
		return err
	}
	if newKey := client.ObjectKeyFromObject(obj); key != newKey {
		return fmt.Errorf("MutateFn cannot mutate object name and/or object namespace")
	}
	return nil
}

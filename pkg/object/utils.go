package object

import (
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"

	"github.com/hsnlab/dcontroller/pkg/util"
)

var scheme = runtime.NewScheme()

// Dump converts an unstuctured object into a human-readable form.
func Dump(obj Object) string {
	// copy
	ro := DeepCopy(obj)

	// strip useless stuff
	as := ro.GetAnnotations()
	if _, ok := as["kubectl.kubernetes.io/last-applied-configuration"]; ok {
		delete(as, "kubectl.kubernetes.io/last-applied-configuration")
		ro.SetAnnotations(as)
	}
	ro.SetManagedFields(nil)

	output := util.Stringify(ro)

	return output
}

// ConvertRuntimeObjectToClientObject converts a core runtime objects into a full client.Object.
func ConvertRuntimeObjectToClientObject(runtimeObj runtime.Object) (client.Object, error) {
	// Try direct type assertion first
	if clientObj, ok := runtimeObj.(client.Object); ok {
		return clientObj, nil
	}

	// Get the GVK for the runtime.Object
	gvk, err := apiutil.GVKForObject(runtimeObj, scheme)
	if err != nil {
		return nil, fmt.Errorf("failed to get GVK: %w", err)
	}

	// Create a new object of the correct type
	newObj, err := scheme.New(gvk)
	if err != nil {
		return nil, fmt.Errorf("failed to create new object: %w", err)
	}

	// Convert the runtime.Object to the new object
	if err := scheme.Convert(runtimeObj, newObj, nil); err != nil {
		return nil, fmt.Errorf("failed to convert object: %w", err)
	}

	// Assert the new object as client.Object
	clientObj, ok := newObj.(client.Object)
	if !ok {
		return nil, fmt.Errorf("converted object is not a client.Object")
	}

	// Copy metadata if the original object implements metav1.Object
	if metaObj, ok := runtimeObj.(metav1.Object); ok {
		clientObj.SetName(metaObj.GetName())
		clientObj.SetNamespace(metaObj.GetNamespace())
		clientObj.SetLabels(metaObj.GetLabels())
		clientObj.SetAnnotations(metaObj.GetAnnotations())
		clientObj.SetResourceVersion(metaObj.GetResourceVersion())
		clientObj.SetUID(metaObj.GetUID())
	}

	return clientObj, nil
}

// GetBaseScheme returns a base scheme. Used mostly for testing.
func GetBaseScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	corev1.AddToScheme(scheme) //nolint:errcheck
	appsv1.AddToScheme(scheme) //nolint:errcheck
	return scheme
}

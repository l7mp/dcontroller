package object

import (
	"encoding/json"
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func (obj *Object) String() string {
	// copy
	ro := obj.DeepCopy()

	// strip useless stuff
	as := ro.GetAnnotations()
	if _, ok := as["kubectl.kubernetes.io/last-applied-configuration"]; ok {
		delete(as, "kubectl.kubernetes.io/last-applied-configuration")
		ro.SetAnnotations(as)
	}
	ro.SetManagedFields(nil)

	// default dump
	output := fmt.Sprintf("%#v", ro)

	if json, err := json.Marshal(ro); err == nil {
		output = string(json)
	}

	return output
}

// DumpUnstructured convers an unstuctured object into a human-readable form.
func DumpUnstructured(obj *unstructured.Unstructured) string {
	// copy
	ro := obj.DeepCopy()

	// strip useless stuff
	as := ro.GetAnnotations()
	if _, ok := as["kubectl.kubernetes.io/last-applied-configuration"]; ok {
		delete(as, "kubectl.kubernetes.io/last-applied-configuration")
		ro.SetAnnotations(as)
	}
	ro.SetManagedFields(nil)

	// default dump
	output := fmt.Sprintf("%#v", ro)

	if json, err := json.Marshal(ro); err == nil {
		output = string(json)
	}

	return output
}

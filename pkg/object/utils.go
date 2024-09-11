package object

import (
	"encoding/json"
	"fmt"

	"hsnlab/dcontroller-runtime/pkg/util"
)

func (obj *ViewObject) String() string {
	// // copy
	// ro := obj.DeepCopy()

	// // strip useless stuff
	// as := ro.GetAnnotations()
	// if _, ok := as["kubectl.kubernetes.io/last-applied-configuration"]; ok {
	// 	delete(as, "kubectl.kubernetes.io/last-applied-configuration")
	// 	ro.SetAnnotations(as)
	// }
	// ro.SetManagedFields(nil)

	// // default dump
	// output := fmt.Sprintf("%#v", ro)
	output := fmt.Sprintf("%#v", obj)
	if json, err := json.Marshal(obj); err == nil {
		output = string(json)
	}

	return output
}

// DumpObject convers an unstuctured object into a human-readable form.
func DumpObject(obj Object) string {
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

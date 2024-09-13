package object

import (
	"hsnlab/dcontroller-runtime/pkg/util"
)

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

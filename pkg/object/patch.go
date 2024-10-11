package object

import (
	"fmt"
	"reflect"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
)

// Patch performs an in-place patch.
func Patch(obj Object, m map[string]any) error {
	res := patch(obj.UnstructuredContent(), m)

	m, ok := res.(map[string]any)
	if !ok {
		return fmt.Errorf("patch result: expected map[string]any but obtained %#v", res)
	}

	obj.SetUnstructuredContent(m)

	return nil
}

func patch(o, m any) any {
	if reflect.DeepEqual(o, m) {
		return DeepCopyAny(m)
	}

	if o == nil {
		return DeepCopyAny(m)
	}

	switch x := m.(type) {
	case bool, int64, float64, string:
		return x

	case []any:
		litlm := x
		litlo, ok2 := o.([]any)
		if !ok2 {
			return DeepCopyAny(litlm)
		}

		retl := DeepCopyAny(litlo).([]any)
		for i := range litlm {
			if i >= len(litlo) {
				retl = append(retl, DeepCopyAny(litlm[i]))
				continue
			}
			if reflect.DeepEqual(litlo[i], litlm[i]) {
				retl[i] = DeepCopyAny(litlm[i])
				continue
			}
			retl[i] = patch(litlo[i], litlm[i])
		}
		return retl

	case map[string]any:
		litmm := x
		litmo, ok2 := o.(map[string]any)
		if !ok2 {
			return DeepCopyAny(litmm)
		}

		retm := DeepCopyAny(litmo).(map[string]any)
		for k, v := range litmm {
			if v == nil {
				delete(retm, k)
				continue
			}

			vo, ok := litmo[k]
			if !ok {
				retm[k] = DeepCopyAny(v)
				continue
			}

			if reflect.DeepEqual(vo, v) {
				retm[k] = DeepCopyAny(v)
				continue
			}

			v := patch(vo, v)
			retm[k] = v
		}
		// for k, v := range litmo {
		// 	_, ok := litmm[k]
		// 	if !ok {
		// 		retm[k] = deepCopy(v)
		// 		continue
		// 	}
		// }
		return retm
	default:
		// this should never happen so we should panic here but we won't
		return nil
	}
}

// ApplyStrategicMergePatch is a partial local re-implementation of strategic merge patches.
func ApplyStrategicMergePatch(original, patch *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	originalData, err := runtime.Encode(unstructured.UnstructuredJSONScheme, original)
	if err != nil {
		return nil, err
	}

	patchData, err := runtime.Encode(unstructured.UnstructuredJSONScheme, patch)
	if err != nil {
		return nil, err
	}

	patchedData, err := strategicpatch.StrategicMergePatch(originalData, patchData, &unstructured.Unstructured{})
	if err != nil {
		return nil, err
	}

	patched := &unstructured.Unstructured{}
	if err := runtime.DecodeInto(unstructured.UnstructuredJSONScheme, patchedData, patched); err != nil {
		return nil, err
	}

	return patched, nil
}

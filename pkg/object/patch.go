package object

import (
	"fmt"
	"reflect"
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
		return deepCopy(m)
	}

	if o == nil {
		return deepCopy(m)
	}

	switch m.(type) {
	case bool, int64, float64, string:
		return m

	case []any:
		litlm := m.([]any)
		litlo, ok2 := o.([]any)
		if !ok2 {
			return deepCopy(litlm)
		}

		retl := deepCopy(litlo).([]any)
		for i := range litlm {
			if i >= len(litlo) {
				retl = append(retl, deepCopy(litlm[i]))
				continue
			}
			if reflect.DeepEqual(litlo[i], litlm[i]) {
				retl[i] = deepCopy(litlm[i])
				continue
			}
			retl[i] = patch(litlo[i], litlm[i])
		}
		return retl

	case map[string]any:
		litmm := m.(map[string]any)
		litmo, ok2 := o.(map[string]any)
		if !ok2 {
			return deepCopy(litmm)
		}

		retm := deepCopy(litmo).(map[string]any)
		for k, v := range litmm {
			vo, ok := litmo[k]
			if !ok {
				retm[k] = deepCopy(v)
				continue
			}

			if reflect.DeepEqual(vo, v) {
				retm[k] = deepCopy(v)
				continue
			}

			v := patch(vo, v)
			retm[k] = v
		}
		return retm
	default:
		// this should never happen so we should panic here but we won't
		return nil
	}
}

func deepCopy(value any) any {
	switch v := value.(type) {
	case bool, int64, float64, string:
		return v
	case []any:
		newList := make([]any, len(v))
		for i, item := range v {
			newList[i] = deepCopy(item)
		}
		return newList
	case map[string]any:
		newMap := make(map[string]any)
		for k, item := range v {
			newMap[k] = deepCopy(item)
		}
		return newMap
	default:
		return v
	}
}

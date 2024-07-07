package object

import (
	"fmt"
	"reflect"
)

func (obj *Object) Patch(m map[string]any) error {
	res, err := patch(obj.Object, m)
	if err != nil {
		return err
	}
	m, ok := res.(map[string]any)
	if !ok {
		fmt.Errorf("patch result: expected map[string]any but obtained %#v", res)
	}
	obj.SetUnstructuredContent(m)

	return nil
}

func patch(o, m any) (any, error) {
	if reflect.DeepEqual(o, m) {
		return m, nil
	}

	if o == nil {
		return m, nil
	}

	// scalars
	litb, ok := m.(bool)
	if ok {
		return litb, nil
	}

	liti, ok := m.(int64)
	if ok {
		return liti, nil
	}

	litf, ok := m.(float64)
	if ok {
		return litf, nil
	}

	lits, ok := m.(string)
	if ok {
		return lits, nil
	}

	// list
	litlm, ok := m.([]any)
	if ok {
		fmt.Printf("---------------%#v\n", litlm)
		litlo, ok2 := o.([]any)
		if !ok2 {
			return litlm, nil
		}

		ret:=make([]any, len(litlo)
	        copy(ret,litlo)

		retl := make([]any, len(litlm))
		for i := range litlm {
			if reflect.DeepEqual(litlo[i], litlm[i]) {
				retl[i] = litlm[i]
			} else {
				v, err := patch(litlo[i], litlm[i])
				if err != nil {
					return nil, err
				}
				retl[i] = v
			}
		}
		return retl, nil
	}

	litmm, ok := m.(map[string]any)
	if ok {
		litmo, ok2 := o.(map[string]any)
		if !ok2 {
			return litmm, nil
		}

		retm := map[string]any{}
		for k, v := range litmo {
			retm[k] = v
		}

		for k, v := range litmm {
			vo, ok := litmo[k]
			if !ok {
				retm[k] = v
				continue
			}

			if reflect.DeepEqual(vo, v) {
				retm[k] = v
				continue
			}

			v, err := patch(vo, v)
			if err != nil {
				return nil, err
			}
			retm[k] = v
		}
		return retm, nil
	}

	return nil, fmt.Errorf("could not patch %#v with %#v", o, m)
}

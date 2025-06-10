package expression

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"
)

func (e *Expression) UnmarshalJSON(b []byte) error {
	// cut raw content
	// try to unmarshal as a bool terminal expression
	bv := false
	if err := json.Unmarshal(b, &bv); err == nil {
		*e = Expression{Op: "@bool", Literal: bv}
		return nil
	}

	// try to unmarshal as an int terminal expression
	var iv int64 = 0
	if err := json.Unmarshal(b, &iv); err == nil {
		*e = Expression{Op: "@int", Literal: iv}
		return nil
	}

	// try to unmarshal as a float terminal expression
	fv := 0.0
	if err := json.Unmarshal(b, &fv); err == nil {
		*e = Expression{Op: "@float", Literal: fv}
		return nil
	}

	// try to unmarshal as a string terminal expression
	sv := ""
	if err := json.Unmarshal(b, &sv); err == nil && sv != "" {
		*e = Expression{Op: "@string", Literal: sv}
		if sv == "@now" {
			*e = Expression{Op: "@now"}
		} else {
			*e = Expression{Op: "@string", Literal: sv}
		}
		return nil
	}

	// try to unmarshal as a literal list expression
	mv := []Expression{}
	if err := json.Unmarshal(b, &mv); err == nil {
		*e = Expression{Op: "@list", Literal: mv}
		return nil
	}

	// try to unmarshal as a map expression
	cv := map[string]Expression{}
	if err := json.Unmarshal(b, &cv); err == nil {
		// specialcase operators: an op has a single key that starts with @
		if len(cv) == 1 {
			op := ""
			for k := range cv {
				op = k
				break
			}
			if string(op[0]) == "@" {
				exp := cv[op]
				*e = Expression{Op: op, Arg: &exp}
				return nil
			}
		}

		// literal map: store as exp with op @dict and map as Literal
		*e = Expression{Op: "@dict", Literal: cv}
		return nil
	}

	return NewUnmarshalError("expression", string(b))
}

func (e *Expression) MarshalJSON() ([]byte, error) {
	switch e.Op {
	case "@any":
		return json.Marshal(e.Literal)

	case "@now":
		v := time.Now().UTC().Format(time.RFC3339)
		return []byte(v), nil

	case "@bool":
		if e.Arg != nil {
			// keep the op for a correct round-trip and possible side-effects (conversion)
			ret := map[string]*Expression{e.Op: e.Arg}
			return json.Marshal(ret)
		}
		v, err := AsBool(e.Literal)
		if err != nil {
			return []byte(""), err
		}
		return json.Marshal(v)

	case "@int":
		if e.Arg != nil {
			// keep the op for a correct round-trip and possible side-effects (conversion)
			ret := map[string]*Expression{e.Op: e.Arg}
			return json.Marshal(ret)
		}
		v, err := AsInt(e.Literal)
		if err != nil {
			return []byte(""), err
		}
		return json.Marshal(v)

	case "@float":
		if e.Arg != nil {
			// keep the op for a correct round-trip and possible side-effects (conversion)
			ret := map[string]*Expression{e.Op: e.Arg}
			return json.Marshal(ret)
		}
		v, err := AsFloat(e.Literal)
		if err != nil {
			return []byte(""), err
		}
		return json.Marshal(v)

	case "@string":
		if e.Arg != nil {
			// keep the op for a correct round-trip and possible side-effects (conversion)
			ret := map[string]*Expression{e.Op: e.Arg}
			return json.Marshal(ret)
		}
		v, err := AsString(e.Literal)
		if err != nil {
			return []byte(""), err
		}
		return json.Marshal(v)

	case "@list":
		if e.Arg != nil {
			return json.Marshal(e.Arg)
		}
		es, ok := e.Literal.([]Expression)
		if !ok {
			return []byte(""), fmt.Errorf("invalid expression list: %#v", e)
		}
		return json.Marshal(es)

	case "@dict":
		if e.Arg != nil {
			// can this ever happen?
			return json.Marshal(e.Arg)
		}

		es, ok := e.Literal.(map[string]Expression)
		if !ok {
			return []byte(""), fmt.Errorf("invalid expression map: %#v", e)
		}
		// this is terribly stupid but here we go
		em := map[string]*Expression{}
		for k, v := range es {
			v := v
			em[k] = &v
		}
		return json.Marshal(em)

	default:
		// everything else is a valid op
		if e.Op[0] != '@' {
			return []byte(""), fmt.Errorf("expected an op starting with @, got %#v", e)
		}

		ret := map[string]*Expression{e.Op: e.Arg}
		return json.Marshal(ret)
	}
	// return []byte(""), fmt.Errorf("failed to JSON marshal expression %#v", e)
}

func (e *Expression) String() string {
	b, err := json.Marshal(e)
	if err != nil {
		return ""
	}
	return string(b)
}

func (e *Expression) DeepCopyInto(out *Expression) {
	if e == nil || out == nil {
		return
	}
	*out = *e

	j, err := json.Marshal(e)
	if err != nil {
		return
	}

	if err := json.Unmarshal(j, out); err != nil {
		return
	}
}

// unpacks the first-level list if any
func unpackList(a any) []any {
	v := reflect.ValueOf(a)

	// If it's not a slice, return nil
	if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
		return []any{a}
	}

	// If it's an empty slice, return nil
	if v.IsNil() || v.Len() == 0 {
		return []any{}
	}

	// If it's [][]any, return the first slice
	elemKind := v.Type().Elem().Kind()
	if elemKind == reflect.Slice || elemKind == reflect.Array {
		return v.Index(0).Interface().([]any)
	}

	// If it's []any{[]any, ...}, check if the first element is a slice
	first := v.Index(0)
	if !first.IsNil() {
		vs, ok := first.Interface().([]any)
		if ok {
			return vs
		}
	}

	return a.([]any)
}

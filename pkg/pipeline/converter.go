package pipeline

import (
	"errors"
	"fmt"
	"reflect"
)

func (e *Expression) isList(d any) error {
	dv := reflect.ValueOf(d)
	if dv.Kind() != reflect.Slice && dv.Kind() != reflect.Array {
		return NewExpressionError(e.Op, e.Raw,
			fmt.Errorf("argument is not a list: %#v", d))
	}
	return nil
}

func (e *Expression) asList(d any) ([]any, error) {
	if err := e.isList(d); err != nil {
		return []any{}, err
	}

	ret, ok := d.([]any)
	if !ok {
		return []any{}, NewExpressionError(e.Op, e.Raw,
			fmt.Errorf("failed to convert argument into a list: %#v", d))
	}

	return ret, nil
}

func (e *Expression) asBool(d any) (bool, error) {
	if reflect.ValueOf(d).Kind() == reflect.Bool {
		return reflect.ValueOf(d).Bool(), nil
	}
	return false, NewExpressionError("bool", e.Raw,
		fmt.Errorf("argument is not a boolean: %#v", d))
}

func (e *Expression) asBoolList(d any) ([]bool, error) {
	if err := e.isList(d); err != nil {
		return []bool{}, err
	}

	dv := reflect.ValueOf(d)
	ret := []bool{}
	for i := 0; i < dv.Len(); i++ {
		arg, err := e.asBool(dv.Index(i).Interface())
		if err != nil {
			return []bool{}, err
		}
		ret = append(ret, arg)
	}
	return ret, nil
}

func (e *Expression) asBinaryBoolList(d any) ([]bool, error) {
	vs, err := e.asBoolList(d)
	if err != nil {
		return []bool{}, err
	}

	if len(vs) != 2 {
		return []bool{}, NewExpressionError("binary bool list", e.Raw,
			fmt.Errorf("invalid number of arguments for a binary operator: %d", len(vs)))
	}

	return vs, nil
}

func (e *Expression) asString(d any) (string, error) {
	if reflect.ValueOf(d).Kind() == reflect.String {
		return reflect.ValueOf(d).String(), nil
	}
	return "", NewExpressionError("string", e.Raw,
		fmt.Errorf("argument is not a string: %#v", d))
}

func (e *Expression) asStringList(d any) ([]string, error) {
	if err := e.isList(d); err != nil {
		return []string{}, err
	}

	dv := reflect.ValueOf(d)
	if dv.Kind() != reflect.Slice && dv.Kind() != reflect.Array {
		return []string{}, NewExpressionError("string-list", e.Raw,
			fmt.Errorf("argument is not a list: %#v", d))
	}

	ret := []string{}
	for i := 0; i < dv.Len(); i++ {
		arg, err := e.asString(dv.Index(i).Interface())
		if err != nil {
			return []string{}, err
		}
		ret = append(ret, arg)
	}
	return ret, nil
}

func (e *Expression) asBinaryStringList(d any) ([]string, error) {
	vs, err := e.asStringList(d)
	if err != nil {
		return []string{}, err
	}

	if len(vs) != 2 {
		return []string{}, NewExpressionError("binary string list", e.Raw,
			fmt.Errorf("invalid number of arguments for a binary operator: %d", len(vs)))
	}

	return vs, nil
}

func (e *Expression) asInt(d any) (int64, error) {
	if reflect.ValueOf(d).Kind() == reflect.Int ||
		reflect.ValueOf(d).Kind() == reflect.Int32 ||
		reflect.ValueOf(d).Kind() == reflect.Int64 {
		return reflect.ValueOf(d).Int(), nil
	}
	return 0, NewExpressionError("int", e.Raw,
		fmt.Errorf("argument is not an int: %#v", d))
}

func (e *Expression) asIntList(d any) ([]int64, error) {
	if err := e.isList(d); err != nil {
		return []int64{}, err
	}

	dv := reflect.ValueOf(d)
	ret := []int64{}
	for i := 0; i < dv.Len(); i++ {
		arg, err := e.asInt(dv.Index(i).Interface())
		if err != nil {
			return []int64{}, err
		}
		ret = append(ret, arg)
	}
	return ret, nil
}

func (e *Expression) asBinaryIntList(d any) ([]int64, error) {
	vs, err := e.asIntList(d)
	if err != nil {
		return []int64{}, err
	}

	if len(vs) != 2 {
		return []int64{}, NewExpressionError("binary int list", e.Raw,
			fmt.Errorf("invalid number of arguments for a binary operator: %d", len(vs)))
	}

	return vs, nil
}

func (e *Expression) asFloat(d any) (float64, error) {
	if reflect.ValueOf(d).Kind() == reflect.Float32 ||
		reflect.ValueOf(d).Kind() == reflect.Float64 {
		return reflect.ValueOf(d).Float(), nil
	}
	if reflect.ValueOf(d).CanConvert(reflect.TypeOf(0.0)) {
		return reflect.ValueOf(d).Convert(reflect.TypeOf(0.0)).Float(), nil
	}

	return 0.0, NewExpressionError("float", e.Raw,
		fmt.Errorf("argument is not a float: %#v", d))
}

func (e *Expression) asFloatList(d any) ([]float64, error) {
	if err := e.isList(d); err != nil {
		return []float64{}, err
	}

	dv := reflect.ValueOf(d)
	ret := []float64{}
	for i := 0; i < dv.Len(); i++ {
		arg, err := e.asFloat(dv.Index(i).Interface())
		if err != nil {
			return []float64{}, err
		}
		ret = append(ret, arg)
	}
	return ret, nil
}

func (e *Expression) asBinaryFloatList(d any) ([]float64, error) {
	vs, err := e.asFloatList(d)
	if err != nil {
		return []float64{}, err
	}

	if len(vs) != 2 {
		return []float64{}, NewExpressionError("binary float list", e.Raw,
			fmt.Errorf("invalid number of arguments for a binary operator: %d", len(vs)))
	}

	return vs, nil
}

func (e *Expression) asIntOrFloat(d any) (int64, float64, reflect.Kind, error) {
	// try as an int
	i, err := e.asInt(d)
	if err == nil {
		return i, 0.0, reflect.Int64, nil
	}

	// convert to float
	f, err := e.asFloat(d)
	if err == nil {
		return 0, f, reflect.Float64, nil
	}

	return 0, 0.0, reflect.Invalid, NewExpressionError("int or float", e.Raw,
		fmt.Errorf("argument is not an int or float: %#v", d))
}

func (e *Expression) asIntOrFloatList(d any) ([]int64, []float64, reflect.Kind, error) {
	is, err := e.asIntList(d)
	if err == nil {
		return is, []float64{}, reflect.Int64, nil
	}

	fs, err := e.asFloatList(d)
	if err == nil {
		return []int64{}, fs, reflect.Float64, nil
	}

	return []int64{}, []float64{}, reflect.Invalid,
		NewExpressionError("numeric list", e.Raw,
			errors.New("incompatible elems in numeric list"))
}

func (e *Expression) asBinaryIntOrFloatList(d any) ([]int64, []float64, reflect.Kind, error) {
	is, fs, kind, err := e.asIntOrFloatList(d)
	if err != nil {
		return is, fs, kind, err
	}

	if kind == reflect.Int64 && len(is) != 2 {
		return is, fs, kind, NewExpressionError("binary int or float list", e.Raw,
			fmt.Errorf("invalid number of arguments in binary numeric list: %d", len(is)))
	}

	if kind == reflect.Float64 && len(fs) != 2 {
		return is, fs, kind, NewExpressionError("binary int or float list", e.Raw,
			fmt.Errorf("invalid number of arguments in binary numeric list: %d", len(is)))
	}

	return is, fs, kind, nil
}

package pipeline

import (
	"errors"
	"fmt"
	"hsnlab/dcontroller-runtime/pkg/util"
	"reflect"
	"strconv"
)

func isList(d any) bool {
	dv := reflect.ValueOf(d)
	return dv.Kind() == reflect.Slice || dv.Kind() == reflect.Array
}

func asList(d any) ([]any, error) {
	if !isList(d) {
		return nil, fmt.Errorf("argument is not a list: %#v", d)
	}

	ret, ok := d.([]any)
	if !ok {
		return nil, fmt.Errorf("failed to convert argument into a list: %#v", d)
	}

	return ret, nil
}

func asBool(d any) (bool, error) {
	if d == nil {
		return false, errors.New("argument is nil")
	}

	if reflect.ValueOf(d).Kind() == reflect.Bool {
		return reflect.ValueOf(d).Bool(), nil
	}
	return false, fmt.Errorf("argument is not a boolean: %#v", d)
}

func asBoolList(d any) ([]bool, error) {
	if !isList(d) {
		return []bool{}, fmt.Errorf("argument is not a list: %#v", d)
	}

	dv := reflect.ValueOf(d)
	ret := []bool{}
	for i := 0; i < dv.Len(); i++ {
		arg, err := asBool(dv.Index(i).Interface())
		if err != nil {
			return []bool{}, err
		}
		ret = append(ret, arg)
	}
	return ret, nil
}

func asBinaryBoolList(d any) ([]bool, error) {
	vs, err := asBoolList(d)
	if err != nil {
		return []bool{}, err
	}

	if len(vs) != 2 {
		return []bool{}, fmt.Errorf("invalid number of arguments for a binary operator: %d", len(vs))
	}

	return vs, nil
}

func asString(d any) (string, error) {
	if d == nil {
		return "", errors.New("argument is nil")
	}

	if reflect.ValueOf(d).Kind() == reflect.String {
		return reflect.ValueOf(d).String(), nil
	}
	return "", fmt.Errorf("argument is not a string: %#v", d)
}

func asStringList(d any) ([]string, error) {
	if !isList(d) {
		return []string{}, fmt.Errorf("argument is not a list: %#v", d)
	}

	dv := reflect.ValueOf(d)
	if dv.Kind() != reflect.Slice && dv.Kind() != reflect.Array {
		return []string{}, fmt.Errorf("argument is not a list: %#v", d)
	}

	ret := []string{}
	for i := 0; i < dv.Len(); i++ {
		arg, err := asString(dv.Index(i).Interface())
		if err != nil {
			return []string{}, err
		}
		ret = append(ret, arg)
	}
	return ret, nil
}

func asBinaryStringList(d any) ([]string, error) {
	vs, err := asStringList(d)
	if err != nil {
		return []string{}, err
	}

	if len(vs) != 2 {
		return []string{}, fmt.Errorf("invalid number of arguments for a binary operator: %d", len(vs))
	}

	return vs, nil
}

func asInt(d any) (int64, error) {
	if d == nil {
		return int64(0), errors.New("argument is nil")
	}

	if reflect.ValueOf(d).Kind() == reflect.Int ||
		reflect.ValueOf(d).Kind() == reflect.Int32 ||
		reflect.ValueOf(d).Kind() == reflect.Int64 {
		return reflect.ValueOf(d).Int(), nil
	}

	if reflect.ValueOf(d).Kind() == reflect.String {
		i, err := strconv.ParseInt(d.(string), 10, 64)
		if err == nil {
			return i, nil
		}
	}

	return 0, fmt.Errorf("argument is not an int: %#v", d)
}

func asIntList(d any) ([]int64, error) {
	if !isList(d) {
		return []int64{}, fmt.Errorf("argument is not a list: %#v", d)
	}

	dv := reflect.ValueOf(d)
	ret := []int64{}
	for i := 0; i < dv.Len(); i++ {
		arg, err := asInt(dv.Index(i).Interface())
		if err != nil {
			return []int64{}, err
		}
		ret = append(ret, arg)
	}
	return ret, nil
}

func asBinaryIntList(d any) ([]int64, error) {
	vs, err := asIntList(d)
	if err != nil {
		return []int64{}, err
	}

	if len(vs) != 2 {
		return []int64{}, fmt.Errorf("invalid number of arguments for a binary operator: %d", len(vs))
	}

	return vs, nil
}

func asFloat(d any) (float64, error) {
	if d == nil {
		return 0.0, errors.New("argument is nil")
	}

	if reflect.ValueOf(d).Kind() == reflect.Float32 ||
		reflect.ValueOf(d).Kind() == reflect.Float64 {
		return reflect.ValueOf(d).Float(), nil
	}
	if reflect.ValueOf(d).CanConvert(reflect.TypeOf(0.0)) {
		return reflect.ValueOf(d).Convert(reflect.TypeOf(0.0)).Float(), nil
	}

	if reflect.ValueOf(d).Kind() == reflect.String {
		f, err := strconv.ParseFloat(d.(string), 64)
		if err == nil {
			return f, nil
		}
	}

	return 0.0, fmt.Errorf("argument is not a float: %#v", d)
}

func asFloatList(d any) ([]float64, error) {
	if !isList(d) {
		return []float64{}, fmt.Errorf("argument is not a list: %#v", d)
	}

	dv := reflect.ValueOf(d)
	ret := []float64{}
	for i := 0; i < dv.Len(); i++ {
		arg, err := asFloat(dv.Index(i).Interface())
		if err != nil {
			return []float64{}, err
		}
		ret = append(ret, arg)
	}
	return ret, nil
}

func asBinaryFloatList(d any) ([]float64, error) {
	vs, err := asFloatList(d)
	if err != nil {
		return []float64{}, err
	}

	if len(vs) != 2 {
		return []float64{}, fmt.Errorf("invalid number (%d) of arguments for a binary operator: %q",
			len(vs), util.Stringify(d))
	}

	return vs, nil
}

func asIntOrFloat(d any) (int64, float64, reflect.Kind, error) {
	// try as an int
	i, err := asInt(d)
	if err == nil {
		return i, 0.0, reflect.Int64, nil
	}

	// convert to float
	f, err := asFloat(d)
	if err == nil {
		return 0, f, reflect.Float64, nil
	}

	return 0, 0.0, reflect.Invalid, fmt.Errorf("argument is not an int or float: %#v", d)
}

func asIntOrFloatList(d any) ([]int64, []float64, reflect.Kind, error) {
	is, err := asIntList(d)
	if err == nil {
		return is, []float64{}, reflect.Int64, nil
	}

	fs, err := asFloatList(d)
	if err == nil {
		return []int64{}, fs, reflect.Float64, nil
	}

	return []int64{}, []float64{}, reflect.Invalid,
		fmt.Errorf("incompatible elems in numeric list: %q", util.Stringify(d))
}

func asBinaryIntOrFloatList(d any) ([]int64, []float64, reflect.Kind, error) {
	is, fs, kind, err := asIntOrFloatList(d)
	if err != nil {
		return is, fs, kind, err
	}

	if kind == reflect.Int64 && len(is) != 2 {
		return is, fs, kind,
			fmt.Errorf("invalid number (%d) of arguments in binary numeric list: %q",
				len(is), util.Stringify(d))
	}

	if kind == reflect.Float64 && len(fs) != 2 {
		return is, fs, kind,
			fmt.Errorf("invalid number (%d) of arguments in binary numeric list: %q",
				len(is), util.Stringify(d))
	}

	return is, fs, kind, nil
}

// returns an expression or an expression list
func asExpOrList(d any) ([]Expression, error) {
	exp, ok := d.(Expression)
	if !ok {
		var expp *Expression
		expp, ok = d.(*Expression)
		if ok {
			exp = *expp
		}
	}
	if !ok {
		return nil, fmt.Errorf("argument is not an expression: %s", util.Stringify(d))
	}

	if exp.Op == "@list" {
		ret, ok := exp.Literal.([]Expression)
		if !ok {
			return nil, fmt.Errorf("internal error: list expression should contain a literal list: %s",
				exp.String())
		}
		return ret, nil
	}

	return []Expression{exp}, nil
}

func asObject(view string, d any) (ObjectContent, error) {
	val := reflect.ValueOf(d)
	if val.Kind() == reflect.Map {
		p := val.Interface()
		if u, ok := p.(ObjectContent); ok && u != nil {
			return u, nil
		}
	}
	return nil, fmt.Errorf("argument is not an object: %#v", d)
}

func asObjectList(view string, d any) ([]ObjectContent, error) {
	if !isList(d) {
		return nil, fmt.Errorf("argument is not a list: %#v", d)
	}

	dv := reflect.ValueOf(d)
	ret := []ObjectContent{}
	for i := 0; i < dv.Len(); i++ {
		arg, err := asObject(view, dv.Index(i).Interface())
		if err != nil {
			return nil, err
		}
		ret = append(ret, arg)
	}
	return ret, nil
}

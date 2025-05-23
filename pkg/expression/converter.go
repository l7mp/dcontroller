package expression

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"

	"github.com/l7mp/dcontroller/pkg/util"
)

func IsList(d any) bool {
	dv := reflect.ValueOf(d)
	return dv.Kind() == reflect.Slice || dv.Kind() == reflect.Array
}

func AsList(d any) ([]any, error) {
	if !IsList(d) {
		return nil, fmt.Errorf("argument is not a list: %s", util.Stringify(d))
	}

	ret, ok := d.([]any)
	if !ok {
		return nil, fmt.Errorf("failed to convert argument into a list: %s", util.Stringify(d))
	}

	return ret, nil
}

func AsBool(d any) (bool, error) {
	if d == nil {
		return false, errors.New("argument is nil")
	}

	if reflect.ValueOf(d).Kind() == reflect.Bool {
		return reflect.ValueOf(d).Bool(), nil
	}
	return false, fmt.Errorf("argument is not a boolean: %s", util.Stringify(d))
}

func AsBoolList(d any) ([]bool, error) {
	if !IsList(d) {
		return []bool{}, fmt.Errorf("argument is not a list: %s", util.Stringify(d))
	}

	dv := reflect.ValueOf(d)
	ret := []bool{}
	for i := 0; i < dv.Len(); i++ {
		arg, err := AsBool(dv.Index(i).Interface())
		if err != nil {
			return []bool{}, err
		}
		ret = append(ret, arg)
	}
	return ret, nil
}

func AsBinaryBoolList(d any) ([]bool, error) {
	vs, err := AsBoolList(d)
	if err != nil {
		return []bool{}, err
	}

	if len(vs) != 2 {
		return []bool{}, fmt.Errorf("invalid number of arguments for a binary operator: %d", len(vs))
	}

	return vs, nil
}

func AsString(d any) (string, error) {
	if d == nil {
		return "", errors.New("argument is nil")
	}

	if reflect.ValueOf(d).Kind() == reflect.String {
		return reflect.ValueOf(d).String(), nil
	}

	// convert numeric
	switch reflect.ValueOf(d).Kind() { //nolint:exhaustive
	case reflect.String:
		return reflect.ValueOf(d).String(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr, reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128:
		return fmt.Sprintf("%d", d), nil
	}

	return "", fmt.Errorf("argument is not a string: %s", util.Stringify(d))
}

func AsStringList(d any) ([]string, error) {
	if !IsList(d) {
		return []string{}, fmt.Errorf("argument is not a list: %s", util.Stringify(d))
	}

	dv := reflect.ValueOf(d)
	if dv.Kind() != reflect.Slice && dv.Kind() != reflect.Array {
		return []string{}, fmt.Errorf("argument is not a list: %s", util.Stringify(d))
	}

	ret := []string{}
	for i := 0; i < dv.Len(); i++ {
		arg, err := AsString(dv.Index(i).Interface())
		if err != nil {
			return []string{}, err
		}
		ret = append(ret, arg)
	}
	return ret, nil
}

func AsBinaryStringList(d any) ([]string, error) {
	vs, err := AsStringList(d)
	if err != nil {
		return []string{}, err
	}

	if len(vs) != 2 {
		return []string{}, fmt.Errorf("invalid number of arguments for a binary operator: %d", len(vs))
	}

	return vs, nil
}

func AsInt(d any) (int64, error) {
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

	return 0, fmt.Errorf("argument is not an int: %s", util.Stringify(d))
}

func AsIntList(d any) ([]int64, error) {
	if !IsList(d) {
		return []int64{}, fmt.Errorf("argument is not a list: %s", util.Stringify(d))
	}

	dv := reflect.ValueOf(d)
	ret := []int64{}
	for i := 0; i < dv.Len(); i++ {
		arg, err := AsInt(dv.Index(i).Interface())
		if err != nil {
			return []int64{}, err
		}
		ret = append(ret, arg)
	}
	return ret, nil
}

func AsBinaryIntList(d any) ([]int64, error) {
	vs, err := AsIntList(d)
	if err != nil {
		return []int64{}, err
	}

	if len(vs) != 2 {
		return []int64{}, fmt.Errorf("invalid number of arguments for a binary operator: %d", len(vs))
	}

	return vs, nil
}

func AsFloat(d any) (float64, error) {
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

	return 0.0, fmt.Errorf("argument is not a float: %s", util.Stringify(d))
}

func AsFloatList(d any) ([]float64, error) {
	if !IsList(d) {
		return []float64{}, fmt.Errorf("argument is not a list: %s", util.Stringify(d))
	}

	dv := reflect.ValueOf(d)
	ret := []float64{}
	for i := 0; i < dv.Len(); i++ {
		arg, err := AsFloat(dv.Index(i).Interface())
		if err != nil {
			return []float64{}, err
		}
		ret = append(ret, arg)
	}
	return ret, nil
}

func AsBinaryFloatList(d any) ([]float64, error) {
	vs, err := AsFloatList(d)
	if err != nil {
		return []float64{}, err
	}

	if len(vs) != 2 {
		return []float64{}, fmt.Errorf("invalid number (%d) of arguments for a binary operator: %s",
			len(vs), util.Stringify(d))
	}

	return vs, nil
}

func AsIntOrFloat(d any) (int64, float64, reflect.Kind, error) {
	// try as an int
	i, err := AsInt(d)
	if err == nil {
		return i, 0.0, reflect.Int64, nil
	}

	// convert to float
	f, err := AsFloat(d)
	if err == nil {
		return 0, f, reflect.Float64, nil
	}

	return 0, 0.0, reflect.Invalid, fmt.Errorf("argument is not an int or float: %s", util.Stringify(d))
}

func AsIntOrFloatList(d any) ([]int64, []float64, reflect.Kind, error) {
	is, err := AsIntList(d)
	if err == nil {
		return is, []float64{}, reflect.Int64, nil
	}

	fs, err := AsFloatList(d)
	if err == nil {
		return []int64{}, fs, reflect.Float64, nil
	}

	return []int64{}, []float64{}, reflect.Invalid,
		fmt.Errorf("incompatible elems in numeric list: %q", util.Stringify(d))
}

func AsBinaryIntOrFloatList(d any) ([]int64, []float64, reflect.Kind, error) {
	is, fs, kind, err := AsIntOrFloatList(d)
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

func AsMap(d any) (map[string]any, error) {
	ret, ok := d.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("failed to convert argument into a map: %s", util.Stringify(d))
	}

	return ret, nil
}

// AsExpOrExpList returns an expression or an expression list.
func AsExpOrExpList(d any) ([]Expression, error) {
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

	if exp.Op == "@list" { //nolint:goconst
		ret, ok := exp.Literal.([]Expression)
		if !ok {
			return nil, fmt.Errorf("internal error: list expression should contain a literal list: %s",
				exp.String())
		}
		return ret, nil
	}

	return []Expression{exp}, nil
}

func AsObject(d any) (unstruct, error) {
	val := reflect.ValueOf(d)
	if val.Kind() == reflect.Map {
		p := val.Interface()
		if u, ok := p.(unstruct); ok && u != nil {
			return u, nil
		}
	}
	return nil, fmt.Errorf("argument is not an object: %s", util.Stringify(d))
}

func AsObjectList(d any) ([]unstruct, error) {
	if !IsList(d) {
		return nil, fmt.Errorf("argument is not a list: %s", util.Stringify(d))
	}

	dv := reflect.ValueOf(d)
	ret := []unstruct{}
	for i := 0; i < dv.Len(); i++ {
		arg, err := AsObject(dv.Index(i).Interface())
		if err != nil {
			return nil, err
		}
		ret = append(ret, arg)
	}
	return ret, nil
}

// AsObjectOrObjectList returns an object or an expression list.
func AsObjectOrObjectList(d any) ([]unstruct, error) {
	if vs, err := AsObject(d); err == nil {
		return []unstruct{vs}, nil
	}

	if vs, err := AsObjectList(d); err == nil {
		return vs, nil
	}

	return nil, fmt.Errorf("argument is not an object or an object list: %s", util.Stringify(d))
}

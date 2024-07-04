package pipeline

import (
	"reflect"
)

func (e *Expression) asBool(d any) (bool, error) {
	if reflect.ValueOf(d).Kind() == reflect.Bool {
		return reflect.ValueOf(d).Bool(), nil
	}
	return false, NewExpressionError("bool", e.Raw)
}

func (e *Expression) asBoolList(ds []any) ([]bool, error) {
	ret := []bool{}
	for _, d := range ds {
		arg, err := e.asBool(d)
		if err != nil {
			return []bool{}, err
		}
		ret = append(ret, arg)
	}
	return ret, nil
}

func (e *Expression) asBinaryBoolList(ds []any) ([]bool, error) {
	vs, err := e.asBoolList(ds)
	if err != nil {
		return []bool{}, err
	}

	if len(vs) != 2 {
		return []bool{}, NewExpressionError("binary bool list", e.Raw)
	}

	return vs, nil
}

func (e *Expression) asString(d any) (string, error) {
	if reflect.ValueOf(d).Kind() == reflect.String {
		return reflect.ValueOf(d).String(), nil
	}
	return "", NewExpressionError("string", e.Raw)
}

func (e *Expression) asStringList(ds []any) ([]string, error) {
	ret := []string{}
	for _, d := range ds {
		arg, err := e.asString(d)
		if err != nil {
			return []string{}, err
		}
		ret = append(ret, arg)
	}
	return ret, nil
}

func (e *Expression) asBinaryStringList(ds []any) ([]string, error) {
	vs, err := e.asStringList(ds)
	if err != nil {
		return []string{}, err
	}

	if len(vs) != 2 {
		return []string{}, NewExpressionError("binary string list", e.Raw)
	}

	return vs, nil
}

func (e *Expression) asInt(d any) (int64, error) {
	if reflect.ValueOf(d).Kind() == reflect.Int ||
		reflect.ValueOf(d).Kind() == reflect.Int32 ||
		reflect.ValueOf(d).Kind() == reflect.Int64 {
		return reflect.ValueOf(d).Int(), nil
	}
	return 0, NewExpressionError("int", e.Raw)
}

func (e *Expression) asIntList(ds []any) ([]int64, error) {
	ret := []int64{}
	for _, d := range ds {
		arg, err := e.asInt(d)
		if err != nil {
			return []int64{}, err
		}
		ret = append(ret, arg)
	}
	return ret, nil
}

func (e *Expression) asBinaryIntList(ds []any) ([]int64, error) {
	vs, err := e.asIntList(ds)
	if err != nil {
		return []int64{}, err
	}

	if len(vs) != 2 {
		return []int64{}, NewExpressionError("binary int list", e.Raw)
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

	return 0.0, NewExpressionError("float", e.Raw)
}

func (e *Expression) asFloatList(ds []any) ([]float64, error) {
	ret := []float64{}
	for _, d := range ds {
		arg, err := e.asFloat(d)
		if err != nil {
			return []float64{}, err
		}
		ret = append(ret, arg)
	}
	return ret, nil
}

func (e *Expression) asBinaryFloatList(ds []any) ([]float64, error) {
	vs, err := e.asFloatList(ds)
	if err != nil {
		return []float64{}, err
	}

	if len(vs) != 2 {
		return []float64{}, NewExpressionError("binary float list", e.Raw)
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

	return 0, 0.0, reflect.Invalid, NewExpressionError("int or float", e.Raw)
}

func (e *Expression) asIntOrFloatList(ds []any) ([]int64, []float64, reflect.Kind, error) {
	is, err := e.asIntList(ds)
	if err == nil {
		return is, []float64{}, reflect.Int64, nil
	}

	fs, err := e.asFloatList(ds)
	if err == nil {
		return []int64{}, fs, reflect.Float64, nil
	}

	return []int64{}, []float64{}, reflect.Invalid, NewExpressionError("numeric list", e.Raw)
}

func (e *Expression) asBinaryIntOrFloatList(ds []any) ([]int64, []float64, reflect.Kind, error) {
	is, fs, kind, err := e.asIntOrFloatList(ds)
	if err != nil {
		return is, fs, kind, err
	}

	if kind == reflect.Int64 && len(is) != 2 {
		return is, fs, kind, NewExpressionError("binary int or float list", e.Raw)
	}

	if kind == reflect.Float64 && len(fs) != 2 {
		return is, fs, kind, NewExpressionError("binary int or float list", e.Raw)
	}

	return is, fs, kind, nil
}

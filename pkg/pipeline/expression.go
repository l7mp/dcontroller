package pipeline

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strings"

	"k8s.io/client-go/util/jsonpath"
)

type Expression struct {
	Op      string
	Args    []Expression
	Raw     string
	Literal any
}

func (e *Expression) Evaluate(state State) (any, error) {

	// evaluate subexpressions
	args := []any{}
	for _, sub := range e.Args {
		res, err := sub.Evaluate(state)
		if err != nil {
			return nil, err
		}
		args = append(args, res)
	}

	switch e.Op {
	// terminals
	case "@bool":
		lit := e.Literal
		if len(args) != 0 {
			lit = args[0]
		}

		// for safety
		v, err := e.asBool(lit)

		if err != nil {
			return nil, err
		}

		state.Log.V(4).Info("eval ready", "expression", e.String(), "result", v)

		return v, nil

	case "@int":
		lit := e.Literal
		if len(args) != 0 {
			lit = args[0]
		}

		// for safety
		v, err := e.asInt(lit)
		if err != nil {
			return nil, err
		}

		state.Log.V(4).Info("eval ready", "expression", e.String(), "result", v)

		return v, nil

	case "@float":
		lit := e.Literal
		if len(args) != 0 {
			lit = args[0]
		}

		// for safety
		v, err := e.asFloat(lit)
		if err != nil {
			return nil, err
		}

		state.Log.V(4).Info("eval ready", "expression", e.String(), "result", v)

		return v, nil

	case "@string":
		lit := e.Literal
		if len(args) != 0 {
			lit = args[0]
		}

		// for safety
		v, err := e.asString(lit)
		if err != nil {
			return nil, err
		}

		state.Log.V(4).Info("eval ready", "expression", e.String(), "result", v)

		return e.EvalStringExp(state, v)

		// binary bool
	case "@eq":
		// try args as int-or-float
		is, fs, kind, err := e.asBinaryIntOrFloatList(args)
		if err == nil {
			if kind == reflect.Int64 {
				v := is[0] == is[1]
				state.Log.V(4).Info("int-eq eval ready", "expression", e.String(), "result", v)
				return v, nil
			}
			if kind == reflect.Float64 {
				v := fs[0] == fs[1]
				state.Log.V(4).Info("float-eq eval ready", "expression", e.String(), "result", v)
				return v, nil
			}
		}

		// try args as string
		vs, err := e.asBinaryStringList(args)
		if err == nil {
			v := vs[0] == vs[1]
			state.Log.V(4).Info("string-eq eval ready", "expression", e.String(), "result", v)
			return v, nil
		}

		// give up
		return nil, NewExpressionError("@eq", e.Raw)

	case "@lt":
		is, fs, kind, err := e.asBinaryIntOrFloatList(args)
		if err != nil {
			return nil, err
		}

		if kind == reflect.Int64 {
			v := is[0] < is[1]
			state.Log.V(4).Info("eval ready", "expression", e.String(), "result", v)
			return v, nil
		}

		v := fs[0] < fs[1]
		state.Log.V(4).Info("eval ready", "expression", e.String(), "result", v)
		return v, nil

	case "@lte":
		is, fs, kind, err := e.asBinaryIntOrFloatList(args)
		if err != nil {
			return nil, err
		}

		if kind == reflect.Int64 {
			v := is[0] <= is[1]
			state.Log.V(4).Info("eval ready", "expression", e.String(), "result", v)
			return v, nil
		}

		v := fs[0] <= fs[1]
		state.Log.V(4).Info("eval ready", "expression", e.String(), "result", v)
		return v, nil

	case "@gt":
		is, fs, kind, err := e.asBinaryIntOrFloatList(args)
		if err != nil {
			return nil, err
		}

		if kind == reflect.Int64 {
			v := is[0] > is[1]
			state.Log.V(4).Info("eval ready", "expression", e.String(), "result", v)
			return v, nil
		}

		v := fs[0] > fs[1]
		state.Log.V(4).Info("eval ready", "expression", e.String(), "result", v)
		return v, nil

	case "@gte":
		is, fs, kind, err := e.asBinaryIntOrFloatList(args)
		if err != nil {
			return nil, err
		}

		if kind == reflect.Int64 {
			v := is[0] >= is[1]
			state.Log.V(4).Info("eval ready", "expression", e.String(), "result", v)
			return v, nil
		}

		v := fs[0] >= fs[1]
		state.Log.V(4).Info("eval ready", "expression", e.String(), "result", v)
		return v, nil

	case "@and":
		args, err := e.asBinaryBoolList(args)
		if err != nil {
			return nil, err
		}

		v := args[0] && args[1]
		state.Log.V(4).Info("eval ready", "expression", e.String(), "result", v)
		return v, nil

	case "@or":
		args, err := e.asBinaryBoolList(args)
		if err != nil {
			return nil, err
		}

		v := args[0] || args[1]
		state.Log.V(4).Info("eval ready", "expression", e.String(), "result", v)
		return v, nil

	case "@not":
		arg, err := e.asBool(args[0])
		if err != nil {
			return nil, err
		}

		v := !arg
		state.Log.V(4).Info("eval ready", "expression", e.String(), "result", v)
		return v, nil

		// arithmetic
	case "@abs":
		f, err := e.asFloat(args[0])
		if err != nil {
			return nil, err
		}

		v := math.Abs(f)
		state.Log.V(4).Info("eval ready", "expression", e.String(), "result", v)
		return v, nil

	case "@ceil":
		f, err := e.asFloat(args[0])
		if err != nil {
			return nil, err
		}

		v := math.Ceil(f)
		state.Log.V(4).Info("eval ready", "expression", e.String(), "result", v)
		return v, nil

	case "@floor":
		f, err := e.asFloat(args[0])
		if err != nil {
			return nil, err
		}

		v := math.Floor(f)
		state.Log.V(4).Info("eval ready", "expression", e.String(), "result", v)
		return v, nil

		// binary arithmetic

	case "@sum":
		is, fs, kind, err := e.asBinaryIntOrFloatList(args)
		if err != nil {
			return nil, err
		}

		if kind == reflect.Int64 {
			v := is[0] + is[1]
			state.Log.V(4).Info("eval ready", "expression", e.String(), "result", v)
			return v, nil
		}

		v := fs[0] + fs[1]
		state.Log.V(4).Info("eval ready", "expression", e.String(), "result", v)
		return v, nil

	case "@sub":
		is, fs, kind, err := e.asBinaryIntOrFloatList(args)
		if err != nil {
			return nil, err
		}

		if kind == reflect.Int64 {
			v := is[0] - is[1]
			state.Log.V(4).Info("eval ready", "expression", e.String(), "result", v)
			return v, nil
		}

		v := fs[0] - fs[1]
		state.Log.V(4).Info("eval ready", "expression", e.String(), "result", v)
		return v, nil

	case "@mul":
		is, fs, kind, err := e.asBinaryIntOrFloatList(args)
		if err != nil {
			return nil, err
		}

		if kind == reflect.Int64 {
			v := is[0] * is[1]
			state.Log.V(4).Info("eval ready", "expression", e.String(), "result", v)
			return v, nil
		}

		v := fs[0] * fs[1]
		state.Log.V(4).Info("eval ready", "expression", e.String(), "result", v)
		return v, nil

	case "@div":
		is, fs, kind, err := e.asBinaryIntOrFloatList(args)
		if err != nil {
			return nil, err
		}

		if kind == reflect.Int64 {
			v := is[0] / is[1]
			state.Log.V(4).Info("eval ready", "expression", e.String(), "result", v)
			return v, nil
		}

		v := fs[0] / fs[1]
		state.Log.V(4).Info("eval ready", "expression", e.String(), "result", v)
		return v, nil

		// string
	case "@concat":
		args, err := e.asStringList(args)
		if err != nil {
			return nil, err
		}

		v := ""
		for i := range args {
			v += args[i]
		}

		state.Log.V(4).Info("eval ready", "expression", e.String(), "result", v)

		return v, nil

	default:
		return nil, NewExpressionError("", e.Raw)
	}
}

func (e *Expression) UnmarshalJSON(b []byte) error {
	// try to unmarshal as a bool terminal expression
	bv := false
	if err := json.Unmarshal(b, &bv); err == nil {
		*e = Expression{Op: "@bool", Literal: bv, Raw: string(b)}
		return nil
	}

	// try to unmarshal as an int terminal expression
	iv := 0
	if err := json.Unmarshal(b, &iv); err == nil {
		*e = Expression{Op: "@int", Literal: iv, Raw: string(b)}
		return nil
	}

	// try to unmarshal as a float terminal expression
	fv := 0.0
	if err := json.Unmarshal(b, &fv); err == nil {
		*e = Expression{Op: "@float", Literal: fv, Raw: string(b)}
		return nil
	}

	// try to unmarshal as a string terminal expression
	sv := ""
	if err := json.Unmarshal(b, &sv); err == nil && sv != "" {
		*e = Expression{Op: "@string", Literal: sv, Raw: string(b)}
		return nil
	}

	// try to unmarshal as a compound expression
	cv := map[string]([]Expression){}
	if err := json.Unmarshal(b, &cv); err == nil && len(cv) == 1 {
		for k, v := range cv {
			*e = Expression{Op: k, Args: v, Raw: string(b)}
			return nil
		}
	}

	return NewUnmarshalError("expression", string(b))
}

func (e *Expression) String() string {
	if len(e.Args) == 1 {
		switch e.Op {
		// terminals
		case "@bool":
			// for safety
			v, err := e.asBool(e.Args[0].Literal)
			if err != nil {
				return "<invalid>"
			}
			return fmt.Sprintf("<bool>%t", v)
		case "@int":
			// for safety
			v, err := e.asInt(e.Args[0].Literal)
			if err != nil {
				return "<invalid>"
			}
			return fmt.Sprintf("<int>%d", v)
		case "@float":
			// for safety
			v, err := e.asFloat(e.Args[0].Literal)
			if err != nil {
				return "<invalid>"
			}
			return fmt.Sprintf("<float>%f", v)
		case "@string":
			// for safety
			v, err := e.asString(e.Args[0].Literal)
			if err != nil {
				return "<invalid>"
			}
			return fmt.Sprintf("<string>%q", v)
		}
	}

	args := make([]string, len(e.Args))
	for i, arg := range e.Args {
		args[i] = arg.String()
	}

	return fmt.Sprintf("%s: [%s]", e.Op, strings.Join(args, ","))
}

// evaluate a terminal expression (json expression or literal)
func (e *Expression) EvalStringExp(state State, str string) (any, error) {
	if len(str) > 0 && str[0] == '$' {
		jsonExp := str[1:]
		switch jsonExp {
		case "":
			jsonExp = "{}"
		case ".":
			jsonExp = "{}"
		case "{.}":
			jsonExp = "{}"
		default:
			jxp, err := RelaxedJSONPathExpression(jsonExp)
			if err != nil {
				return nil, err
			}
			jsonExp = jxp
		}

		j := jsonpath.New("JSONpathParser")
		if err := j.Parse(jsonExp); err != nil {
			return nil, err
		}
		j.AllowMissingKeys(true)

		input := state.Object.UnstructuredContent()
		values, err := j.FindResults(input)
		if err != nil {
			return nil, err
		}

		state.Log.V(4).Info("JSONpath expression eval ready", "expression", e.String(),
			"result", values)

		if len(values) == 0 || len(values[0]) == 0 {
			return nil, NewExpressionError("JSONpath", fmt.Sprintf("%s on %#v", jsonExp, input))
		}

		if values[0][0].IsNil() {
			return "", nil
		}

		return values[0][0].Interface(), nil
	}

	return str, nil
}

// from https://github.com/kubernetes/kubectl/blob/master/pkg/cmd/get/customcolumn.go
var jsonRegexp = regexp.MustCompile(`^\{\.?([^{}]+)\}$|^\.?([^{}]+)$`)

// RelaxedJSONPathExpression attempts to be flexible with JSONPath expressions, it accepts:
//   - metadata.name (no leading '.' or curly braces '{...}'
//   - {metadata.name} (no leading '.')
//   - .metadata.name (no curly braces '{...}')
//   - {.metadata.name} (complete expression)
//
// And transforms them all into a valid jsonpath expression: {.metadata.name}
func RelaxedJSONPathExpression(pathExpression string) (string, error) {
	if len(pathExpression) == 0 {
		return pathExpression, nil
	}
	submatches := jsonRegexp.FindStringSubmatch(pathExpression)
	if submatches == nil {
		return "", fmt.Errorf("unexpected path string, expected a 'name1.name2' " +
			"or '.name1.name2' or '{name1.name2}' or '{.name1.name2}'")
	}
	if len(submatches) != 3 {
		return "", fmt.Errorf("unexpected submatch list: %v", submatches)
	}
	var fieldSpec string
	if len(submatches[1]) != 0 {
		fieldSpec = submatches[1]
	} else {
		fieldSpec = submatches[2]
	}
	return fmt.Sprintf("{.%s}", fieldSpec), nil
}

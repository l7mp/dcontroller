package pipeline

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strings"

	"k8s.io/client-go/util/jsonpath"

	"hsnlab/dcontroller-runtime/pkg/util"
)

// Expression is an operator, an argument, and potentially a literal value.
type Expression struct {
	Op      string
	Arg     *Expression
	Literal any
	Raw     string
}

func (e *Expression) Evaluate(state *State) (any, error) {
	if len(e.Op) == 0 {
		return nil, NewInvalidArgumentsError(fmt.Sprintf("empty operator in expession %q", e.Raw))
	}

	switch e.Op {
	case "@bool":
		lit := e.Literal
		if e.Arg != nil {
			// eval stacked expressions stored in e.Arg
			v, err := e.Arg.Evaluate(state)
			if err != nil {
				return nil, err
			}
			lit = v
		}

		v, err := e.asBool(lit)
		if err != nil {
			return nil, err
		}

		state.Log.V(4).Info("eval ready", "expression", e.String(), "result", v)

		return v, nil

	case "@int":
		lit := e.Literal
		if e.Arg != nil {
			// eval stacked expressions stored in e.Arg
			v, err := e.Arg.Evaluate(state)
			if err != nil {
				return nil, err
			}
			lit = v
		}

		v, err := e.asInt(lit)
		if err != nil {
			return nil, err
		}

		state.Log.V(4).Info("eval ready", "expression", e.String(), "result", v)

		return v, nil

	case "@float":
		lit := e.Literal
		if e.Arg != nil {
			// eval stacked expressions stored in e.Arg
			v, err := e.Arg.Evaluate(state)
			if err != nil {
				return nil, err
			}
			lit = v
		}

		v, err := e.asFloat(lit)
		if err != nil {
			return nil, err
		}

		state.Log.V(4).Info("eval ready", "expression", e.String(), "result", v)

		return v, nil

	case "@string":
		lit := e.Literal
		if e.Arg != nil {
			// eval stacked expressions stored in e.Arg
			v, err := e.Arg.Evaluate(state)
			if err != nil {
				return nil, err
			}
			lit = v
		}

		v, err := e.asString(lit)
		if err != nil {
			return nil, err
		}

		state.Log.V(4).Info("eval ready", "expression", e.String(), "result", v)

		return e.EvalStringExp(state, v)

	case "@list":
		ret := []any{}
		if e.Arg != nil {
			// eval stacked expressions stored in e.Arg
			v, err := e.Arg.Evaluate(state)
			if err != nil {
				return nil, err
			}

			// must be []any
			vs, ok := v.([]any)
			if !ok {
				return nil, NewExpressionError("@list", e.Raw,
					errors.New("argument must be a list"))
			}
			ret = vs
		} else {
			// literal lists stored in Literal
			vs, ok := e.Literal.([]Expression)
			if !ok {
				return nil, NewExpressionError("@list", e.Raw,
					errors.New("argument must be an expression list"))
			}

			for _, exp := range vs {
				res, err := exp.Evaluate(state)
				if err != nil {
					return nil, err
				}
				ret = append(ret, res)
			}
		}

		state.Log.V(4).Info("eval ready", "expression", e.String(), "result", ret)

		return ret, nil

	case "@dict":
		ret := map[string]any{}
		if e.Arg != nil {
			// eval stacked expressions stored in e.Arg
			v, err := e.Arg.Evaluate(state)
			if err != nil {
				return nil, err
			}

			// must be map[string]any
			vs, ok := v.(map[string]any)
			if !ok {
				return nil, NewExpressionError("@dict", e.Raw,
					errors.New("argument must be a map"))
			}
			ret = vs

		} else {
			// map stored as a Literal
			if reflect.ValueOf(e.Literal).Kind() != reflect.Map {
				return nil, NewExpressionError("@dict", e.Raw,
					errors.New("argument must be a map literal"))
			}

			vm, ok := e.Literal.(map[string]Expression)
			if !ok {
				return nil, NewExpressionError("@dict", e.Raw,
					errors.New("argument must be a string->expression map: %q"))
			}

			for k, exp := range vm {
				// evaluate arguments
				res, err := exp.Evaluate(state)
				if err != nil {
					return nil, err
				}
				ret[k] = res
			}
		}

		state.Log.V(4).Info("eval ready", "expression", e.String(), "result", ret)

		return ret, nil
	}

	// operators
	// evaluate subexpression
	if e.Arg == nil {
		return nil, NewExpressionError(e.Op, e.Raw, errors.New("empty argument list"))
	}

	arg, err := e.Arg.Evaluate(state)
	if err != nil {
		return nil, err
	}

	if arg == nil {
		return nil, NewExpressionError(e.Op, e.Raw,
			errors.New("argument evaluates to undefined value"))
	}

	if string(e.Op[0]) == "@" {
		switch e.Op {
		// binary bool
		case "@eq":
			is, fs, kind, err := e.asBinaryIntOrFloatList(arg)
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

			vs, err := e.asBinaryStringList(arg)
			if err == nil {
				v := vs[0] == vs[1]
				state.Log.V(4).Info("string-eq eval ready", "expression", e.String(), "result", v)
				return v, nil
			}

			// give up
			return nil, NewExpressionError("@eq", e.Raw,
				fmt.Errorf("incompatible arguments: %#v", arg))

		case "@lt":
			is, fs, kind, err := e.asBinaryIntOrFloatList(arg)
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
			is, fs, kind, err := e.asBinaryIntOrFloatList(arg)
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
			is, fs, kind, err := e.asBinaryIntOrFloatList(arg)
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
			is, fs, kind, err := e.asBinaryIntOrFloatList(arg)
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

		case "@not":
			arg, err := e.asBool(arg)
			if err != nil {
				return nil, err
			}

			v := !arg
			state.Log.V(4).Info("eval ready", "expression", e.String(), "result", v)
			return v, nil

			// arithmetic
		case "@abs":
			f, err := e.asFloat(arg)
			if err != nil {
				return nil, err
			}

			v := math.Abs(f)
			state.Log.V(4).Info("eval ready", "expression", e.String(), "result", v)
			return v, nil

		case "@ceil":
			f, err := e.asFloat(arg)
			if err != nil {
				return nil, err
			}

			v := math.Ceil(f)
			state.Log.V(4).Info("eval ready", "expression", e.String(), "result", v)
			return v, nil

		case "@floor":
			f, err := e.asFloat(arg)
			if err != nil {
				return nil, err
			}

			v := math.Floor(f)
			state.Log.V(4).Info("eval ready", "expression", e.String(), "result", v)
			return v, nil

			// list
		case "@sum":
			is, fs, kind, err := e.asIntOrFloatList(arg)
			if err != nil {
				return nil, err
			}

			if kind == reflect.Int64 {
				v := int64(0)
				for i := range is {
					v += is[i]
				}
				state.Log.V(4).Info("eval ready", "expression", e.String(), "result", v)
				return v, nil
			}

			v := 0.0
			for i := range fs {
				v += fs[i]
			}
			state.Log.V(4).Info("eval ready", "expression", e.String(), "result", v)
			return v, nil

		case "@first":
			args, err := e.asList(arg)
			if err != nil {
				return nil, err
			}

			if len(args) == 0 {
				return nil, NewExpressionError("@first", e.Raw, errors.New("empty list"))
			}
			v := args[0]
			state.Log.V(4).Info("eval ready", "expression", e.String(), "result", v)
			return v, nil

		case "@last":
			args, err := e.asList(arg)
			if err != nil {
				return nil, err
			}

			if len(args) == 0 {
				return nil, NewExpressionError("@last", e.Raw, errors.New("empty list"))
			}
			v := args[len(args)-1]
			state.Log.V(4).Info("eval ready", "expression", e.String(), "result", v)
			return v, nil

		case "@len":
			args, err := e.asList(arg)
			if err != nil {
				return nil, err
			}

			v := int64(len(args))
			state.Log.V(4).Info("eval ready", "expression", e.String(), "result", v)
			return v, nil

			// binary arithmetic
		case "@sub":
			is, fs, kind, err := e.asBinaryIntOrFloatList(arg)
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
			is, fs, kind, err := e.asBinaryIntOrFloatList(arg)
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
			is, fs, kind, err := e.asBinaryIntOrFloatList(arg)
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
			args, err := e.asStringList(arg)
			if err != nil {
				return nil, err
			}

			v := ""
			for i := range args {
				v += args[i]
			}

			state.Log.V(4).Info("eval ready", "expression", e.String(), "result", v)

			return v, nil

		case "@and":
			args, err := e.asBoolList(arg)
			if err != nil {
				return nil, err
			}

			v := true
			for i := range args {
				v = v && args[i]
			}

			state.Log.V(4).Info("eval ready", "expression", e.String(), "result", v)

			return v, nil

		case "@or":
			args, err := e.asBoolList(arg)
			if err != nil {
				return nil, err
			}

			v := false
			for i := range args {
				v = v || args[i]
			}

			state.Log.V(4).Info("eval ready", "expression", e.String(), "result", v)

			return v, nil

		default:
			return nil, NewExpressionError("command reference (@*)", e.Raw,
				fmt.Errorf("unknown operator: %q", e.Op))
		}
	}

	// literal map
	return map[string]any{e.Op: arg}, nil
}

func (e *Expression) UnmarshalJSON(b []byte) error {
	// try to unmarshal as a bool terminal expression
	bv := false
	if err := json.Unmarshal(b, &bv); err == nil {
		*e = Expression{Op: "@bool", Literal: bv, Raw: string(b)}
		return nil
	}

	// try to unmarshal as an int terminal expression
	var iv int64 = 0
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

	// try to unmarshal as a literal list expression
	mv := []Expression{}
	if err := json.Unmarshal(b, &mv); err == nil {
		*e = Expression{Op: "@list", Literal: mv, Raw: string(b)}
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
				*e = Expression{Op: op, Arg: &exp, Raw: string(b)}
				return nil
			}
		}

		// literal map: store as exp with op @dict and map as Literal
		*e = Expression{Op: "@dict", Literal: cv, Raw: string(b)}
		return nil
	}

	return NewUnmarshalError("expression", string(b))
}

func (e *Expression) String() string {
	// literals
	switch e.Op {
	case "@bool":
		if e.Arg != nil {
			return fmt.Sprintf("<bool>{%s}", e.Arg.String())
		}
		v, err := e.asBool(e.Literal)
		if err != nil {
			return "<invalid>"
		}
		return fmt.Sprintf("<bool>%t", v)
	case "@int":
		if e.Arg != nil {
			return fmt.Sprintf("<int>{%s}", e.Arg.String())
		}
		v, err := e.asInt(e.Literal)
		if err != nil {
			return "<invalid>"
		}
		return fmt.Sprintf("<int>%d", v)
	case "@float":
		if e.Arg != nil {
			return fmt.Sprintf("<float>{%s}", e.Arg.String())
		}
		v, err := e.asFloat(e.Literal)
		if err != nil {
			return "<invalid>"
		}
		return fmt.Sprintf("<float>%f", v)
	case "@string":
		if e.Arg != nil {
			return fmt.Sprintf("<string>{%s}", e.Arg.String())
		}
		v, err := e.asString(e.Literal)
		if err != nil {
			return "<invalid>"
		}
		return fmt.Sprintf("<string>%q", v)
	case "@list":
		if e.Arg != nil {
			return fmt.Sprintf("<list>{%s}", e.Arg.String())
		}
		es, ok := e.Literal.([]Expression)
		if !ok {
			return "<invalid>"
		}
		return fmt.Sprintf("<list>[%s]", strings.Join(util.Map(
			func(exp Expression) string { return exp.String() }, es,
		), ","))
	case "@dict":
		if e.Arg != nil {
			return fmt.Sprintf("<map>{%s}", e.Arg.String())
		}
		return fmt.Sprintf("<map>%s", util.Stringify(e.Literal))
	}

	return fmt.Sprintf("%s:[%s]", e.Op, e.Arg.String())
}

// evaluate a terminal expression (json expression or literal)
func (e *Expression) EvalStringExp(state *State, str string) (any, error) {
	if len(str) > 0 && str[0] == '$' {
		return EvalJSONpathExp(state, str[1:], e.String())
	}

	return str, nil
}

func EvalJSONpathExp(state *State, jsonExp, raw string) (any, error) {
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

	if len(values) == 0 || len(values[0]) == 0 {
		return nil, NewExpressionError("JSONpath", jsonExp,
			fmt.Errorf("failed to apply JSONPath expression on %#v", input))
	}

	if values[0][0].IsNil() {
		state.Log.V(4).Info("JSONpath expression eval ready", "expression", raw,
			"result", "")
		return "", nil
	}

	state.Log.V(4).Info("JSONpath expression eval ready", "expression", raw,
		"result", values[0][0].Interface())

	return values[0][0].Interface(), nil
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

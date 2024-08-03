package pipeline

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"

	"hsnlab/dcontroller-runtime/pkg/util"
)

// Expression is an operator, an argument, and potentially a literal value.
type Expression struct {
	Op      string
	Arg     *Expression
	Literal any
	Raw     string
}

func (e *Expression) Evaluate(eng *Engine) (any, error) {
	if len(e.Op) == 0 {
		return nil, NewInvalidArgumentsError(fmt.Sprintf("empty operator in expession %q", e.Raw))
	}

	switch e.Op {
	case "@any":
		lit := e.Literal
		if e.Arg != nil {
			// eval stacked expressions stored in e.Arg
			v, err := e.Arg.Evaluate(eng)
			if err != nil {
				return nil, err
			}
			lit = v
		}

		eng.log.V(4).Info("eval ready", "expression", e.String(), "result", lit)

		return lit, nil

	case "@bool":
		lit := e.Literal
		if e.Arg != nil {
			// eval stacked expressions stored in e.Arg
			v, err := e.Arg.Evaluate(eng)
			if err != nil {
				return nil, err
			}
			lit = v
		}

		v, err := asBool(lit)
		if err != nil {
			return nil, NewExpressionError(e.Op, e.Raw, err)
		}

		eng.log.V(4).Info("eval ready", "expression", e.String(), "result", v)

		return v, nil

	case "@int":
		lit := e.Literal
		if e.Arg != nil {
			// eval stacked expressions stored in e.Arg
			v, err := e.Arg.Evaluate(eng)
			if err != nil {
				return nil, err
			}
			lit = v
		}

		v, err := asInt(lit)
		if err != nil {
			return nil, NewExpressionError(e.Op, e.Raw, err)
		}

		eng.log.V(4).Info("eval ready", "expression", e.String(), "result", v)

		return v, nil

	case "@float":
		lit := e.Literal
		if e.Arg != nil {
			// eval stacked expressions stored in e.Arg
			v, err := e.Arg.Evaluate(eng)
			if err != nil {
				return nil, err
			}
			lit = v
		}

		v, err := asFloat(lit)
		if err != nil {
			return nil, NewExpressionError(e.Op, e.Raw, err)
		}

		eng.log.V(4).Info("eval ready", "expression", e.String(), "result", v)

		return v, nil

	case "@string":
		lit := e.Literal
		if e.Arg != nil {
			// eval stacked expressions stored in e.Arg
			v, err := e.Arg.Evaluate(eng)
			if err != nil {
				return nil, err
			}
			lit = v
		}

		str, err := asString(lit)
		if err != nil {
			return nil, NewExpressionError(e.Op, e.Raw, err)
		}

		ret, err := e.GetJSONPath(eng, str)
		if err != nil {
			return nil, err
		}

		eng.log.V(4).Info("eval ready", "expression", e.String(), "result", ret)

		return ret, nil

	case "@list":
		ret := []any{}
		if e.Arg != nil {
			// eval stacked expressions stored in e.Arg
			v, err := e.Arg.Evaluate(eng)
			if err != nil {
				return nil, err
			}

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
				res, err := exp.Evaluate(eng)
				if err != nil {
					return nil, err
				}
				ret = append(ret, res)
			}
		}

		// WARNING: this will destroy multi-dimensional arrays
		ret = unpackList(ret)

		eng.log.V(4).Info("eval ready", "expression", e.String(), "result", ret)

		return ret, nil

	case "@dict":
		ret := map[string]any{}
		if e.Arg != nil {
			// eval stacked expressions stored in e.Arg
			v, err := e.Arg.Evaluate(eng)
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
					errors.New("argument must be a string->expression map"))
			}

			for k, exp := range vm {
				// evaluate arguments
				res, err := exp.Evaluate(eng)
				if err != nil {
					return nil, err
				}
				err = exp.SetJSONPath(eng, k, res, ret)
				if err != nil {
					return nil, NewExpressionError("@dict", e.Raw, err)
				}
			}
		}

		eng.log.V(4).Info("eval ready", "expression", e.String(), "result", ret)

		return ret, nil
	}

	// commands: must eval the arg themselves
	if string(e.Op[0]) == "@" {
		switch e.Op {
		case "@filter":
			args, err := asExpOrList(e.Arg)
			if err != nil {
				return nil, NewExpressionError(e.Op, e.Raw, err)

			}

			if len(args) == 0 {
				return nil, NewExpressionError(e.Op, e.Raw, errors.New("not enough arguments"))
			}

			// conditional
			cond := args[0]
			args = args[1:]

			// arguments
			inputs, err := prepareListCommandArgs(eng, args)
			if err != nil {
				return nil, NewExpressionError(e.Op, e.Raw, err)
			}

			vs := []any{}
			for _, input := range inputs {
				eng.pushStack(input)
				res, err := cond.Evaluate(eng)
				eng.popStack()

				if err != nil {
					return nil, err
				}

				b, err := asBool(res)
				if err != nil {
					return nil, NewExpressionError(e.Op, e.Raw,
						fmt.Errorf("expected conditional expression to "+
							"evaluate to boolean: %w", err))
				}

				if b {
					vs = append(vs, input)
				}

			}

			eng.log.V(4).Info("eval ready", "expression", e.String(), "result", vs)

			return vs, nil

		case "@map":
			args, err := asExpOrList(e.Arg)
			if err != nil {
				return nil, NewExpressionError(e.Op, e.Raw, err)
			}

			if len(args) == 0 {
				return nil, NewExpressionError(e.Op, e.Raw, errors.New("not enough arguments"))
			}

			// pop conditional
			fun := args[0]
			args = args[1:]

			inputs, err := prepareListCommandArgs(eng, args)
			if err != nil {
				return nil, NewExpressionError(e.Op, e.Raw, err)
			}

			vs := []any{}
			for _, input := range inputs {
				eng.pushStack(input)
				res, err := fun.Evaluate(eng)
				eng.popStack()

				if err != nil {
					return nil, err
				}

				vs = append(vs, res)
			}

			eng.log.V(4).Info("eval ready", "expression", e.String(), "result", vs)

			return vs, nil

		case "@aggregate":
			args, err := asExpOrList(e.Arg)
			if err != nil {
				return nil, NewExpressionError(e.Op, e.Raw, err)
			}

			if len(args) == 0 {
				return nil, NewExpressionError(e.Op, e.Raw, errors.New("not enough arguments"))
			}

			var outputs []map[string]any
			for _, exp := range args {
				res, err := exp.Evaluate(eng)
				if err != nil {
					return nil, NewExpressionError(e.Op, e.Raw,
						fmt.Errorf("should evaluate to an object list: %s", err))
				}

				outputs, err = asObjectList(eng.view, res)
				if err != nil {
					return nil, err
				}

				eng.inputs = outputs
			}

			eng.log.V(4).Info("eval ready", "expression", e.String(), "result", outputs)

			return outputs, nil
		}
	}

	// operators
	// evaluate subexpression
	if e.Arg == nil {
		return nil, NewExpressionError(e.Op, e.Raw, errors.New("empty argument list"))
	}

	arg, err := e.Arg.Evaluate(eng)
	if err != nil {
		return nil, err
	}

	if string(e.Op[0]) == "@" {
		switch e.Op {
		// unary bool
		case "@isnil":
			v := arg == nil
			eng.log.V(4).Info("eval ready", "expression", e.String(), "result", v)
			return v, nil

		case "@not":
			arg, err := asBool(arg)
			if err != nil {
				return nil, NewExpressionError(e.Op, e.Raw, err)
			}

			v := !arg
			eng.log.V(4).Info("eval ready", "expression", e.String(), "result", v)
			return v, nil

		// binary bool
		case "@eq":
			is, fs, kind, err := asBinaryIntOrFloatList(arg)
			if err == nil {
				if kind == reflect.Int64 {
					v := is[0] == is[1]
					eng.log.V(4).Info("int-eq eval ready", "expression", e.String(), "result", v)
					return v, nil
				}
				if kind == reflect.Float64 {
					v := fs[0] == fs[1]
					eng.log.V(4).Info("float-eq eval ready", "expression", e.String(), "result", v)
					return v, nil
				}
			}

			vs, err := asBinaryStringList(arg)
			if err == nil {
				v := vs[0] == vs[1]
				eng.log.V(4).Info("string-eq eval ready", "expression", e.String(), "result", v)
				return v, nil
			}

			// give up
			return nil, NewExpressionError("@eq", e.Raw,
				fmt.Errorf("incompatible arguments: %#v", arg))

			// list bool
		case "@and":
			args, err := asBoolList(arg)
			if err != nil {
				return nil, NewExpressionError(e.Op, e.Raw, err)
			}

			v := true
			for i := range args {
				v = v && args[i]
			}

			eng.log.V(4).Info("eval ready", "expression", e.String(), "result", v)

			return v, nil

		case "@or":
			args, err := asBoolList(arg)
			if err != nil {
				return nil, NewExpressionError(e.Op, e.Raw, err)
			}

			v := false
			for i := range args {
				v = v || args[i]
			}

			eng.log.V(4).Info("eval ready", "expression", e.String(), "result", v)

			return v, nil

			// unary arithmetic
		case "@abs":
			f, err := asFloat(arg)
			if err != nil {
				return nil, NewExpressionError(e.Op, e.Raw, err)
			}

			v := math.Abs(f)
			eng.log.V(4).Info("eval ready", "expression", e.String(), "result", v)
			return v, nil

		case "@ceil":
			f, err := asFloat(arg)
			if err != nil {
				return nil, NewExpressionError(e.Op, e.Raw, err)
			}

			v := math.Ceil(f)
			eng.log.V(4).Info("eval ready", "expression", e.String(), "result", v)
			return v, nil

		case "@floor":
			f, err := asFloat(arg)
			if err != nil {
				return nil, NewExpressionError(e.Op, e.Raw, err)
			}

			v := math.Floor(f)
			eng.log.V(4).Info("eval ready", "expression", e.String(), "result", v)
			return v, nil

			// binary arithmetic
		case "@lt":
			is, fs, kind, err := asBinaryIntOrFloatList(arg)
			if err != nil {
				return nil, NewExpressionError(e.Op, e.Raw, err)
			}

			if kind == reflect.Int64 {
				v := is[0] < is[1]
				eng.log.V(4).Info("eval ready", "expression", e.String(), "result", v)
				return v, nil
			}

			v := fs[0] < fs[1]
			eng.log.V(4).Info("eval ready", "expression", e.String(), "result", v)
			return v, nil

		case "@lte":
			is, fs, kind, err := asBinaryIntOrFloatList(arg)
			if err != nil {
				return nil, NewExpressionError(e.Op, e.Raw, err)
			}

			if kind == reflect.Int64 {
				v := is[0] <= is[1]
				eng.log.V(4).Info("eval ready", "expression", e.String(), "result", v)
				return v, nil
			}

			v := fs[0] <= fs[1]
			eng.log.V(4).Info("eval ready", "expression", e.String(), "result", v)
			return v, nil

		case "@gt":
			is, fs, kind, err := asBinaryIntOrFloatList(arg)
			if err != nil {
				return nil, NewExpressionError(e.Op, e.Raw, err)
			}

			if kind == reflect.Int64 {
				v := is[0] > is[1]
				eng.log.V(4).Info("eval ready", "expression", e.String(), "result", v)
				return v, nil
			}

			v := fs[0] > fs[1]
			eng.log.V(4).Info("eval ready", "expression", e.String(), "result", v)
			return v, nil

		case "@gte":
			is, fs, kind, err := asBinaryIntOrFloatList(arg)
			if err != nil {
				return nil, NewExpressionError(e.Op, e.Raw, err)
			}

			if kind == reflect.Int64 {
				v := is[0] >= is[1]
				eng.log.V(4).Info("eval ready", "expression", e.String(), "result", v)
				return v, nil
			}

			v := fs[0] >= fs[1]
			eng.log.V(4).Info("eval ready", "expression", e.String(), "result", v)
			return v, nil

			// list arithemtic
		case "@sum":
			is, fs, kind, err := asIntOrFloatList(arg)
			if err != nil {
				return nil, NewExpressionError(e.Op, e.Raw, err)
			}

			if kind == reflect.Int64 {
				v := int64(0)
				for i := range is {
					v += is[i]
				}
				eng.log.V(4).Info("eval ready", "expression", e.String(), "result", v)
				return v, nil
			}

			v := 0.0
			for i := range fs {
				v += fs[i]
			}
			eng.log.V(4).Info("eval ready", "expression", e.String(), "result", v)
			return v, nil

			// list
		case "@first":
			args, err := asList(arg)
			if err != nil {
				return []any{}, NewExpressionError(e.Op, e.Raw, err)
			}

			if len(args) == 0 {
				return nil, NewExpressionError("@first", e.Raw, errors.New("empty list"))
			}
			v := args[0]
			eng.log.V(4).Info("eval ready", "expression", e.String(), "result", v)
			return v, nil

		case "@last":
			args, err := asList(arg)
			if err != nil {
				return []any{}, NewExpressionError(e.Op, e.Raw, err)
			}

			if len(args) == 0 {
				return nil, NewExpressionError("@last", e.Raw, errors.New("empty list"))
			}
			v := args[len(args)-1]
			eng.log.V(4).Info("eval ready", "expression", e.String(), "result", v)
			return v, nil

		case "@len":
			args, err := asList(arg)
			if err != nil {
				return nil, NewExpressionError(e.Op, e.Raw, err)

			}

			args = unpackList(args)

			v := int64(len(args))
			eng.log.V(4).Info("eval ready", "expression", e.String(), "result", v)
			return v, nil

			// binary arithmetic
		case "@sub":
			is, fs, kind, err := asBinaryIntOrFloatList(arg)
			if err != nil {
				return nil, NewExpressionError(e.Op, e.Raw, err)
			}

			if kind == reflect.Int64 {
				v := is[0] - is[1]
				eng.log.V(4).Info("eval ready", "expression", e.String(), "result", v)
				return v, nil
			}

			v := fs[0] - fs[1]
			eng.log.V(4).Info("eval ready", "expression", e.String(), "result", v)
			return v, nil

		case "@mul":
			is, fs, kind, err := asBinaryIntOrFloatList(arg)
			if err != nil {
				return nil, NewExpressionError(e.Op, e.Raw, err)
			}

			if kind == reflect.Int64 {
				v := is[0] * is[1]
				eng.log.V(4).Info("eval ready", "expression", e.String(), "result", v)
				return v, nil
			}

			v := fs[0] * fs[1]
			eng.log.V(4).Info("eval ready", "expression", e.String(), "result", v)
			return v, nil

		case "@div":
			is, fs, kind, err := asBinaryIntOrFloatList(arg)
			if err != nil {
				return nil, NewExpressionError(e.Op, e.Raw, err)
			}

			if kind == reflect.Int64 {
				v := is[0] / is[1]
				eng.log.V(4).Info("eval ready", "expression", e.String(), "result", v)
				return v, nil
			}

			v := fs[0] / fs[1]
			eng.log.V(4).Info("eval ready", "expression", e.String(), "result", v)
			return v, nil

			// string
		case "@concat":
			args, err := asStringList(arg)
			if err != nil {
				return nil, NewExpressionError(e.Op, e.Raw, err)
			}

			v := ""
			for i := range args {
				v += args[i]
			}

			eng.log.V(4).Info("eval ready", "expression", e.String(), "result", v)

			return v, nil

		default:
			return nil, NewExpressionError("command reference (@*)", e.Raw,
				fmt.Errorf("unknown operator: %q", e.Op))
		}
	}

	// literal map
	return map[string]any{e.Op: arg}, nil
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
	case "@any":
		return fmt.Sprintf("<any>{%#v}", util.Stringify(e.Literal))
	case "@bool":
		if e.Arg != nil {
			return fmt.Sprintf("<bool>{%s}", e.Arg.String())
		}
		v, err := asBool(e.Literal)
		if err != nil {
			return "<invalid>"
		}
		return fmt.Sprintf("<bool>{%t}", v)
	case "@int":
		if e.Arg != nil {
			return fmt.Sprintf("<int>{%s}", e.Arg.String())
		}
		v, err := asInt(e.Literal)
		if err != nil {
			return "<invalid>"
		}
		return fmt.Sprintf("<int>{%d}", v)
	case "@float":
		if e.Arg != nil {
			return fmt.Sprintf("<float>{%s}", e.Arg.String())
		}
		v, err := asFloat(e.Literal)
		if err != nil {
			return "<invalid>"
		}
		return fmt.Sprintf("<float>{%f}", v)
	case "@string":
		if e.Arg != nil {
			return fmt.Sprintf("<string>{%s}", e.Arg.String())
		}
		v, err := asString(e.Literal)
		if err != nil {
			return "<invalid>"
		}
		return fmt.Sprintf("<string>{%q}", v)
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
			return fmt.Sprintf("<dict>{%s}", e.Arg.String())
		}
		return fmt.Sprintf("<dict>{%s}", util.Stringify(e.Literal))
	}

	return fmt.Sprintf("%s:[%s]", e.Op, e.Arg.String())
}

// returns a list of arguments to for a list command (@map, @filter, etc) to iterate on
//   - if there is explicit argument list, use that
//   - if there is no argument list, use the provided objects (useful for the top-level aggregation
//     command)
func prepareListCommandArgs(eng *Engine, args []Expression) ([]any, error) {
	ret := []any{}

	// if no args, use the object context
	if len(args) == 0 {
		for i := range eng.inputs {
			ret = append(ret, eng.inputs[i])
		}
		return ret, nil
	}

	// use the explicit args provided by the expression
	rawArg, err := args[0].Evaluate(eng)
	if err != nil {
		return nil, errors.New("failed to evaluate arguments")
	}

	ret, err = asList(rawArg)
	if err != nil {
		return nil, errors.New("invlaid arguments: expecting a list")
	}

	return ret, nil
}

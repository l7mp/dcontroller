package expression

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"

	"github.com/go-logr/logr"
	"github.com/grokify/mogo/encoding/base36"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/json"
)

const ExpressionDumpMaxLevel = 10

type Unstructured = map[string]any
type GVK = schema.GroupVersionKind

type EvalCtx struct {
	Object, Subject any
	Log             logr.Logger
}

type Expression struct {
	Op      string
	Arg     *Expression
	Literal any
}

func (e *Expression) Evaluate(ctx EvalCtx) (any, error) {
	if len(e.Op) == 0 {
		return nil, NewInvalidArgumentsError(fmt.Sprintf("empty operator in expession %q", e.String()))
	}

	switch e.Op {
	case "@bool":
		lit := e.Literal
		if e.Arg != nil {
			// eval stacked expressions stored in e.Arg
			v, err := e.Arg.Evaluate(ctx)
			if err != nil {
				return nil, err
			}
			lit = v
		}

		v, err := AsBool(lit)
		if err != nil {
			return nil, NewExpressionError(e, err)
		}

		ctx.Log.V(8).Info("eval ready", "expression", e.String(), "result", v)

		return v, nil

	case "@int":
		lit := e.Literal
		if e.Arg != nil {
			// eval stacked expressions stored in e.Arg
			v, err := e.Arg.Evaluate(ctx)
			if err != nil {
				return nil, err
			}
			lit = v
		}

		v, err := AsInt(lit)
		if err != nil {
			return nil, NewExpressionError(e, err)
		}

		ctx.Log.V(8).Info("eval ready", "expression", e.String(), "result", v)

		return v, nil

	case "@float":
		lit := e.Literal
		if e.Arg != nil {
			// eval stacked expressions stored in e.Arg
			v, err := e.Arg.Evaluate(ctx)
			if err != nil {
				return nil, err
			}
			lit = v
		}

		v, err := AsFloat(lit)
		if err != nil {
			return nil, NewExpressionError(e, err)
		}

		ctx.Log.V(8).Info("eval ready", "expression", e.String(), "result", v)

		return v, nil

	case "@string":
		lit := e.Literal
		if e.Arg != nil {
			// eval stacked expressions stored in e.Arg
			v, err := e.Arg.Evaluate(ctx)
			if err != nil {
				return nil, err
			}
			lit = v
		}

		str, err := AsString(lit)
		if err != nil {
			return nil, NewExpressionError(e, err)
		}

		ret, err := GetJSONPath(ctx, str)
		if err != nil {
			return nil, err
		}

		ctx.Log.V(8).Info("eval ready", "expression", e.String(), "result", ret)

		return ret, nil

	case "@list":
		ret := []any{}
		if e.Arg != nil {
			// eval stacked expressions stored in e.Arg
			v, err := e.Arg.Evaluate(ctx)
			if err != nil {
				return nil, err
			}

			vs, ok := v.([]any)
			if !ok {
				return nil, NewExpressionError(e, errors.New("argument must be a list"))
			}

			ret = vs
		} else {
			// literal lists stored in Literal
			vs, ok := e.Literal.([]Expression)
			if !ok {
				return nil, NewExpressionError(e,
					errors.New("argument must be an expression list"))
			}

			for _, exp := range vs {
				res, err := exp.Evaluate(ctx)
				if err != nil {
					return nil, err
				}
				ret = append(ret, res)
			}
		}

		// WARNING: this will destroy multi-dimensional arrays
		ret = unpackList(ret)

		ctx.Log.V(8).Info("eval ready", "expression", e.String(), "result", ret)

		return ret, nil

	case "@dict":
		ret := Unstructured{}
		if e.Arg != nil {
			// eval stacked expressions stored in e.Arg
			v, err := e.Arg.Evaluate(ctx)
			if err != nil {
				return nil, err
			}

			// must be Unstructured
			vs, ok := v.(Unstructured)
			if !ok {
				return nil, NewExpressionError(e, errors.New("argument must be a map"))
			}
			ret = vs
		} else {
			// map stored as a Literal
			if reflect.ValueOf(e.Literal).Kind() != reflect.Map {
				return nil, NewExpressionError(e, errors.New("argument must be a map literal"))
			}

			vm, ok := e.Literal.(map[string]Expression)
			if !ok {
				return nil, NewExpressionError(e,
					errors.New("argument must be a string->expression map"))
			}

			for k, exp := range vm {
				// evaluate arguments
				res, err := exp.Evaluate(ctx)
				if err != nil {
					return nil, err
				}
				err = SetJSONPath(ctx, k, res, ret)
				if err != nil {
					return nil, NewExpressionError(e,
						fmt.Errorf("could not deference JSON \"set\" expression: %w", err))
				}
			}
		}

		ctx.Log.V(8).Info("eval ready", "expression", e.String(), "result", ret)

		return ret, nil
	}

	// @cond: conditional must eval the arg
	if string(e.Op[0]) == "@" {
		switch e.Op {
		case "@cond":
			args, err := AsExpOrExpList(e.Arg)
			if err != nil {
				return nil, NewExpressionError(e, err)
			}

			if len(args) != 2 && len(args) != 3 {
				return nil, NewExpressionError(e,
					errors.New("invalid arguments: expected 2 (if/then) or 3 (if/then/else) arguments"))
			}

			// conditional
			ce, err := args[0].Evaluate(ctx)
			if err != nil {
				return nil, fmt.Errorf("failed to evaluate conditional: %w", err)
			}

			c, err := AsBool(ce)
			if err != nil {
				return nil, NewExpressionError(e, fmt.Errorf("expected conditional to evaluate "+
					"to boolean: %w", err))
			}

			var v any
			if c {
				// then branch
				arg, err := args[1].Evaluate(ctx)
				if err != nil {
					return nil, fmt.Errorf("failed to evaluate \"true\" branch: %w", err)
				}
				v = arg

			} else if len(args) == 3 {
				// else branch
				arg, err := args[2].Evaluate(ctx)
				if err != nil {
					return nil, fmt.Errorf("failed to evaluate \"false\" branch: %w", err)
				}
				v = arg
			}

			ctx.Log.V(8).Info("eval ready", "expression", e.String(), "arg", args, "result", v)
			return v, nil

		case "@definedOr": // useful for setting defaults
			args, err := AsExpOrExpList(e.Arg)
			if err != nil {
				return nil, NewExpressionError(e, err)
			}

			if len(args) != 2 {
				return nil, NewExpressionError(e,
					errors.New("invalid arguments: expected 2 arguments"))
			}

			// conditional
			v, err := args[0].Evaluate(ctx)
			if err != nil {
				return nil, err
			}
			fmt.Printf("--------- \"%#v\"\n", v)

			if v == nil {
				v, err = args[1].Evaluate(ctx)
				if err != nil {
					return nil, err
				}
			}

			ctx.Log.V(8).Info("eval ready", "expression", e.String(), "arg", args, "result", v)
			return v, nil
		}
	}

	// list commands: must eval the arg themselves
	if string(e.Op[0]) == "@" {
		switch e.Op {
		// list bool
		case "@and":
			args, err := AsExpOrExpList(e.Arg)
			if err != nil {
				return nil, NewExpressionError(e, err)
			}

			v := true
			for _, arg := range args {
				r, err := arg.Evaluate(ctx)
				if err != nil {
					return nil, fmt.Errorf("failed to evaluate argument: %w", err)
				}
				c, err := AsBool(r)
				if err != nil {
					return nil, NewExpressionError(e, err)
				}
				if !c {
					v = false
					break
				}
			}

			ctx.Log.V(8).Info("eval ready", "expression", e.String(), "args", args, "result", v)

			return v, nil

		case "@or":
			args, err := AsExpOrExpList(e.Arg)
			if err != nil {
				return nil, NewExpressionError(e, err)
			}

			v := false
			for _, arg := range args {
				r, err := arg.Evaluate(ctx)
				if err != nil {
					return nil, fmt.Errorf("failed to evaluate argument: %w", err)
				}
				c, err := AsBool(r)
				if err != nil {
					return nil, NewExpressionError(e, err)
				}
				if c {
					v = true
					break
				}
			}

			ctx.Log.V(8).Info("eval ready", "expression", e.String(), "args", args, "result", v)

			return v, nil

		case "@filter":
			args, err := AsExpOrExpList(e.Arg)
			if err != nil {
				return nil, NewExpressionError(e, err)
			}

			if len(args) != 2 {
				return nil, NewExpressionError(e,
					errors.New("invalid arguments: expected 2 arguments"))
			}

			// conditional
			cond := args[0]

			// arguments
			rawArg, err := args[1].Evaluate(ctx)
			if err != nil {
				return nil, errors.New("failed to evaluate arguments")
			}

			list, err := AsList(rawArg)
			if err != nil {
				return nil, errors.New("invalid arguments: expected a list")
			}

			vs := []any{}
			for _, input := range list {
				res, err := cond.Evaluate(EvalCtx{Object: ctx.Object, Subject: input, Log: ctx.Log})
				if err != nil {
					return nil, err
				}

				b, err := AsBool(res)
				if err != nil {
					return nil, NewExpressionError(e,
						fmt.Errorf("expected conditional expression to "+
							"evaluate to boolean: %w", err))
				}

				if b {
					vs = append(vs, input)
				}
			}

			ctx.Log.V(8).Info("eval ready", "expression", e.String(), "result", vs)

			return vs, nil

			// @in: [exp, list]
		case "@any": //nolint:dupl
			args, err := AsExpOrExpList(e.Arg)
			if err != nil {
				return nil, NewExpressionError(e, err)
			}

			if len(args) != 2 {
				return nil, NewExpressionError(e,
					errors.New("invalid arguments: expected 2 arguments"))
			}

			// conditional
			exp := args[0]

			// arguments
			rawArg, err := args[1].Evaluate(ctx)
			if err != nil {
				return nil, errors.New("failed to evaluate arguments")
			}

			list, err := AsList(rawArg)
			if err != nil {
				return nil, errors.New("invalid arguments: expected a list")
			}

			v := false
			for _, input := range list {
				res, err := exp.Evaluate(EvalCtx{Object: ctx.Object, Subject: input, Log: ctx.Log})
				if err != nil {
					return nil, err
				}

				b, err := AsBool(res)
				if err != nil {
					return nil, NewExpressionError(e,
						fmt.Errorf("expected conditional expression to "+
							"evaluate to boolean: %w", err))
				}

				if b {
					v = true
					break
				}
			}

			ctx.Log.V(8).Info("eval ready", "expression", e.String(), "arg", args, "result", v)
			return v, nil

			// @in: [exp, list]
		case "@none": //nolint:dupl
			args, err := AsExpOrExpList(e.Arg)
			if err != nil {
				return nil, NewExpressionError(e, err)
			}

			if len(args) != 2 {
				return nil, NewExpressionError(e,
					errors.New("invalid arguments: expected 2 arguments"))
			}

			// conditional
			exp := args[0]

			// arguments
			rawArg, err := args[1].Evaluate(ctx)
			if err != nil {
				return nil, errors.New("failed to evaluate arguments")
			}

			list, err := AsList(rawArg)
			if err != nil {
				return nil, errors.New("invalid arguments: expected a list")
			}

			v := false
			for _, input := range list {
				res, err := exp.Evaluate(EvalCtx{Object: ctx.Object, Subject: input, Log: ctx.Log})
				if err != nil {
					return nil, err
				}

				b, err := AsBool(res)
				if err != nil {
					return nil, NewExpressionError(e,
						fmt.Errorf("expected conditional expression to "+
							"evaluate to boolean: %w", err))
				}

				if b {
					v = false
					break
				}
			}

			ctx.Log.V(8).Info("eval ready", "expression", e.String(), "arg", args, "result", v)
			return v, nil

		case "@all": // @in: [exp, list]
			args, err := AsExpOrExpList(e.Arg)
			if err != nil {
				return nil, NewExpressionError(e, err)
			}

			if len(args) != 2 {
				return nil, NewExpressionError(e,
					errors.New("invalid arguments: expected 2 arguments"))
			}

			// conditional
			exp := args[0]

			// arguments
			rawArg, err := args[1].Evaluate(ctx)
			if err != nil {
				return nil, errors.New("failed to evaluate arguments")
			}

			list, err := AsList(rawArg)
			if err != nil {
				return nil, errors.New("invalid arguments: expected a list")
			}

			v := true
			for _, input := range list {
				res, err := exp.Evaluate(EvalCtx{Object: ctx.Object, Subject: input, Log: ctx.Log})
				if err != nil {
					return nil, err
				}

				b, err := AsBool(res)
				if err != nil {
					return nil, NewExpressionError(e,
						fmt.Errorf("expected conditional expression to "+
							"evaluate to boolean: %w", err))
				}

				if !b {
					v = false
					break
				}
			}

			ctx.Log.V(8).Info("eval ready", "expression", e.String(), "arg", args, "result", v)
			return v, nil

		case "@map":
			args, err := AsExpOrExpList(e.Arg)
			if err != nil {
				return nil, NewExpressionError(e, err)
			}

			// function
			exp := args[0]

			// arguments
			rawArg, err := args[1].Evaluate(ctx)
			if err != nil {
				return nil, errors.New("failed to evaluate arguments")
			}

			list, err := AsList(rawArg)
			if err != nil {
				return nil, errors.New("invalid arguments: expected a list")
			}

			vs := []any{}
			for _, input := range list {
				res, err := exp.Evaluate(EvalCtx{Object: ctx.Object, Subject: input, Log: ctx.Log})
				if err != nil {
					return nil, err
				}

				vs = append(vs, res)
			}

			ctx.Log.V(8).Info("eval ready", "expression", e.String(), "result", vs)

			return vs, nil

			// case "@fold":
			// 	args, err := asExpOrList(e.Arg)
			// 	if err != nil {
			// 		return nil, NewExpressionError(e.Op, e.Raw, err)
			// 	}

			// 	if len(args) == 2 {
			// 		return nil, NewExpressionError(e.Op, e.Raw, errors.New("not enough arguments"))
			// 	}

			// 	var outputs []map[string]any
			// 	for _, exp := range args {
			// 		res, err := exp.Evaluate(eng)
			// 		if err != nil {
			// 			return nil, NewExpressionError(e.Op, e.Raw,
			// 				fmt.Errorf("should evaluate to an object list: %s", err))
			// 		}

			// 		outputs, err = asObjectList(eng.view, res)
			// 		if err != nil {
			// 			return nil, err
			// 		}

			// 		eng.inputs = outputs
			// 	}

			// 	eng.log.V(4).Info("eval ready", "expression", e.String(), "result", outputs)

			// 	return outputs, nil
		}
	}

	// operators
	// evaluate subexpression
	if e.Arg == nil {
		return nil, NewExpressionError(e, errors.New("empty argument list"))
	}

	arg, err := e.Arg.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	if string(e.Op[0]) == "@" {
		switch e.Op {
		// unary bool
		case "@isnil":
			v := arg == nil
			ctx.Log.V(8).Info("eval ready", "expression", e.String(), "args", arg, "result", v)
			return v, nil

		case "@exists":
			v := arg != nil
			ctx.Log.V(8).Info("eval ready", "expression", e.String(), "args", arg, "result", v)
			return v, nil

		case "@not":
			arg, err := AsBool(arg)
			if err != nil {
				return nil, NewExpressionError(e, err)
			}

			v := !arg
			ctx.Log.V(8).Info("eval ready", "expression", e.String(), "args", arg, "result", v)
			return v, nil

		// binary bool
		case "@eq":
			args, err := AsList(arg)
			if err != nil {
				return nil, NewExpressionError(e, err)
			}

			if len(args) != 2 {
				return nil, NewExpressionError(e, errors.New("expected 2 arguments"))
			}

			v := reflect.DeepEqual(args[0], args[1])
			ctx.Log.V(8).Info("eval ready", "expression", e.String(), "args", args, "result", v)
			return v, nil

			// binary
		case "@lt":
			is, fs, kind, err := AsBinaryIntOrFloatList(arg)
			if err != nil {
				return nil, NewExpressionError(e, err)
			}

			if kind == reflect.Int64 {
				v := is[0] < is[1]
				ctx.Log.V(8).Info("eval ready", "expression", e.String(), "args", is, "result", v)
				return v, nil
			}

			v := fs[0] < fs[1]
			ctx.Log.V(8).Info("eval ready", "expression", e.String(), "args", fs, "result", v)
			return v, nil

		case "@lte":
			is, fs, kind, err := AsBinaryIntOrFloatList(arg)
			if err != nil {
				return nil, NewExpressionError(e, err)
			}

			if kind == reflect.Int64 {
				v := is[0] <= is[1]
				ctx.Log.V(8).Info("eval ready", "expression", e.String(), "args", is, "result", v)
				return v, nil
			}

			v := fs[0] <= fs[1]
			ctx.Log.V(8).Info("eval ready", "expression", e.String(), "args", fs, "result", v)
			return v, nil

		case "@gt":
			is, fs, kind, err := AsBinaryIntOrFloatList(arg)
			if err != nil {
				return nil, NewExpressionError(e, err)
			}

			if kind == reflect.Int64 {
				v := is[0] > is[1]
				ctx.Log.V(8).Info("eval ready", "expression", e.String(), "args", is, "result", v)
				return v, nil
			}

			v := fs[0] > fs[1]
			ctx.Log.V(8).Info("eval ready", "expression", e.String(), "args", fs, "result", v)
			return v, nil

		case "@gte":
			is, fs, kind, err := AsBinaryIntOrFloatList(arg)
			if err != nil {
				return nil, NewExpressionError(e, err)
			}

			if kind == reflect.Int64 {
				v := is[0] >= is[1]
				ctx.Log.V(8).Info("eval ready", "expression", e.String(), "args", is, "result", v)
				return v, nil
			}

			v := fs[0] >= fs[1]
			ctx.Log.V(8).Info("eval ready", "expression", e.String(), "args", fs, "result", v)
			return v, nil

		case "@selector": // [selector, labels]
			args, err := AsList(arg)
			if err != nil {
				return nil, NewExpressionError(e, err)
			}

			if len(args) != 2 {
				return nil, NewExpressionError(e,
					errors.New("invalid arguments: expected 2 arguments"))
			}

			if args[0] == nil || args[1] == nil {
				return false, nil
			}

			selector, err := AsObject(args[0])
			if err != nil {
				return nil, NewExpressionError(e, fmt.Errorf("invalid label selector: %w", err))
			}

			labels, err := AsObject(args[1])
			if err != nil {
				return nil, NewExpressionError(e, fmt.Errorf("invalid label set: %w", err))
			}

			// arguments
			res, err := MatchLabels(labels, selector)
			if err != nil {
				return nil, fmt.Errorf("failed to evaluate label selector: %w", err)
			}

			v, err := AsBool(res)
			if err != nil {
				return nil, NewExpressionError(e, fmt.Errorf("expected label selector expression to "+
					"evaluate to boolean: %w", err))
			}

			ctx.Log.V(8).Info("eval ready", "expression", e.String(), "arg", args, "result", v)
			return v, nil

			// unary arithmetic
		case "@abs":
			f, err := AsFloat(arg)
			if err != nil {
				return nil, NewExpressionError(e, err)
			}

			v := math.Abs(f)
			ctx.Log.V(8).Info("eval ready", "expression", e.String(), "args", f, "result", v)
			return v, nil

		case "@ceil":
			f, err := AsFloat(arg)
			if err != nil {
				return nil, NewExpressionError(e, err)
			}

			v := math.Ceil(f)
			ctx.Log.V(8).Info("eval ready", "expression", e.String(), "args", f, "result", v)
			return v, nil

		case "@floor":
			f, err := AsFloat(arg)
			if err != nil {
				return nil, NewExpressionError(e, err)
			}

			v := math.Floor(f)
			ctx.Log.V(8).Info("eval ready", "expression", e.String(), "args", f, "result", v)
			return v, nil

			// list ops
		case "@sum":
			is, fs, kind, err := AsIntOrFloatList(arg)
			if err != nil {
				return nil, NewExpressionError(e, err)
			}

			var v any
			if kind == reflect.Int64 {
				vi := int64(0)
				for i := range is {
					vi += is[i]
				}
				v = vi
			} else {
				vf := 0.0
				for i := range fs {
					vf += fs[i]
				}
				v = vf
			}

			ctx.Log.V(8).Info("eval ready", "expression", e.String(), "arg", arg, "result", v)
			return v, nil

		case "@len":
			args, err := AsList(arg)
			if err != nil {
				return nil, NewExpressionError(e, err)
			}

			v := int64(len(args))
			ctx.Log.V(8).Info("eval ready", "expression", e.String(), "arg", args, "result", v)
			return v, nil

		case "@in": // @in: [elem, list]
			args, err := AsList(arg)
			if err != nil {
				return nil, NewExpressionError(e, err)
			}

			if len(args) != 2 {
				return nil, NewExpressionError(e, errors.New("expected 2 arguments"))
			}

			elem := args[0]
			list, err := AsList(args[1])
			if err != nil {
				return nil, NewExpressionError(e, err)
			}

			v := false
			for i := range list {
				if reflect.DeepEqual(list[i], elem) {
					v = true
					break
				}
			}

			ctx.Log.V(8).Info("eval ready", "expression", e.String(), "arg", args, "result", v)
			return v, nil

		case "@concat":
			args, err := AsStringList(arg)
			if err != nil {
				return nil, NewExpressionError(e, err)
			}

			v := ""
			for i := range args {
				v += args[i]
			}

			ctx.Log.V(8).Info("eval ready", "expression", e.String(), "arg", args, "result", v)

			return v, nil

		case "@hash":
			js, err := json.Marshal(arg)
			if err != nil {
				return nil, fmt.Errorf("@hash: failed to marshal value to JSON: %w", err)
			}

			v := base36.Md5Base36(string(js))
			if len(v) < 6 {
				v += strings.Repeat("x", 6-len(v))
			} else {
				v = v[0:6]
			}

			ctx.Log.V(8).Info("eval ready", "expression", e.String(), "args", arg, "result", v)

			return v, nil

		default:
			return nil, NewExpressionError(e, errors.New("unknown op"))
		}
	}

	// literal map
	return Unstructured{e.Op: arg}, nil
}

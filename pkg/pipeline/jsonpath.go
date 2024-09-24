package pipeline

import (
	"errors"
	"fmt"

	"github.com/ohler55/ojg/jp"
)

func (e *Expression) GetJSONPath(ctx evalCtx, key string) (any, error) {
	if len(key) == 0 || key[0] != '$' {
		return key, nil
	}

	// handle root ref "$." that is not handled by ojg/jp for some reason
	if key == "$." {
		key = "$" // $ "$" will be stripped, plain "" is accepted as a root ref
	} else if key == "$$." {
		key = "$$" // $ "$$" will be stripped, plain "" is accepted as a root ref
	}

	// $... is object
	subject := ctx.object
	// $$... is local subject (@map, @filter, etc.)
	if len(key) >= 2 && key[0] == '$' && key[1] == '$' && ctx.subject != nil {
		// remove first $
		key = key[1:]
		subject = ctx.subject
	}
	ret, err := GetJSONPathExp(key, subject)
	if err != nil {
		return nil, NewExpressionError(e, err)
	}
	return ret, nil
}

func (e *Expression) SetJSONPath(ctx evalCtx, key string, value, data any) error {
	if len(key) == 0 {
		return errors.New("empty key")
	}

	// first get the value
	if str, ok := value.(string); ok {
		res, err := e.GetJSONPath(ctx, str)
		if err != nil {
			return NewExpressionError(e, err)
		}
		value = res
	}

	// copy: if key is a JSONpath root ref and the result is a map, overwrite the entire map
	if d, ok := data.(Unstructured); ok && key == "$." {
		if val, ok := value.(Unstructured); ok {
			// cannot just overwrite the map as this would not affect the caller, we
			// have to remove all existing keys and copy new keys
			for k := range val {
				delete(d, k)
			}
			for k, v := range val {
				d[k] = v
			}
			return nil
		}
		return NewExpressionError(e, fmt.Errorf("JSONPath expression error: cannot set root "+
			"key \"$.\" to value %q of type %T, only map types can be copied", value, value))
	}

	// if not a JSONpath, just set it as is
	if d, ok := data.(Unstructured); ok && key[0] != '$' {
		d[key] = value
		return nil
	}

	// then call the low-level set util
	if err := SetJSONPathExp(key, value, data); err != nil {
		return NewExpressionError(e, fmt.Errorf("JSONPath expression error: cannot set "+
			"key %q to value %q: %w", key, value, err))
	}

	return nil
}

// low-level utils

// GetJSONPathExp evaluates a JSONPath expression on the specified object and returns the result or
// an error.
func GetJSONPathExp(query string, object any) (any, error) {
	je, err := jp.ParseString(query)
	if err != nil {
		return nil, err
	}

	// jsonpath works on implicit object context
	values := je.Get(object)
	if len(values) == 0 {
		// return nil, NewExpressionError("JSONPath", jsonExp,
		// 	fmt.Errorf("failed to apply JSONPath expression on %#v", input))
		return nil, nil
	}

	return values[0], nil
}

// SetJSONPathExp sets a key (possibly represented with a JSONPath expression) to a value (can also
// be a JSONPath expression, which will be evaluated using the object argument) in the given data
// structure.
func SetJSONPathExp(key string, value, target any) error {
	je, err := jp.ParseString(key)
	if err != nil {
		return err
	}

	return je.Set(target, value)
}

// lit := []Expression{}
// for _, arg := range args {
// 	lit = append(lit, Expression{Op: "@any", Literal: arg, Raw: util.Stringify(arg)})
// }
// argList, err := asExpList(exp.Arg)
// Expect(err).NotTo(HaveOccurred())
// argList = append(argList, Expression{
// 	Op:      "@list",
// 	Literal: lit,
// })
// exp.Arg.Literal = argList

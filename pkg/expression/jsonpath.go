package expression

import (
	"errors"
	"fmt"

	"github.com/ohler55/ojg/jp"
)

// GetJSONPath evaluates a string op that may or may not be a JSONPath expession.
func GetJSONPath(ctx EvalCtx, key string) (any, error) {
	if len(key) == 0 || key[0] != '$' {
		return key, nil
	}

	// handle root ref "$." that is not handled by ojg/jp for some reason
	switch key {
	case "$.":
		key = "$" // $ "$" will be stripped, plain "" is accepted as a root ref
	case "$$.":
		key = "$$" // $ "$$" will be stripped, plain "" is accepted as a root ref
	}

	// $... is object
	subject := ctx.Object

	// $$... is local subject (@map, @filter, etc.)
	if len(key) >= 2 && key[0] == '$' && key[1] == '$' && ctx.Subject != nil {
		// remove first $
		key = key[1:]
		subject = ctx.Subject
	}

	ret, err := GetJSONPathRaw(key, subject)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

// SetJSONPath overwrites the data in-place with the given value at the given key. Leaves the rest
// of the data unchanged.
func SetJSONPath(ctx EvalCtx, key string, value, data any) error {
	if len(key) == 0 {
		return errors.New("empty key")
	}

	// first get the value
	if str, ok := value.(string); ok {
		res, err := GetJSONPath(ctx, str)
		if err != nil {
			return err
		}
		value = res
	}

	// copy: if key is a JSONpath root ref and the result is a map, overwrite the entire map
	// if d, ok := data.(Unstructured); ok && key == "$." {
	if key == "$." {
		d, okd := data.(map[string]any)
		val, okv := value.(map[string]any)
		if !okd || !okv {
			return fmt.Errorf("JSONPath expression error: cannot set root object of type %T "+
				"to value %q of type %T, only map types can be copied with %q", d,
				value, value, "$.")
		}

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

	// if not a JSONpath, just set it as is
	if d, ok := data.(map[string]any); ok && key[0] != '$' {
		d[key] = value
		return nil
	}

	// then call the low-level set util
	if err := SetJSONPathRaw(key, value, data); err != nil {
		return fmt.Errorf("JSONPath expression error: cannot set "+
			"key %q to value %q: %w", key, value, err)
	}

	return nil
}

// low-level utils

// GetJSONPathRaw evaluates a JSONPath expression on the specified object and returns the result or
// an error.
func GetJSONPathRaw(query string, object any) (any, error) {
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

// SetJSONPathRaw sets a key (possibly represented with a JSONPath expression) to a value (can also
// be a JSONPath expression, which will be evaluated using the object argument) in the given data
// structure.
func SetJSONPathRaw(key string, value, target any) error {
	je, err := jp.ParseString(key)
	if err != nil {
		return err
	}

	return je.Set(target, value)
}

// SetJSONPathRawExp is the same as SetJSONPathRaw but takes the key as a string expression.
func SetJSONPathRawExp(keyExp *Expression, value, data any) error {
	key, err := keyExp.GetLiteralString()
	if err != nil {
		return err
	}

	if err := SetJSONPathRaw(key, value, data); err != nil {
		return err
	}

	return nil
}

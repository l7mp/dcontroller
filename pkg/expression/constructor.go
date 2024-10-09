package expression

import "fmt"

// NewLiteralExpression creates a new literal expression with the given argument.
func NewLiteralExpression(value any) (Expression, error) {
	op := ""
	switch value.(type) {
	case bool:
		op = "@bool"
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		op = "@int"
	case string:
		op = "@string"
	case float32, float64:
		op = "float"
	default:
		return Expression{}, fmt.Errorf("cannot create a literal expression from an "+
			"argument %#v", value)
	}

	return Expression{Op: op, Literal: value}, nil
}

// NewJSONPathGetExpression creates an expression that, when evaluated on an object, will return
// the value at the given key.
func NewJSONPathGetExpression(key string) Expression {
	return Expression{Op: "@string", Literal: key}
}

// NewJSONPathSetExpression creates an expression that, when evaluated on an object, will set the
// value at the given key to the value.
func NewJSONPathSetExpression(key string, value any) (Expression, error) {
	lit, err := NewLiteralExpression(value)
	if err != nil {
		return Expression{}, err
	}
	return Expression{Op: "@dict", Literal: map[string]Expression{key: lit}}, nil
}

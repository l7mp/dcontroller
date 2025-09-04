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

// GetLiteralBool returns a liteal bool from an expression.
func (e *Expression) GetLiteralBool() (bool, error) {
	ret, err := AsBool(e.Literal)
	if err != nil {
		return false, err
	}
	return ret, nil
}

// GetLiteralInt returns a liteal integer from an expression.
func (e *Expression) GetLiteralInt() (int64, error) {
	ret, err := AsInt(e.Literal)
	if err != nil {
		return 0, err
	}
	return ret, nil
}

// GetLiteralString returns a liteal string from an expression.
func (e *Expression) GetLiteralString() (string, error) {
	ret, err := AsString(e.Literal)
	if err != nil {
		return "", err
	}
	return ret, nil
}

// GetLiteralFloat returns a liteal floating point number from an expression.
func (e *Expression) GetLiteralFloat() (float64, error) {
	ret, err := AsFloat(e.Literal)
	if err != nil {
		return 0.0, err
	}
	return ret, nil
}

// GetLiteralList returns a liteal list from an expression.
func (e *Expression) GetLiteralList() ([]any, error) {
	ret, err := AsList(e.Literal)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

// GetLiteralMap returns a liteal set of key-value pairs from an expression.
func (e *Expression) GetLiteralMap() (map[string]any, error) {
	ret, err := AsMap(e.Literal)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

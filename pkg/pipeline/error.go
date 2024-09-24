package pipeline

import (
	"fmt"
)

type ErrInvalidArguments = error

func NewInvalidArgumentsError(content string) ErrInvalidArguments {
	return fmt.Errorf("invalid arguments at %q", content)
}

type ErrUnmarshal = error

func NewUnmarshalError(kind, content string) ErrUnmarshal {
	return fmt.Errorf("JSON parsing error in %s at %q", kind, content)
}

type ErrExpression = error

func NewExpressionError(e *Expression, err error) ErrExpression {
	return fmt.Errorf("failed to evaluate %s expression %s: %w", e.Op, e.String(), err)
}

// func NewExpressionError(kind, content string, err error) ErrExpression {
// 	return fmt.Errorf("failed to evaluate %s expression %q: %w", kind, content, err)
// }

type ErrAggregation = error

func NewAggregationError(err error) ErrAggregation {
	return fmt.Errorf("failed to evaluate aggregation expression: %w", err)
}

type ErrJoin = error

func NewJoinError(err error) ErrJoin {
	return fmt.Errorf("failed to evaluate join expression: %w", err)
}

type ErrInvalidObject = error

func NewInvalidObjectError(message string) ErrInvalidObject {
	return fmt.Errorf("invalid object: %s", message)
}

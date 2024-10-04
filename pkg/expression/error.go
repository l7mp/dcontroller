package expression

import (
	"fmt"
)

// ErrInvalidArguments is a custom error.
type ErrInvalidArguments = error

// NewInvalidArgumentsError creates a new custom error.
func NewInvalidArgumentsError(content string) ErrInvalidArguments {
	return fmt.Errorf("invalid arguments at %q", content)
}

// ErrUnmarshal is a custom error.
type ErrUnmarshal = error

// NewUnmarshalError creates a new custom error.
func NewUnmarshalError(kind, content string) ErrUnmarshal {
	return fmt.Errorf("JSON parsing error in %s at %q", kind, content)
}

// ErrExpression is a custom error.
type ErrExpression = error

// NewExpressionError creates a new custom error.
func NewExpressionError(e *Expression, err error) ErrExpression {
	return fmt.Errorf("failed to evaluate %s expression %s: %w", e.Op, e.String(), err)
}

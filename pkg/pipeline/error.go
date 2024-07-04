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

func NewExpressionError(e *Expression, kind, content string) ErrExpression {
	return fmt.Errorf("cannot evaluate %s expression %q (parsed: %s)", kind, content, e.String())
}

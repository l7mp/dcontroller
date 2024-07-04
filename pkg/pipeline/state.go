package pipeline

import (
	"reflect"

	"github.com/go-logr/logr"

	"hsnlab/dcontroller-runtime/pkg/object"
)

type VerdictType int

const (
	Continue VerdictType = iota + 1
	Stop
	Drop
	Error
)

type State struct {
	View      string
	Object    *object.Object
	Variables map[string]any
	Log       logr.Logger
	// Verdict VerdictType
}

func (s *State) Normalize() error {
	str := s.Object.String()

	// view: cannot be empty
	if s.View == "" {
		return NewInvalidObjectError("empty view name")
	}
	s.Object.SetKind(s.View)

	// name: cannot be empty
	sv, err := EvalJSONpathExp(s, ".metadata.name", str)
	if err != nil {
		return err
	}
	if reflect.ValueOf(sv).Kind() != reflect.String {
		return NewInvalidObjectError("name must be a string")
	}
	name := sv.(string)
	if name == "" {
		return NewInvalidObjectError("empty name in projection result")
	}
	s.Object.SetName(name)

	// namespace: can be empty
	sv, err = EvalJSONpathExp(s, ".metadata.namespace", str)
	if err != nil {
		return err
	}
	if reflect.ValueOf(sv).Kind() != reflect.String {
		return NewInvalidObjectError("namespace must be a string")
	}
	namespace := sv.(string)
	s.Object.SetNamespace(namespace)

	return nil
}

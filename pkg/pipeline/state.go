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

func (s *State) Normalize(view string) error {
	s.View = view
	s.Object.SetKind(s.View)

	// metadata: must exist
	meta, ok := s.Object.Object["metadata"]
	if !ok {
		return NewInvalidObjectError("no .metadata in object")
	}
	metaMap, ok := meta.(map[string]any)
	if !ok {
		return NewInvalidObjectError("invalid .metadata in object")
	}

	// name must be defined
	name, ok := metaMap["name"]
	if !ok {
		return NewInvalidObjectError("missing name")
	}
	if reflect.ValueOf(name).Kind() != reflect.String {
		return NewInvalidObjectError("name must be a string")
	}
	nameStr := name.(string)
	if nameStr == "" {
		return NewInvalidObjectError("empty name in projection result")
	}
	s.Object.SetName(nameStr)

	// namespace: can be empty
	namespace, ok := metaMap["namespace"]
	if !ok {
		metaMap["namespace"] = ""
	} else {
		if reflect.ValueOf(namespace).Kind() != reflect.String {
			return NewInvalidObjectError("namespace must be a string")
		}
		namespaceStr := namespace.(string)
		metaMap["namespace"] = namespaceStr
	}

	return nil
}

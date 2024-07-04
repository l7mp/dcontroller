package pipeline

import "hsnlab/dcontroller-runtime/pkg/object"

type VerdictType int

const (
	Continue VerdictType = iota + 1
	Stop
	Drop
	Error
)

type State struct {
	Object    *object.Object
	Variables map[string]any
	// Verdict VerdictType
}

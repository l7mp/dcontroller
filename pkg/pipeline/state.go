package pipeline

import (
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
	Object    *object.Object
	Variables map[string]any
	Log       logr.Logger
	// Verdict VerdictType
}

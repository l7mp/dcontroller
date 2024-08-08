package cache

import (
	"hsnlab/dcontroller-runtime/pkg/object"

	toolscache "k8s.io/client-go/tools/cache"
)

const (
	Added    = toolscache.Added
	Deleted  = toolscache.Deleted
	Updated  = toolscache.Updated
	Replaced = toolscache.Replaced
	Sync     = toolscache.Sync
)

// NilDelta is a placeholder for a delta that means no change.
var NilDelta = Delta{Object: nil}

// Delta registers a change (addition, deletion, etc) on an object. By convention, Object is nil if no change occurs.
type Delta struct {
	Object *object.Object
	Type   toolscache.DeltaType
}

func (d Delta) IsUnchanged() bool { return d.Object == nil }

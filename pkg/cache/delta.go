package cache

import (
	"fmt"

	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"hsnlab/dcontroller/pkg/object"
)

type DeltaType = toolscache.DeltaType

const (
	Added    = toolscache.Added
	Deleted  = toolscache.Deleted
	Updated  = toolscache.Updated
	Replaced = toolscache.Replaced
	Sync     = toolscache.Sync
	Upserted = "Upserted" // for events that are either an update/replace or an add
)

// NilDelta is a placeholder for a delta that means no change.
var NilDelta = Delta{Object: nil}

// Delta registers a change (addition, deletion, etc) on an object. By convention, Object is nil if no change occurs.
type Delta struct {
	Object object.Object
	Type   DeltaType
}

func (d Delta) IsUnchanged() bool { return d.Object == nil }

func (d Delta) String() string {
	key := ""
	if d.Object != nil {
		key = client.ObjectKeyFromObject(d.Object).String()
	}
	return fmt.Sprintf("Delta(Type:%s,Object:%s)", d.Type, key)
}

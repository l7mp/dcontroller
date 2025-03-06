package cache

import (
	"fmt"

	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/hsnlab/dcontroller/pkg/object"
)

type DeltaType string

const (
	Added    = DeltaType(toolscache.Added)
	Deleted  = DeltaType(toolscache.Deleted)
	Updated  = DeltaType(toolscache.Updated)
	Replaced = DeltaType(toolscache.Replaced)
	Sync     = DeltaType(toolscache.Sync)
	Upserted = DeltaType("Upserted") // for events that are either an update/replace or an add
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
	key := "<empty>"
	if d.Object != nil {
		key = client.ObjectKeyFromObject(d.Object).String()
	}
	return fmt.Sprintf("Delta(Type:%s,Object:%s)", d.Type, key)
}

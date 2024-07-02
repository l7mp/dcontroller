package engine

import "hsnlab/dcontroller-runtime/pkg/object"

type Pipeline interface {
	Process(obj *object.Object) (*object.Object, error)
}

package engine

import "hsnlab/dcontroller-runtime/pkg/object"

type pipeline struct{}

func NewEmptyPipeline() Pipeline { return &pipeline{} }

func (p *pipeline) Process(obj *object.Object) (*object.Object, error) {
	return obj, nil
}

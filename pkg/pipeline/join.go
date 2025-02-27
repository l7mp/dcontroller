package pipeline

import (
	"fmt"

	opv1a1 "github.com/hsnlab/dcontroller/pkg/api/operator/v1alpha1"
	"github.com/hsnlab/dcontroller/pkg/cache"
)

var _ Evaluator = &Join{}

const joinOp = "@join"

// Join is an operation that can be used to perform an inner Join on a list of views.
type Join struct {
	*opv1a1.Join
	engine Engine
}

// NewJoin creates a new join from a seralized representation.
func NewJoin(engine Engine, config *opv1a1.Join) *Join {
	if config == nil {
		return nil
	}
	return &Join{
		Join:   config,
		engine: engine,
	}
}
func (j *Join) String() string {
	return fmt.Sprintf("%s:{%s}", joinOp, j.Expression.String())
}

// Evaluate processes a join expression on the given deltas. Returns the new deltas if there were
// no errors and an error otherwise.
func (j *Join) Evaluate(delta cache.Delta) ([]cache.Delta, error) {
	eng := j.engine
	res, err := eng.EvaluateJoin(j, delta)
	if err != nil {
		return nil, NewJoinError(err)
	}

	return res, nil
}

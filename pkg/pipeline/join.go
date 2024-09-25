package pipeline

import (
	"fmt"
	"hsnlab/dcontroller/pkg/cache"
)

const joinOp = "@join"

// Join is an operation that can be used to perform an inner join on a list of views.
type Join struct {
	Expression Expression `json:"@join"`
}

func (j *Join) String() string {
	return fmt.Sprintf("%s:{%s}", joinOp, j.Expression.String())
}

// Evaluate processes a join expression on the given deltas. Returns the new deltas if there were
// no errors and an error otherwise.
func (j *Join) Evaluate(eng Engine, delta cache.Delta) ([]cache.Delta, error) {
	res, err := eng.EvaluateJoin(j, delta)
	if err != nil {
		return nil, NewJoinError(err)
	}

	return res, nil
}

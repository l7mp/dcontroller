package pipeline

import (
	"fmt"
	"strings"

	"hsnlab/dcontroller/pkg/cache"
)

const aggregateOp = "@aggregate"

// Aggregation is an operation that can be used to process, objects, or alter the shape of a list
// of objects in a view.
type Aggregation struct {
	Expressions []Expression `json:"@aggregate"`
}

func (a *Aggregation) String() string {
	ss := []string{}
	for _, e := range a.Expressions {
		ss = append(ss, e.String())
	}
	return fmt.Sprintf("%s:[%s]", aggregateOp, strings.Join(ss, ","))
}

// Evaluate processes an aggregation expression on the given delta.
func (a *Aggregation) Evaluate(eng Engine, delta cache.Delta) ([]cache.Delta, error) {
	res, err := eng.EvaluateAggregation(a, delta)
	if err != nil {
		return nil, NewAggregationError(fmt.Errorf("aggregation error: %w", err))
	}

	return res, nil
}

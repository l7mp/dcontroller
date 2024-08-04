package pipeline

import (
	"fmt"
	"hsnlab/dcontroller-runtime/pkg/object"

	"github.com/go-logr/logr"
)

// Aggregation is an operation that can be used to process, objects, or alter the shape of a list
// of objects in a view.
type Aggregation struct {
	Expression
}

// Run processes an aggregation expression.
func (a *Aggregation) Run(view string, objects []*object.Object, log logr.Logger) ([]*object.Object, error) {
	eng := NewEngine(view, log).WithObjects(objects)

	res, err := a.Evaluate(eng)
	if err != nil {
		return nil, NewAggregationError(a.String(),
			fmt.Errorf("aggregation error: %q", err))
	}

	ret, err := eng.Normalize(res)
	if err != nil {
		return nil, NewAggregationError(a.String(),
			fmt.Errorf("aggregation error: %q", err))
	}

	return ret, nil
}

// Evaluate processes an aggregation expression on the given state.
func (a *Aggregation) Evaluate(eng *Engine) (any, error) {
	res, err := a.Expression.Evaluate(eng)
	if err != nil {
		return nil, NewAggregationError(a.String(),
			fmt.Errorf("aggregation error: %q", err))
	}

	return res, nil
}

func (a *Aggregation) String() string {
	return fmt.Sprintf("aggregation:{%s}", a.Expression.String())
}

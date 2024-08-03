package pipeline

import (
	"fmt"
	"hsnlab/dcontroller-runtime/pkg/object"

	"github.com/go-logr/logr"
)

// Aggregation is an operation that can be used to filter objects by certain condition or alter the
// object's shape.
type Aggregation struct {
	Expression
}

// Evaluate processes an aggregation expression on the given state. Returns the new state
// if there were no errors, nil if there were no errors but the pipeline execution should
// stop, and error otherwise.
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

// Evaluate processes an aggregation expression on the given state. Returns the new state
// if there were no errors, nil if there were no errors but the pipeline execution should
// stop, and error otherwise.
func (a *Aggregation) Evaluate(eng *Engine) (any, error) {
	res, err := a.Expression.Evaluate(eng)
	if err != nil {
		return nil, NewAggregationError(a.String(),
			fmt.Errorf("aggregation error: %q", err))
	}

	return res, nil
}

// func (a *Aggregation) UnmarshalJSON(b []byte) error {
// 	// single arg
// 	fv := Expression{}
// 	if err := json.Unmarshal(b, &fv); err == nil {
// 		*a = Aggregation{Expression: fv}
// 	}

// 	return NewUnmarshalError("aggregation", string(b))
// }

func (a *Aggregation) String() string {
	return fmt.Sprintf("aggregation:{%s}", a.Expression.String())
}

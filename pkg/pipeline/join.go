package pipeline

import (
	"encoding/json"
	"fmt"
	"hsnlab/dcontroller-runtime/pkg/cache"
)

// Join is an operation that can be used to perform an inner join on a list of views.
type Join struct {
	Op         string
	Expression Expression
	Raw        string
}

func (j *Join) String() string {
	return fmt.Sprintf("%s:{views:%s}", j.Op, j.Expression.String())
}

// Evaluate processes a join expression. Returns the new state if there were no errors, nil if
// there were no errors but the pipeline execution should stop, and error otherwise.
func (j *Join) Evaluate(eng Engine, delta cache.Delta) ([]cache.Delta, error) {
	res, err := eng.EvaluateJoin(j, delta)
	if err != nil {
		return nil, NewAggregationError(j.String(),
			fmt.Errorf("join error: %w", err))
	}

	return res, nil
}

func (j *Join) UnmarshalJSON(b []byte) error {
	jv := map[string]Expression{}
	if err := json.Unmarshal(b, &jv); err != nil {
		return NewUnmarshalError("join",
			fmt.Sprintf("%q: %s", string(b), err.Error()))
	}

	// check
	if _, ok := jv["@join"]; !ok || len(jv) != 1 {
		return NewUnmarshalError("join",
			fmt.Sprintf("expected a @join op in %q", string(b)))
	}

	*j = Join{Op: "@join", Expression: jv["@join"], Raw: string(b)}

	return nil
}

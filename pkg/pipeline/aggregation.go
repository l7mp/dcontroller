package pipeline

import (
	"encoding/json"
	"fmt"
	"hsnlab/dcontroller-runtime/pkg/cache"
	"strings"
)

// Aggregation is an operation that can be used to process, objects, or alter the shape of a list
// of objects in a view.
type Aggregation struct {
	Op          string
	Expressions []Expression
	Raw         string
}

func (a *Aggregation) String() string {
	ss := []string{}
	for _, e := range a.Expressions {
		ss = append(ss, e.String())
	}
	return fmt.Sprintf("%s:[%s]", a.Op, strings.Join(ss, ","))
}

// Evaluate processes an aggregation expression on the given state.
func (a *Aggregation) Evaluate(eng Engine, delta cache.Delta) (cache.Delta, error) {
	res, err := eng.EvaluateAggregation(a, delta)
	if err != nil {
		return cache.Delta{}, NewAggregationError(a.String(),
			fmt.Errorf("aggregation error: %w", err))
	}

	return res, nil
}

func (a *Aggregation) UnmarshalJSON(b []byte) error {
	//map key @aggregate, value is a @list expr for the stages
	av := map[string]Expression{}
	if err := json.Unmarshal(b, &av); err != nil {
		return NewUnmarshalError("aggregation",
			fmt.Sprintf("%q: %s", string(b), err.Error()))
	}

	if _, ok := av["@aggregate"]; !ok || len(av) != 1 {
		return NewUnmarshalError("aggregation",
			fmt.Sprintf("expected a single @aggregate op in %q", string(b)))
	}

	ls, err := asExpOrList(av["@aggregate"])
	if err != nil {
		return NewUnmarshalError("aggregation",
			fmt.Sprintf("invalid aggregation stage list in %q", av["@aggregate"].Raw))
	}

	*a = Aggregation{Op: "@aggregate", Expressions: ls}

	return nil
}

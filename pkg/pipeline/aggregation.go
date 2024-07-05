package pipeline

import (
	"encoding/json"
	"errors"
	"fmt"

	"hsnlab/dcontroller-runtime/pkg/object"
	"hsnlab/dcontroller-runtime/pkg/util"
)

// Aggregation is an operation that can be used to filter objects by certain condition or alter the
// object's shape.
type Aggregation struct {
	// Filter can be used to drop objects based on a boolean condition.
	Filter *Filter
	// Project can be used to remove fields from an object.
	Project *Project
	// Map can be used to change certain fields in an object.
	Map *Map
}

// Evaluate processes an aggregation expression on the given state. Returns the new state
// if there were no errors, nil if there were no errors but the pipeline execution should
// stop, and error otherwise.
func (a *Aggregation) Evaluate(state *State) (*State, error) {
	if a.Filter != nil {
		return a.Filter.Evaluate(state)
	} else if a.Project != nil {
		return a.Project.Evaluate(state)
	} else if a.Map != nil {
		return a.Map.Evaluate(state)
	}

	return nil, NewAggregationError("aggregation", a.String(), errors.New("invalid aggregation"))
}

func (a *Aggregation) UnmarshalJSON(b []byte) error {
	fv := map[string]Expression{}
	if err := json.Unmarshal(b, &fv); err == nil && len(fv) == 1 {
		for k, v := range fv {
			if k == "@filter" {
				f := &Filter{Condition: v}
				*a = Aggregation{Filter: f}
			}
		}
	}

	// parse Project and Map as an Unstructured: hopefully it has a sane deserializer
	pmv := map[string]Expression{}
	if err := json.Unmarshal(b, &pmv); err == nil && len(pmv) == 1 {
		for k, v := range pmv {
			switch k {
			case "@project":
				p := &Project{Projector: v}
				*a = Aggregation{Project: p}
			case "@map":
				m := &Map{Mapper: v}
				*a = Aggregation{Map: m}
			}
		}
	}

	return NewUnmarshalError("aggregation", string(b))
}

func (a *Aggregation) String() string {
	if a.Filter != nil {
		return a.Filter.String()
	} else if a.Project != nil {
		return a.Project.String()
	} else if a.Map != nil {
		return a.Map.String()
	}

	return "<invalid>"
}

// Project
type Project struct {
	Projector Expression
}

func (p *Project) Evaluate(state *State) (*State, error) {
	res, err := p.Projector.Evaluate(state)
	if err != nil {
		return nil, NewAggregationError("@project", p.Projector.Raw, err)
	}
	mv, ok := res.(map[string]any)
	if !ok {
		return nil, NewAggregationError("@project", p.Projector.Raw,
			errors.New("should evaluate to a map[string]any"))
	}

	newState := &State{
		View:      state.View,
		Object:    object.New(state.View).WithContent(mv),
		Variables: state.Variables,
		Log:       state.Log,
	}

	if err := newState.Normalize(); err != nil {
		return nil, NewAggregationError("@project", newState.Object.String(), err)
	}

	return newState, nil
}

func (p *Project) String() string {
	return fmt.Sprintf("@project: %s", p.Projector.String())
}

// Map
type Map struct {
	Mapper Expression
}

func (m *Map) Evaluate(state *State) (*State, error) {
	res, err := m.Mapper.Evaluate(state)
	if err != nil {
		return nil, NewAggregationError("@map", m.Mapper.Raw, err)
	}
	mv, ok := res.(map[string]any)
	if !ok {
		return nil, NewAggregationError("@map", m.Mapper.Raw,
			errors.New("should evaluate to a map[string]any"))
	}

	if err := state.Object.Patch(mv); err != nil {
		return nil, NewAggregationError("@map", util.Stringify(mv), err)
	}

	if err := state.Normalize(); err != nil {
		return nil, NewAggregationError("@map", util.Stringify(mv), err)
	}

	return state, nil
}

func (m *Map) String() string {
	return fmt.Sprintf("@map: %s", m.Mapper.String())
}

// Filter
type Filter struct {
	Condition Expression
}

func (f *Filter) Evaluate(state *State) (*State, error) {
	res, err := f.Condition.Evaluate(state)
	if err != nil {
		return nil, NewAggregationError("filter", f.Condition.Raw, err)
	}

	v, err := f.Condition.asBool(res)
	if err != nil {
		return nil, NewExpressionError("bool", f.Condition.Raw, err)
	}

	state.Log.V(4).Info("aggregation eval ready", "aggreg", f.String(), "result", v)

	if v {
		return state, nil
	}

	return nil, nil
}

func (f *Filter) String() string {
	return fmt.Sprintf("@filter: %s", f.Condition.String())
}

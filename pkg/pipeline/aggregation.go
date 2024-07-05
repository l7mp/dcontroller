package pipeline

import (
	"encoding/json"
	"errors"
	"fmt"

	"hsnlab/dcontroller-runtime/pkg/object"
	"hsnlab/dcontroller-runtime/pkg/util"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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
			switch k {
			case "@filter":
				f := &Filter{Condition: v}
				*a = Aggregation{Filter: f}
				return nil
			case "@project":
				p := &Project{Projector: v}
				*a = Aggregation{Project: p}
				return nil
			case "@map":
				m := &Map{Mapper: v}
				*a = Aggregation{Map: m}
				return nil
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

	state.Log.V(4).Info("project: projection expression eval ready",
		"aggreg", p.String(), "result", mv)

	obj := object.New(state.View)
	if name, ok, err := unstructured.NestedString(mv, "metadata", "name"); err != nil {
		return nil, NewAggregationError("@project", p.Projector.Raw,
			fmt.Errorf("invalid 'name' in projection result:%w", err))
	} else {
		if !ok {
			return nil, NewAggregationError("@project", p.Projector.Raw,
				errors.New("missing 'name' in projection result:%w"))
		}
		obj.SetName(name)
	}
	if namespace, ok, err := unstructured.NestedString(mv, "metadata", "namespace"); err != nil {
		return nil, NewAggregationError("@project", p.Projector.Raw,
			fmt.Errorf("invalid 'namespace' in projection result:%w", err))
	} else {
		if !ok {
			state.Log.Info("@project:missing 'namespace' in projection result",
				"projection", p.String(), "result", util.Stringify(mv))
			// cannot use SetNested*: it removes key-value for an empty argument
			// .metadata is guaranteed to exist at this point
			obj.Object["metadata"].(map[string]any)["namespace"] = ""
		} else {
			obj.SetNamespace(namespace)
		}
	}
	obj.WithContent(mv)

	newState := &State{
		View:      state.View,
		Object:    obj,
		Variables: state.Variables,
		Log:       state.Log,
	}

	if err := newState.Normalize(state.View); err != nil {
		return nil, NewAggregationError("@project", newState.Object.String(), err)
	}

	state.Log.V(2).Info("project: new object ready", "aggreg", p.String(),
		"result", newState.Object.String())

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

	if err := state.Normalize(state.View); err != nil {
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

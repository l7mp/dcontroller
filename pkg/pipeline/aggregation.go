package pipeline

import (
	"fmt"
	"strings"

	opv1a1 "github.com/hsnlab/dcontroller/pkg/api/operator/v1alpha1"
	"github.com/hsnlab/dcontroller/pkg/cache"
)

var _ Evaluator = &Aggregation{}

const aggregateOp = "@aggregate"

// Aggregation is an operation that can be used to process, objects, or alter the shape of a list
// of objects in a view.
type Aggregation struct {
	*opv1a1.Aggregation
	Stages []*Stage
	engine Engine
}

// NewAggregation creates a new aggregation from a seralized representation.
func NewAggregation(engine Engine, config *opv1a1.Aggregation) *Aggregation {
	if config == nil {
		return nil
	}

	a := &Aggregation{
		Aggregation: config,
		Stages:      make([]*Stage, len(config.Expressions)),
		engine:      engine,
	}

	for i, e := range config.Expressions {
		a.Stages[i] = NewStage(engine, &e)
	}

	return a
}

func (a *Aggregation) String() string {
	ss := []string{}
	for _, s := range a.Stages {
		ss = append(ss, s.String())
	}
	return fmt.Sprintf("%s:[%s]", aggregateOp, strings.Join(ss, ","))
}

// Evaluate processes an aggregation expression on the given delta.
func (a *Aggregation) Evaluate(delta cache.Delta) ([]cache.Delta, error) {
	eng := a.engine
	res, err := eng.EvaluateAggregation(a, delta)
	if err != nil {
		return nil, err
	}

	return res, nil
}

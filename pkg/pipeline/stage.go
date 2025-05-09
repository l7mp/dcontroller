package pipeline

import (
	"github.com/l7mp/dcontroller/pkg/cache"
	"github.com/l7mp/dcontroller/pkg/expression"
)

var _ Evaluator = &Stage{}

// Stage is a single operation in an aggregation.
type Stage struct {
	*expression.Expression
	inCache *cache.Store
	engine  Engine
}

// NewStage creates a new stage from a single aggregation stage.
func NewStage(engine Engine, e *expression.Expression) *Stage {
	if e == nil {
		return nil
	}

	s := &Stage{
		Expression: e,
		engine:     engine,
	}

	if e.Op == "@gather" {
		s.inCache = cache.NewStore()
	}

	return s
}

func (s *Stage) String() string {
	return s.Expression.String()
}

// Evaluate processes an aggregation stage on the given delta.
func (s *Stage) Evaluate(delta cache.Delta) ([]cache.Delta, error) {
	eng := s.engine
	res, err := eng.EvaluateStage(s, delta)
	if err != nil {
		return nil, err
	}

	return res, nil
}

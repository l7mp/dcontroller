package pipeline

import (
	"fmt"

	"github.com/go-logr/logr"
	"github.com/l7mp/dcontroller/pkg/dbsp"
	"github.com/l7mp/dcontroller/pkg/expression"
)

type SelectEvaluator struct {
	e   *expression.Expression
	log logr.Logger
}

func (eval *SelectEvaluator) String() string {
	return fmt.Sprintf("eval:%s", eval.e)
}

func (eval *SelectEvaluator) Evaluate(doc dbsp.Document) ([]dbsp.Document, error) {
	ret := []dbsp.Document{}

	res, err := eval.e.Arg.Evaluate(expression.EvalCtx{Object: doc, Log: eval.log})
	if err != nil {
		return nil, err
	}

	b, err := expression.AsBool(res)
	if err != nil {
		return nil, fmt.Errorf("expected conditional expression to evaluate to "+
			"boolean: %w", err)
	}

	// default is no change
	if b {
		ret = append(ret, doc)
	}

	return ret, nil
}

func (p *Pipeline) NewSelectOp(e *expression.Expression) dbsp.Operator {
	eval := &SelectEvaluator{e: e, log: p.log}
	return dbsp.NewSelection(eval.String(), eval)
}

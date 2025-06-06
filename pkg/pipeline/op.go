package pipeline

import (
	"fmt"

	"github.com/go-logr/logr"
	"github.com/l7mp/dcontroller/pkg/dbsp"
	"github.com/l7mp/dcontroller/pkg/expression"
	"github.com/l7mp/dcontroller/pkg/object"
)

type SelectionEvaluator struct {
	e   *expression.Expression
	log logr.Logger
}

func (eval *SelectionEvaluator) String() string {
	return fmt.Sprintf("eval:%s", eval.e)
}

func (eval *SelectionEvaluator) Evaluate(doc dbsp.Document) ([]dbsp.Document, error) {
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

func (p *Pipeline) NewSelectionOp(e *expression.Expression) dbsp.Operator {
	eval := &SelectionEvaluator{e: e, log: p.log}
	return dbsp.NewSelection(eval)
}

type ProjectionEvaluator struct {
	e   *expression.Expression
	log logr.Logger
}

func (eval *ProjectionEvaluator) String() string {
	return fmt.Sprintf("eval:%s", eval.e)
}

func (eval *ProjectionEvaluator) Evaluate(doc dbsp.Document) ([]dbsp.Document, error) {
	res, err := eval.e.Arg.Evaluate(expression.EvalCtx{Object: doc, Log: eval.log})
	if err != nil {
		return nil, err
	}

	us, err := expression.AsObjectOrObjectList(res)
	if err != nil {
		return nil, err
	}

	// if project receives a list, merge the resultant objects
	var v any
	for _, u := range us {
		v, err = object.MergeAny(v, u)
		if err != nil {
			return nil, err
		}
	}

	ret, err := expression.AsObject(v)
	if err != nil {
		return nil, err
	}

	return []dbsp.Document{ret}, nil
}

func (p *Pipeline) NewProjectionOp(e *expression.Expression) dbsp.Operator {
	eval := &ProjectionEvaluator{e: e, log: p.log}
	return dbsp.NewProjection(eval)
}

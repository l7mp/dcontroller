package pipeline

import (
	"errors"
	"fmt"

	"github.com/go-logr/logr"
	"github.com/l7mp/dcontroller/pkg/dbsp"
	"github.com/l7mp/dcontroller/pkg/expression"
	"github.com/l7mp/dcontroller/pkg/object"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const trimEvalLen = 30

// Selection operator.
type SelectionOp struct {
	e   *expression.Expression
	log logr.Logger
}

func (eval *SelectionOp) String() string {
	return fmt.Sprintf("select:%s", trim(eval.e.String()))
}

func (eval *SelectionOp) Evaluate(doc dbsp.Document) ([]dbsp.Document, error) {
	ret := []dbsp.Document{}

	res, err := eval.e.Evaluate(expression.EvalCtx{Object: doc, Log: eval.log})
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate expression %s: %w",
			eval.String(), err)
	}

	b, err := expression.AsBool(res)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate pipeline stage %s: %w",
			eval.String(), err)
	}

	// default is no change
	if b {
		ret = append(ret, doc)
	}

	return ret, nil
}

func (p *Pipeline) NewSelectionOp(e *expression.Expression) dbsp.Operator {
	eval := &SelectionOp{e: e, log: p.log.WithName("@select")}
	return dbsp.NewSelection(eval)
}

// Projection operator.
type ProjectionOp struct {
	e   *expression.Expression
	log logr.Logger
}

func (eval *ProjectionOp) String() string {
	return fmt.Sprintf("project:%s", trim(eval.e.String()))
}

func (eval *ProjectionOp) Evaluate(doc dbsp.Document) ([]dbsp.Document, error) {
	res, err := eval.e.Evaluate(expression.EvalCtx{Object: doc, Log: eval.log})
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate expression %s: %w",
			eval.String(), err)
	}

	us, err := expression.AsObjectOrObjectList(res)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate pipeline stage %s: %w",
			eval.String(), err)
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
	eval := &ProjectionOp{e: e, log: p.log.WithName("@project")}
	return dbsp.NewProjection(eval)
}

// Unwind operator.
type UnwindOp struct {
	e   *expression.Expression
	log logr.Logger
}

func (eval *UnwindOp) String() string {
	return fmt.Sprintf("unwind:%s", trim(eval.e.String()))
}

type listElem struct {
	idx   int
	value any
}

func (eval *UnwindOp) Extract(doc dbsp.Document) (any, error) {
	// unwind requires a valid name to distinguish unwound objects
	if _, err := expression.GetJSONPathRaw("$.metadata.name", doc); err != nil {
		return nil, errors.New("valid .metadata.name required")
	}

	arg, err := eval.e.Evaluate(expression.EvalCtx{Object: doc, Log: eval.log})
	if err != nil {
		return nil, err
	}

	list := []any{}
	if arg != nil {
		list, err = expression.AsList(arg)
		if err != nil {
			return nil, err
		}
	}

	ret := make([]any, len(list))
	for i, v := range list {
		ret[i] = listElem{idx: i, value: v}
	}

	return ret, nil
}

func (eval *UnwindOp) Transform(doc dbsp.Document, v any) (dbsp.Document, error) {
	elem, ok := v.(listElem)
	if !ok {
		return nil, fmt.Errorf("list item not found in %s: expected list-elem", eval.String())
	}

	// the elem to the corresponding jsonpath
	jp, err := eval.e.GetLiteralString()
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate JSON expression in %s: %w",
			eval.String(), err)
	}

	// must use the low-level jsonpath setter so that we retain the original object
	if err := expression.SetJSONPathRaw(jp, elem.value, doc); err != nil {
		return nil, fmt.Errorf("failed to set JSONpath %s to value %v: %w", jp, elem.value, err)
	}

	name, err := expression.GetJSONPathRaw("$.metadata.name", doc)
	if err != nil {
		return nil, errors.New("valid .metadata.name required") // can never happen
	}

	// add index to name
	if err := expression.SetJSONPathRaw("$.metadata.name", fmt.Sprintf("%s-%d", name, elem.idx), doc); err != nil {
		return nil, fmt.Errorf("could not add index to .metadata.name")
	}

	return doc, nil
}

func (p *Pipeline) NewUnwindOp(e *expression.Expression) (dbsp.Operator, error) {
	if _, err := e.GetLiteralString(); err != nil {
		return nil, fmt.Errorf("expected a JSONpath expression")
	}
	eval := &UnwindOp{e: e, log: p.log.WithName("@unwind")}
	return dbsp.NewUnwind(eval, eval), nil
}

// Gather operator.
type GatherOp struct {
	e                            *expression.Expression // for the transformer
	keyExtractor, valueExtractor *gatherExtractor
	log                          logr.Logger
}

func (eval *GatherOp) String() string {
	return fmt.Sprintf("gather:key=%s/val=%s", eval.keyExtractor.String(), eval.valueExtractor.String())
}

type gatherExtractor struct {
	e   *expression.Expression
	log logr.Logger
}

func (ext *gatherExtractor) Extract(doc dbsp.Document) (any, error) {
	arg, err := ext.e.Evaluate(expression.EvalCtx{Object: doc, Log: ext.log})
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate pipeline stage %s: %w",
			ext.String(), err)
	}
	return arg, nil
}

func (ext *gatherExtractor) String() string {
	return ext.e.String()
}

func (eval *GatherOp) Transform(doc dbsp.Document, v any) (dbsp.Document, error) {
	// no need to ddepcopy doc: dbsp.UnwindOp does that for us
	aggrData, ok := v.(*dbsp.AggregateInput)
	if !ok {
		return nil, errors.New("expected AggregateInput")
	}

	if err := expression.SetJSONPathRawExp(eval.e, aggrData.Values, doc); err != nil {
		return nil, fmt.Errorf("failed to set elem %s at JSONpath %q: %w", v, eval.e.String(), err)
	}

	return doc, nil
}

func (p *Pipeline) NewGatherOp(e *expression.Expression) (dbsp.Operator, error) {
	args, err := expression.AsExpOrExpList(e)
	if err != nil {
		return nil, err
	}

	if len(args) != 2 {
		return nil, errors.New("expected two expressions")
	}

	eval := &GatherOp{
		e:              &args[1],
		keyExtractor:   &gatherExtractor{e: &args[0], log: p.log},
		valueExtractor: &gatherExtractor{e: &args[1], log: p.log},
		log:            p.log.WithName("@gather"),
	}

	// Return snapshot GatherOp - it will be lifted to I→Gather→D by the rewrite engine.
	return dbsp.NewGather(eval.keyExtractor, eval.valueExtractor, eval), nil
}

// Join operator.
type JoinOp struct {
	e   *expression.Expression
	log logr.Logger
}

func (eval *JoinOp) String() string {
	return fmt.Sprintf("join:%s", trim(eval.e.String()))
}

func (eval *JoinOp) Evaluate(doc dbsp.Document) ([]dbsp.Document, error) {
	res, err := eval.e.Evaluate(expression.EvalCtx{Object: doc, Log: eval.log})
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate join expression %s: %w",
			eval.String(), err)
	}

	arg, err := expression.AsBool(res)
	if err != nil {
		return nil, fmt.Errorf("expected boolean result in %s: %w", eval.String(), err)
	}

	ret := []dbsp.Document{}
	if arg {
		ret = append(ret, doc)
	}

	return ret, nil
}

func (p *Pipeline) NewJoinOp(e *expression.Expression, sources []schema.GroupVersionKind) dbsp.Operator {
	inputs := make([]string, len(sources))
	for i, src := range sources {
		inputs[i] = src.Kind
	}
	eval := &JoinOp{e: e, log: p.log.WithName("@join")}
	return dbsp.NewIncrementalJoin(eval, inputs)
}

func trim(s string) string {
	r := []rune(s)
	if len(r) <= trimEvalLen+3 {
		return s
	}
	return string(r[:trimEvalLen]) + "..."
}

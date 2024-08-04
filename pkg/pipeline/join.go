package pipeline

import (
	"fmt"
	"hsnlab/dcontroller-runtime/pkg/object"

	"github.com/go-logr/logr"
)

// Join is an operation that can be used to perform an inner join on a list of views.
type Join struct {
	Expression
}

// Evaluate processes a join expression. Returns the new state if there were no errors, nil if
// there were no errors but the pipeline execution should stop, and error otherwise.
func (j *Join) Run(view string, views [][]*object.Object, log logr.Logger) ([]*object.Object, error) {
	eng := NewEngine(view, log)
	joins := make([][]ObjectContent, len(views))
	for i := range views {
		joins[i] = make([]ObjectContent, len(views[i]))
		for j := range views[i] {
			joins[i][j] = views[i][j].UnstructuredContent()
		}
	}
	eng.views = joins

	res, err := j.Evaluate(eng)
	if err != nil {
		return nil, NewJoinError(j.String(), fmt.Errorf("join error: %q", err))
	}

	// return an unnormalized object list
	objs, err := asList(res)
	if err != nil {
		return nil, NewInvalidObjectError("join: evaluation result must " +
			"be a list of unstructured objects")
	}

	ret := []*object.Object{}
	for _, res := range objs {
		content, ok := res.(ObjectContent)
		if !ok {
			return nil, NewInvalidObjectError("join: evaluation result must " +
				"be an unstructured object")
		}

		obj := object.New(eng.view)
		obj.SetUnstructuredContent(content)
		ret = append(ret, obj)
	}

	return ret, nil
}

// Evaluate processes an join expression on the given state.
func (a *Join) Evaluate(eng *Engine) (any, error) {
	res, err := a.Expression.Evaluate(eng)
	if err != nil {
		return nil, NewJoinError(a.String(), fmt.Errorf("join error: %q", err))
	}

	return res, nil
}

func (a *Join) String() string {
	return fmt.Sprintf("join:{%s}", a.Expression.String())
}

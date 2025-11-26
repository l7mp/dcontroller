package v1alpha1

import (
	"encoding/json"
	"fmt"

	"github.com/l7mp/dcontroller/pkg/expression"
)

// Pipeline is a sequence of pipeline operations that process objects.
// The first operation may optionally be a @join operation.
// The pipeline can be specified as:
//   - A single operation: pipeline: {"@project": ...}
//   - An array of operations: pipeline: [{"@join": ...}, {"@select": ...}]
//
// +kubebuilder:object:generate=false
type Pipeline struct {
	Ops []PipelineOp
}

// PipelineOp represents a pipeline operation that wraps one or more expressions.
//
// +kubebuilder:object:generate=false
type PipelineOp interface {
	// GetExpression returns the expressions wrapped by this operation.
	GetExpression() *expression.Expression
	// OpType returns the operation type (e.g., "@join", "@select").
	OpType() string
}

// JoinOp represents a @join operation with a join condition expression.
//
// +kubebuilder:object:generate=false
type JoinOp struct {
	Expression expression.Expression
}

func (o *JoinOp) GetExpression() *expression.Expression {
	return &o.Expression
}
func (o *JoinOp) OpType() string { return "@join" }

// SelectionOp represents a @select operation with a filter expression.
//
// +kubebuilder:object:generate=false
type SelectionOp struct {
	Expression expression.Expression
}

func (o *SelectionOp) GetExpression() *expression.Expression {
	return &o.Expression
}
func (o *SelectionOp) OpType() string { return "@select" }

// ProjectionOp represents a @project operation with projection expressions.
//
// +kubebuilder:object:generate=false
type ProjectionOp struct {
	Expression expression.Expression
}

func (o *ProjectionOp) GetExpression() *expression.Expression {
	return &o.Expression
}
func (o *ProjectionOp) OpType() string { return "@project" }

// UnwindOp represents a @unwind or @demux operation.
//
// +kubebuilder:object:generate=false
type UnwindOp struct {
	Expression expression.Expression
}

func (o *UnwindOp) GetExpression() *expression.Expression {
	return &o.Expression
}
func (o *UnwindOp) OpType() string { return "@unwind" }

// GatherOp represents a @gather or @mux operation.
//
// +kubebuilder:object:generate=false
type GatherOp struct {
	Expression expression.Expression
}

func (o *GatherOp) GetExpression() *expression.Expression {
	return &o.Expression
}
func (o *GatherOp) OpType() string { return "@gather" }

// DeepCopyInto is a manual deepcopy implementation for Pipeline since it contains interfaces.
func (in *Pipeline) DeepCopyInto(out *Pipeline) {
	*out = *in
	if in.Ops != nil {
		out.Ops = make([]PipelineOp, len(in.Ops))
		for i := range in.Ops {
			out.Ops[i] = deepCopyPipelineOp(in.Ops[i])
		}
	}
}

// DeepCopy is a manual deepcopy implementation for Pipeline.
func (in *Pipeline) DeepCopy() *Pipeline {
	if in == nil {
		return nil
	}
	out := new(Pipeline)
	in.DeepCopyInto(out)
	return out
}

// deepCopyPipelineOp creates a deep copy of a PipelineOp.
func deepCopyPipelineOp(op PipelineOp) PipelineOp {
	if op == nil {
		return nil
	}

	switch v := op.(type) {
	case *JoinOp:
		return &JoinOp{Expression: v.Expression}
	case *SelectionOp:
		return &SelectionOp{Expression: v.Expression}
	case *ProjectionOp:
		return &ProjectionOp{Expression: v.Expression}
	case *UnwindOp:
		return &UnwindOp{Expression: v.Expression}
	case *GatherOp:
		return &GatherOp{Expression: v.Expression}
	default:
		return nil
	}
}

// UnmarshalJSON implements custom JSON unmarshaling for Pipeline.
// It handles both single operations and arrays of operations.
func (p *Pipeline) UnmarshalJSON(data []byte) error {
	// Try to unmarshal as array first.
	var rawArray []json.RawMessage
	if err := json.Unmarshal(data, &rawArray); err == nil {
		p.Ops = make([]PipelineOp, 0, len(rawArray))
		for _, raw := range rawArray {
			op, err := unmarshalPipelineOp(raw)
			if err != nil {
				return err
			}
			p.Ops = append(p.Ops, op)
		}
		return nil
	}

	// Try to unmarshal as single operation.
	op, err := unmarshalPipelineOp(data)
	if err != nil {
		return err
	}
	p.Ops = []PipelineOp{op}
	return nil
}

// unmarshalPipelineOp unmarshals a single pipeline operation.
func unmarshalPipelineOp(data []byte) (PipelineOp, error) {
	var op map[string]expression.Expression
	if err := json.Unmarshal(data, &op); err != nil {
		return nil, err
	}
	if len(op) != 1 {
		return nil, fmt.Errorf("invalid pipeline op %q: expected a single op", string(data))
	}

	// Create the appropriate PipelineOp based on the op type.
	var opName string
	var expr expression.Expression
	for k, v := range op {
		opName = k
		expr = v
		break
	}

	switch opName {
	case "@join":
		return &JoinOp{Expression: expr}, nil
	case "@select":
		return &SelectionOp{Expression: expr}, nil
	case "@project":
		return &ProjectionOp{Expression: expr}, nil
	case "@unwind", "@demux":
		return &UnwindOp{Expression: expr}, nil
	case "@gather", "@mux":
		return &GatherOp{Expression: expr}, nil
	default:
		return nil, fmt.Errorf("unknown pipeline op %q", string(data))
	}
}

// MarshalJSON implements custom JSON marshaling for Pipeline.
// It marshals as an array if there are multiple operations, otherwise as a single operation.
func (p Pipeline) MarshalJSON() ([]byte, error) {
	if len(p.Ops) == 1 {
		return marshalPipelineOp(p.Ops[0])
	}

	ops := make([]json.RawMessage, len(p.Ops))
	for i, op := range p.Ops {
		data, err := marshalPipelineOp(op)
		if err != nil {
			return nil, err
		}
		ops[i] = data
	}
	return json.Marshal(ops)
}

// marshalPipelineOp marshals a single pipeline operation.
func marshalPipelineOp(op PipelineOp) ([]byte, error) {
	expr := op.GetExpression()
	if expr == nil {
		return nil, fmt.Errorf("invalid pipeline op: %q", op)
	}

	// Wrap the inner expression with the op type.
	wrapper := map[string]*expression.Expression{op.OpType(): expr}
	return json.Marshal(wrapper)
}

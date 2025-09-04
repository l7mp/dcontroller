package dbsp

import (
	"fmt"
)

// Document transformation (current use).
type Evaluator interface {
	Evaluate(Document) ([]Document, error)
	fmt.Stringer
}

// Extract values from documents.
type Extractor interface {
	Extract(Document) (any, error)
	fmt.Stringer
}

// Transform documents by setting/replacing fields.
type Transformer interface {
	Transform(Document, any) (Document, error)
	fmt.Stringer
}

// OperatorType classifies operators for rewrite rules
type OperatorType int

const (
	OpTypeLinear     OperatorType = iota // Op^Δ = Op
	OpTypeBilinear                       // Op^Δ needs expansion (like joins)
	OpTypeNonLinear                      // Op^Δ needs special handling (like distinct)
	OpTypeStructural                     // Graph structure (add, subtract, etc.)
)

// Operator represents a computation node in the graph.
type Operator interface {
	// Process input ZSets and produce output ZSet.
	Process(inputs ...*DocumentZSet) (*DocumentZSet, error)

	// id returns the node name for debugging/rewriting
	id() string

	// Get input arity (number of inputs expected).
	Arity() int

	// OpType returns the type of the operator.
	OpType() OperatorType

	IsTimeInvariant() bool
	HasZeroPreservationProperty() bool
}

// BaseOp is a base operator embedded by all ops.
type BaseOp struct {
	arity int
	name  string
}

// NewBaseOp returns a new base op.
func NewBaseOp(name string, arity int) BaseOp {
	return BaseOp{arity: arity, name: name}
}

func (n *BaseOp) id() string { return n.name } // internal
func (n *BaseOp) Arity() int { return n.arity }

// validateInputs validates inputs in Process methods.
func (n *BaseOp) validateInputs(inputs []*DocumentZSet) error {
	if len(inputs) != n.arity {
		return fmt.Errorf("node %s expects %d inputs, got %d", n.name, n.arity, len(inputs))
	}
	return nil
}

// IncrementalizeOp converts a "snapshot" operator into an incremental operator and returns a
// boolean to signal whether the conversion was successful.
func IncrementalizeOp(in Operator) (Operator, bool) {
	switch op := in.(type) {
	case *ProjectionOp, *SelectionOp, *UnwindOp:
		// These linear ops are stateless so already incremental - no change needed
		return op, false
	case *GatherOp:
		// Gather, although theoretically linear,  needs a specialized incremental version for efficiency
		return NewIncrementalGather(op.keyExtractor, op.valueExtractor, op.aggregator), true
	case *BinaryJoinOp:
		// Join ops need incrementalization
		return NewIncrementalBinaryJoin(op.eval, op.inputs), true
	case *JoinOp:
		return NewIncrementalJoin(op.eval, op.inputs), true
	default:
		return op, false
	}
}

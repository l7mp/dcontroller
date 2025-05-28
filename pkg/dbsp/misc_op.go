package dbsp

import (
	"fmt"
)

// Distinct node
type DistinctOp struct {
	BaseOp
}

func NewDistinct() *DistinctOp {
	return &DistinctOp{
		BaseOp: NewBaseOp("distinct", 1),
	}
}

func (n *DistinctOp) OpType() OperatorType              { return OpTypeNonLinear }
func (n *DistinctOp) IsTimeInvariant() bool             { return true }
func (n *DistinctOp) HasZeroPreservationProperty() bool { return true }

func (n *DistinctOp) Process(inputs ...*DocumentZSet) (*DocumentZSet, error) {
	if err := n.validateInputs(inputs); err != nil {
		return nil, err
	}
	res, err := inputs[0].Distinct()
	if err != nil {
		return nil, err
	}
	return res, nil
}

// IntegratorOp implements the I operator: converts deltas to snapshots
// I(s)[t] = Σ(i=0 to t) s[i]
type IntegratorOp struct {
	BaseOp
	state *DocumentZSet // Accumulated state
}

func NewIntegrator() *IntegratorOp {
	return &IntegratorOp{
		BaseOp: NewBaseOp("I", 1),
		state:  NewDocumentZSet(),
	}
}

func (n *IntegratorOp) Process(inputs ...*DocumentZSet) (*DocumentZSet, error) {
	if err := n.validateInputs(inputs); err != nil {
		return nil, err
	}

	delta := inputs[0]

	// state[t+1] = state[t] + delta[t]
	s, err := n.state.Add(delta)
	if err != nil {
		return nil, err
	}
	n.state = s

	res, err := n.state.DeepCopy()
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (n *IntegratorOp) OpType() OperatorType              { return OpTypeLinear }
func (n *IntegratorOp) IsTimeInvariant() bool             { return true }
func (n *IntegratorOp) HasZeroPreservationProperty() bool { return true }

// Reset state (useful for testing or restarting computation)
func (n *IntegratorOp) Reset() {
	n.state = NewDocumentZSet()
}

// DifferentiatorOp implements the D operator: converts snapshots to deltas
// D(s)[t] = s[t] - s[t-1]
type DifferentiatorOp struct {
	BaseOp
	prevState *DocumentZSet // Previous snapshot
}

func NewDifferentiator() *DifferentiatorOp {
	return &DifferentiatorOp{
		BaseOp:    NewBaseOp("D", 1),
		prevState: NewDocumentZSet(),
	}
}

func (n *DifferentiatorOp) Process(inputs ...*DocumentZSet) (*DocumentZSet, error) {
	if err := n.validateInputs(inputs); err != nil {
		return nil, err
	}

	snapshot := inputs[0]

	// delta[t] = snapshot[t] - snapshot[t-1]
	delta, err := snapshot.Subtract(n.prevState)
	if err != nil {
		return nil, err
	}

	// Update state for next call
	p, err := snapshot.DeepCopy()
	if err != nil {
		return nil, err
	}
	n.prevState = p

	return delta, nil
}

func (n *DifferentiatorOp) OpType() OperatorType              { return OpTypeLinear }
func (n *DifferentiatorOp) IsTimeInvariant() bool             { return true }
func (n *DifferentiatorOp) HasZeroPreservationProperty() bool { return true }

// Reset state (useful for testing or restarting computation)
func (n *DifferentiatorOp) Reset() {
	n.prevState = NewDocumentZSet()
}

// Input node (source of data)
type InputOp struct {
	BaseOp
	data *DocumentZSet
}

func NewInput(name string) *InputOp {
	return &InputOp{
		BaseOp: NewBaseOp("input:"+name, 0),
		data:   NewDocumentZSet(),
	}
}

func (n *InputOp) OpType() OperatorType              { return OpTypeLinear }
func (n *InputOp) IsTimeInvariant() bool             { return true }
func (n *InputOp) HasZeroPreservationProperty() bool { return true }

func (n *InputOp) Process(inputs ...*DocumentZSet) (*DocumentZSet, error) {
	if err := n.validateInputs(inputs); err != nil {
		return nil, err
	}
	res, err := n.data.DeepCopy()
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (n *InputOp) SetData(data *DocumentZSet) {
	n.data = data
}

// Constant node
type ConstantOp struct {
	BaseOp
	value *DocumentZSet
}

func NewConstant(value *DocumentZSet, name string) *ConstantOp {
	return &ConstantOp{
		BaseOp: NewBaseOp("const:"+name, 0),
		value:  value,
	}
}

func (n *ConstantOp) Process(inputs ...*DocumentZSet) (*DocumentZSet, error) {
	if err := n.validateInputs(inputs); err != nil {
		return nil, err
	}
	res, err := n.value.DeepCopy()
	if err != nil {
		return nil, err
	}
	return res, nil
}

// FusedOp combines multiple nodes for optimization
type FusedOp struct {
	BaseOp
	nodes []Operator
}

func NewFusedOp(nodes []Operator, name string) (*FusedOp, error) {
	if len(nodes) == 0 {
		return nil, fmt.Errorf("cannot create empty fused node")
	}

	return &FusedOp{
		BaseOp: NewBaseOp("fused:"+name, nodes[0].Arity()),
		nodes:  nodes,
	}, nil
}

func (n *FusedOp) Process(inputs ...*DocumentZSet) (*DocumentZSet, error) {
	if err := n.validateInputs(inputs); err != nil {
		return nil, err
	}

	// Chain execution through all nodes, propagating errors
	result := inputs[0]
	for i, node := range n.nodes {
		var err error
		result, err = node.Process(result)
		if err != nil {
			return NewDocumentZSet(), fmt.Errorf("error in fused node %s at step %d (%s): %w",
				n.Name(), i, node.Name(), err)
		}
	}

	return result, nil
}

func (n *FusedOp) OpType() OperatorType              { return OpTypeLinear }
func (n *FusedOp) IsTimeInvariant() bool             { return true }
func (n *FusedOp) HasZeroPreservationProperty() bool { return true }

// Helper function for safe fusion
func FuseFilterProject(filter *SelectionOp, project *ProjectionOp) (Operator, error) {
	return NewFusedOp(
		[]Operator{filter, project},
		fmt.Sprintf("%s→%s", filter.Name(), project.Name()),
	)
}

// Addition node
type AddOp struct {
	BaseOp
}

func NewAdd() *AddOp {
	return &AddOp{
		BaseOp: NewBaseOp("+", 2),
	}
}

func (n *AddOp) Process(inputs ...*DocumentZSet) (*DocumentZSet, error) {
	if err := n.validateInputs(inputs); err != nil {
		return nil, err
	}
	res, err := inputs[0].Add(inputs[1])
	if err != nil {
		return nil, err
	}
	return res, nil
}

// Subtraction node
type SubtractOp struct {
	BaseOp
}

func NewSubtract() *SubtractOp {
	return &SubtractOp{
		BaseOp: NewBaseOp("-", 2),
	}
}

func (n *SubtractOp) Process(inputs ...*DocumentZSet) (*DocumentZSet, error) {
	if err := n.validateInputs(inputs); err != nil {
		return nil, err
	}
	res, err := inputs[0].Subtract(inputs[1])
	if err != nil {
		return nil, err
	}
	return res, nil
}

// Negation node
type NegateOp struct {
	BaseOp
}

func NewNegate() *NegateOp {
	return &NegateOp{
		BaseOp: NewBaseOp("neg", 1),
	}
}

func (n *NegateOp) Process(inputs ...*DocumentZSet) (*DocumentZSet, error) {
	if err := n.validateInputs(inputs); err != nil {
		return nil, err
	}
	res, err := NewDocumentZSet().Subtract(inputs[0])
	if err != nil {
		return nil, err
	}
	return res, nil
}

// DelayOp implements the z^(-1) operator: delays stream by one timestep
// PROBLEMATIC: This is stateful and needs careful handling in the execution model
type DelayOp struct {
	BaseOp
	buffer *DocumentZSet // Buffered value from previous timestep
}

func NewDelay() *DelayOp {
	return &DelayOp{
		BaseOp: NewBaseOp("z^(-1)", 1),
		buffer: NewDocumentZSet(), // Initial value is zero
	}
}

func (n *DelayOp) Process(inputs ...*DocumentZSet) (*DocumentZSet, error) {
	if err := n.validateInputs(inputs); err != nil {
		return nil, err
	}

	input := inputs[0]

	// Output the buffered value from previous timestep
	output, err := n.buffer.DeepCopy()
	if err != nil {
		return nil, err
	}

	// Buffer current input for next timestep
	n.buffer, err = input.DeepCopy()
	if err != nil {
		return nil, err
	}

	return output, nil
}

func (n *DelayOp) OpType() OperatorType              { return OpTypeLinear }
func (n *DelayOp) IsTimeInvariant() bool             { return true }
func (n *DelayOp) HasZeroPreservationProperty() bool { return true }

func (n *DelayOp) Reset() {
	n.buffer = NewDocumentZSet()
}

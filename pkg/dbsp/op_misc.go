package dbsp

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

	return n.state.DeepCopy(), nil
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
	n.prevState = snapshot.DeepCopy()

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
	name string
}

func NewInput(name string) *InputOp {
	return &InputOp{
		BaseOp: NewBaseOp("input:"+name, 0),
		data:   NewDocumentZSet(),
		name:   name,
	}
}

func (n *InputOp) OpType() OperatorType              { return OpTypeLinear }
func (n *InputOp) IsTimeInvariant() bool             { return true }
func (n *InputOp) HasZeroPreservationProperty() bool { return true }

func (n *InputOp) Process(inputs ...*DocumentZSet) (*DocumentZSet, error) {
	if err := n.validateInputs(inputs); err != nil {
		return nil, err
	}
	return n.data.DeepCopy(), nil
}

func (n *InputOp) SetData(data *DocumentZSet) { n.data = data }
func (n *InputOp) Name() string               { return n.name }

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
	return n.value.DeepCopy(), nil
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
	output := n.buffer.DeepCopy()

	// Buffer current input for next timestep
	n.buffer = input.DeepCopy()

	return output, nil
}

func (n *DelayOp) OpType() OperatorType              { return OpTypeLinear }
func (n *DelayOp) IsTimeInvariant() bool             { return true }
func (n *DelayOp) HasZeroPreservationProperty() bool { return true }

func (n *DelayOp) Reset() {
	n.buffer = NewDocumentZSet()
}

package pipeline

// import (
// 	"reflect"
// )

// var (
// 	int64Type   = reflect.TypeOf(int64(0))
// 	float64Type = reflect.TypeOf(float64(0.0))
// )

// type Condition interface {
// 	Evaluate(state State) (bool, error)
// }

// type AndCondition struct {
// 	And []Condition `json:"@and"`
// }

// func (c AndCondition) Evaluate(state State) (bool, error) {
// 	for _, cond := range c.And {
// 		res, err := cond.Evaluate(state)
// 		if err != nil {
// 			return false, err
// 		}
// 		if !res {
// 			return false, nil
// 		}
// 	}
// 	return true, nil
// }

// type OrCondition struct {
// 	Or []Condition `json:"@or"`
// }

// func (c OrCondition) Evaluate(state State) (bool, error) {
// 	for _, cond := range c.Or {
// 		res, err := cond.Evaluate(state)
// 		if err != nil {
// 			return false, err
// 		}
// 		if res {
// 			return true, nil
// 		}
// 	}
// 	return false, nil
// }

// type EqCondition struct {
// 	Eq []Expression `json:"@eq"`
// }

// func (c EqCondition) Evaluate(state State) (bool, error) {
// 	return arithmeticRelEval(state, c.Eq, func(a, b float64) (bool, error) {
// 		return a == b, nil
// 	})
// }

// type GtCondition struct {
// 	Gt []Expression `json:"@gt"`
// }

// func (c GtCondition) Evaluate(state State) (bool, error) {
// 	return arithmeticRelEval(state, c.Gt, func(a, b float64) (bool, error) {
// 		return a > b, nil
// 	})
// }

// type LtCondition struct {
// 	Lt []Expression `json:"@lt"`
// }

// func (c LtCondition) Evaluate(state State) (bool, error) {
// 	return arithmeticRelEval(state, c.Lt, func(a, b float64) (bool, error) {
// 		return a > b, nil
// 	})
// }

// type GtECondition struct {
// 	Gte []Expression `json:"@gte"`
// }

// func (c GtECondition) Evaluate(state State) (bool, error) {
// 	return arithmeticRelEval(state, c.Gte, func(a, b float64) (bool, error) {
// 		return a >= b, nil
// 	})
// }

// type LteCondition struct {
// 	Lte []Expression `json:"@lt"`
// }

// func (c LteCondition) Evaluate(state State) (bool, error) {
// 	return arithmeticRelEval(state, c.Lte, func(a, b float64) (bool, error) {
// 		return a <= b, nil
// 	})
// }

// func arithmeticRelEval(state State, args []Expression, f func(a, b float64) (bool, error)) (bool, error) {
// 	if len(args) != 2 {
// 		return false, NewInvalidArgumentsError()
// 	}
// 	arg1, err := args[0].Evaluate(state)
// 	if err != nil {
// 		return false, nil
// 	}
// 	arg2, err := args[1].Evaluate(state)
// 	if err != nil {
// 		return false, nil
// 	}

// 	// both convert to float64
// 	if reflect.ValueOf(arg1).CanConvert(float64Type) && reflect.ValueOf(arg2).CanConvert(float64Type) {
// 		return f(reflect.ValueOf(arg1).Convert(float64Type).Float(), reflect.ValueOf(arg2).Convert(float64Type).Float())
// 	}

// 	return false, ErrInvalidArguments
// }

package pipeline

type Aggregation interface {
	Evaluate(state State) any
}

// type Map map[string]any

// type Filter struct {
// 	Condition Condition
// }

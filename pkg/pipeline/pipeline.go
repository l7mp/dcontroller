package pipeline

// Pipeline is an optional join operator followed by a nonempty list of aggregation operators.
type Pipeline struct {
	// Join        Join
	Aggregation []Aggregation
}

// type Join interface {
// 	Join        Join
// 	Aggregation []Aggregation
// }

// func NewEmptyPipeline() *Pipeline { return &Pipeline{} }

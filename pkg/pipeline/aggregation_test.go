package pipeline

// import (
// 	. "github.com/onsi/ginkgo/v2"
// 	. "github.com/onsi/gomega"

// 	"hsnlab/dcontroller-runtime/pkg/object"
// )

// var _ = Describe("Aggregations", func() {
// 	var state = &State{
// 		Object: object.New("view").WithName("default", "name").
// 			WithContent(map[string]any{"spec": map[string]any{"a": 1, "b": map[string]any{"c": 2}}}),
// 		Log: logger,
// 	}

// Describe("Evaluating filter aggregations", func() {
// 	It("should deserialize and evaluate a bool literal expression", func() {
// 		jsonData := "true"
// 		var exp Expression
// 		err := json.Unmarshal([]byte(jsonData), &exp)
// 		Expect(err).NotTo(HaveOccurred())
// 		Expect(exp).To(Equal(Expression{Op: "@bool", Literal: true, Raw: jsonData}))

// 		res, err := exp.Evaluate(state)
// 		Expect(err).NotTo(HaveOccurred())
// 		Expect(reflect.ValueOf(res).Kind()).To(Equal(reflect.Bool))
// 		Expect(reflect.ValueOf(res).Bool()).To(Equal(true))
// 	})
// })
// })

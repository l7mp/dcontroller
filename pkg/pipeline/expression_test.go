package pipeline

import (
	"encoding/json"
	"reflect"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"hsnlab/dcontroller-runtime/pkg/object"
)

var _ = Describe("Expressions", func() {
	var state = State{Object: object.New("view").WithName("default", "name").
		WithContent(map[string]any{"a": 1, "b": map[string]any{"c": 2}})}

	Describe("Evaluating terminal expressions", func() {
		It("should deserialize and evaluate a bool literal expression", func() {
			jsonData := "true"
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			Expect(exp).To(Equal(Expression{Op: "@bool", Literal: true, Raw: jsonData}))

			res, err := exp.Evaluate(state)
			Expect(err).NotTo(HaveOccurred())
			Expect(reflect.ValueOf(res).Kind()).To(Equal(reflect.Bool))
			Expect(reflect.ValueOf(res).Bool()).To(Equal(true))
		})

		It("should deserialize and evaluate an integer literal expression", func() {
			jsonData := "10"
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			Expect(exp).To(Equal(Expression{Op: "@int", Literal: 10, Raw: jsonData}))

			res, err := exp.Evaluate(state)
			Expect(err).NotTo(HaveOccurred())
			Expect(reflect.ValueOf(res).Kind()).To(Equal(reflect.Int64))
			Expect(reflect.ValueOf(res).Int()).To(Equal(int64(10)))
		})

		It("should deserialize and evaluate a float literal expression", func() {
			jsonData := "10.12"
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			Expect(exp).To(Equal(Expression{Op: "@float", Literal: 10.12, Raw: jsonData}))

			res, err := exp.Evaluate(state)
			Expect(err).NotTo(HaveOccurred())
			Expect(reflect.ValueOf(res).Kind()).To(Equal(reflect.Float64))
			Expect(reflect.ValueOf(res).Float()).To(Equal(10.12))
		})

		It("should deserialize and evaluate a string literal expression", func() {
			jsonData := `"a10"`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			Expect(exp).To(Equal(Expression{Op: "@string", Literal: "a10", Raw: jsonData}))

			res, err := exp.Evaluate(state)
			Expect(err).NotTo(HaveOccurred())
			Expect(reflect.ValueOf(res).Kind()).To(Equal(reflect.String))
			Expect(reflect.ValueOf(res).String()).To(Equal("a10"))
		})

		It("should deserialize and evaluate a compound literal expression", func() {
			jsonData := `{"@eq": [10, 10]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			Expect(exp).To(Equal(Expression{Op: "@eq",
				Args: []Expression{{
					Op: "@int", Literal: 10, Raw: "10",
				}, {
					Op: "@int", Literal: 10, Raw: "10",
				}},
				Raw: jsonData}))

			res, err := exp.Evaluate(state)
			Expect(err).NotTo(HaveOccurred())
			Expect(reflect.ValueOf(res).Kind()).To(Equal(reflect.Bool))
			Expect(reflect.ValueOf(res).Bool()).To(Equal(true))
		})

		It("should deserialize and evaluate a multi-level compound literal expression", func() {
			jsonData := `{"@and": [{"@eq": [10, 10]}, {"@lt": [1, 2]}]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			Expect(exp).To(Equal(Expression{Op: "@and",
				Args: []Expression{{
					Op: "@eq",
					Args: []Expression{{
						Op: "@int", Literal: 10, Raw: "10",
					}, {
						Op: "@int", Literal: 10, Raw: "10",
					}},
					Raw: `{"@eq": [10, 10]}`,
				}, {
					Op: "@lt",
					Args: []Expression{{
						Op: "@int", Literal: 1, Raw: "1",
					}, {
						Op: "@int", Literal: 2, Raw: "2",
					}},
					Raw: `{"@lt": [1, 2]}`,
				}},
				Raw: jsonData,
			}))

			res, err := exp.Evaluate(state)
			Expect(err).NotTo(HaveOccurred())
			Expect(reflect.ValueOf(res).Kind()).To(Equal(reflect.Bool))
			Expect(reflect.ValueOf(res).Bool()).To(Equal(true))
		})

		It("should deserialize and evaluate a compound bool expression", func() {
			jsonData := `{"@bool": [{"@eq": [10, 10]}]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			Expect(exp).To(Equal(Expression{
				Op: "@bool",
				Args: []Expression{{
					Op: "@eq",
					Args: []Expression{{
						Op: "@int", Literal: 10, Raw: "10",
					}, {
						Op: "@int", Literal: 10, Raw: "10",
					}},
					Raw: `{"@eq": [10, 10]}`,
				}},
				Raw: jsonData,
			}))

			res, err := exp.Evaluate(state)
			Expect(err).NotTo(HaveOccurred())
			Expect(reflect.ValueOf(res).Kind()).To(Equal(reflect.Bool))
			Expect(reflect.ValueOf(res).Bool()).To(Equal(true))
		})

		It("should deserialize and evaluate a compound bool expression", func() {
			jsonData := `{"@bool": [true]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			Expect(exp).To(Equal(Expression{
				Op:   "@bool",
				Args: []Expression{{Op: "@bool", Literal: true, Raw: "true"}},
				Raw:  jsonData,
			}))

			res, err := exp.Evaluate(state)
			Expect(err).NotTo(HaveOccurred())
			Expect(reflect.ValueOf(res).Kind()).To(Equal(reflect.Bool))
			Expect(reflect.ValueOf(res).Bool()).To(Equal(true))
		})

	})
})

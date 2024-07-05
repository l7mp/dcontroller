package pipeline

import (
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"hsnlab/dcontroller-runtime/pkg/object"
)

var _ = Describe("Aggregations", func() {
	var state = &State{
		View: "view",
		Object: object.New("view").WithName("default", "name").
			WithContent(map[string]any{"spec": map[string]any{"a": int64(1), "b": map[string]any{"c": int64(2)}}}),
		Log: logger,
	}

	Describe("Evaluating filter aggregations", func() {
		It("should evaluate true filter expression", func() {
			jsonData := `{"@filter":{"@eq":["$.metadata.name","name"]}}`
			var ag Aggregation
			err := json.Unmarshal([]byte(jsonData), &ag)
			Expect(err).NotTo(HaveOccurred())
			Expect(ag).To(Equal(Aggregation{
				Filter: &Filter{
					Condition: Expression{
						Op: "@eq",
						Arg: &Expression{
							Op: "@list",
							Literal: []Expression{
								{
									Op:      "@string",
									Literal: "$.metadata.name",
									Raw:     "\"$.metadata.name\"",
								},
								{
									Op:      "@string",
									Literal: "name",
									Raw:     "\"name\"",
								},
							},
							Raw: "[\"$.metadata.name\",\"name\"]",
						},
						Raw: "{\"@eq\":[\"$.metadata.name\",\"name\"]}",
					},
				},
				Project: nil,
				Map:     nil,
			}))

			s, err := ag.Evaluate(state)
			Expect(err).NotTo(HaveOccurred())
			Expect(s.Object).To(Equal(state.Object))
		})

		It("should evaluate false filter expression", func() {
			jsonData := `{"@filter":{"@eq":["$.spec.b.c",1]}}`
			var ag Aggregation
			err := json.Unmarshal([]byte(jsonData), &ag)
			Expect(err).NotTo(HaveOccurred())
			Expect(ag.Filter).NotTo(BeNil())
			Expect(ag.Project).To(BeNil())
			Expect(ag.Map).To(BeNil())

			s, err := ag.Evaluate(state)
			Expect(err).NotTo(HaveOccurred())
			Expect(s).To(BeNil()) // nil means to block
		})
	})

	Describe("Evaluating projection aggregations", func() {
		It("should evaluate a valid projection expression", func() {
			jsonData := `{"@project":{"metadata":{"name":"$.metadata.name"}}}`
			var ag Aggregation
			err := json.Unmarshal([]byte(jsonData), &ag)
			Expect(err).NotTo(HaveOccurred())
			Expect(ag).To(Equal(Aggregation{
				Filter: nil,
				Map:    nil,
				Project: &Project{
					Projector: Expression{
						Op: "@dict",
						Literal: map[string]Expression{
							"metadata": {
								Op: "@dict",
								Literal: map[string]Expression{
									"name": {
										Op:      "@string",
										Literal: "$.metadata.name",
										Raw:     "\"$.metadata.name\"",
									},
								},
								Raw: "{\"name\":\"$.metadata.name\"}",
							},
						},
						Raw: "{\"metadata\":{\"name\":\"$.metadata.name\"}}",
					},
				},
			}))
			s, err := ag.Evaluate(state)

			Expect(err).NotTo(HaveOccurred())
			Expect(s.Object).To(Equal(&object.Object{
				Unstructured: unstructured.Unstructured{
					Object: map[string]any{
						"apiVersion": "dcontroller.github.io/v1alpha1",
						"kind":       "view",
						"metadata": map[string]any{
							"name":      "name",
							"namespace": "",
						},
					},
				},
				View: "view",
			}))
		})

		// It("should evaluate a valid projection expression", func() {
		// 	jsonData := `{"@project":{"@eq":["$.metadata.name","name"]}}`
		// 	var ag Aggregation
		// 	err := json.Unmarshal([]byte(jsonData), &ag)
		// 	Expect(err).NotTo(HaveOccurred())
		// 	s, err := ag.Evaluate(state)
		// 	Expect(err).NotTo(HaveOccurred())
		// 	Expect(s.Object).To(Equal(state.Object))
		// })
	})
})

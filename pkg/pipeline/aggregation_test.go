package pipeline

import (
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"hsnlab/dcontroller-runtime/pkg/object"
)

var _ = Describe("Aggregations", func() {
	var objs []*object.Object

	BeforeEach(func() {
		objs = []*object.Object{
			object.New("view").WithName("default", "name").
				WithContent(ObjectContent{
					"spec": ObjectContent{
						"a": int64(1),
						"b": ObjectContent{"c": int64(2)},
					},
					"c": "c",
				}),
			object.New("view").WithName("default", "name2").
				WithContent(ObjectContent{
					"spec": ObjectContent{
						"a": int64(2),
						"b": ObjectContent{"c": int64(3)},
					},
					"d": "d",
				}),
		}
	})

	Describe("Evaluating filter aggregations", func() {
		It("should evaluate true filter expression", func() {
			jsonData := `{"@filter":{"@eq":["$.metadata.name","name"]}}`
			var ag Aggregation
			err := json.Unmarshal([]byte(jsonData), &ag)
			Expect(err).NotTo(HaveOccurred())

			res, err := ag.Run("view", objs, logger)
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(1))
			Expect(res[0]).To(Equal(objs[0]))
		})

		It("should evaluate a false filter expression", func() {
			jsonData := `{"@filter":{"@eq":["$.spec.b.c",1]}}`
			var ag Aggregation
			err := json.Unmarshal([]byte(jsonData), &ag)
			Expect(err).NotTo(HaveOccurred())

			res, err := ag.Run("view", objs, logger)
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(0))
		})

		It("should evaluate an inverted false filter expression", func() {
			jsonData := `{"@filter":{"@not":{"@eq":["$.spec.b.c",1]}}}`
			var ag Aggregation
			err := json.Unmarshal([]byte(jsonData), &ag)
			Expect(err).NotTo(HaveOccurred())

			res, err := ag.Run("view", objs, logger)
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(2))
			Expect(res).To(Equal(objs))
		})

		It("should err for a filter expression referring to a nonexistent field", func() {
			jsonData := `{"@filter":{"@eq":["$.spec.x",true]}}`
			var ag Aggregation
			err := json.Unmarshal([]byte(jsonData), &ag)
			Expect(err).NotTo(HaveOccurred())

			_, err = ag.Run("view", objs, logger)
			Expect(err).To(HaveOccurred())
		})

		// Describe("Evaluating projection aggregations", func() {
		// 	It("should evaluate a simple projection expression", func() {
		// 		jsonData := `{"@project":{"metadata":{"name":"$.metadata.name"}}}`
		// 		var ag Aggregation
		// 		err := json.Unmarshal([]byte(jsonData), &ag)
		// 		Expect(err).NotTo(HaveOccurred())
		// 		Expect(ag).To(Equal(Aggregation{
		// 			Filter: nil,
		// 			Map:    nil,
		// 			Set: &Set{
		// 				Setters: Expression{
		// 					Op: "@dict",
		// 					Literal: map[string]Expression{
		// 						"metadata": {
		// 							Op: "@dict",
		// 							Literal: map[string]Expression{
		// 								"name": {
		// 									Op:      "@string",
		// 									Literal: "$.metadata.name",
		// 									Raw:     "\"$.metadata.name\"",
		// 								},
		// 							},
		// 							Raw: "{\"name\":\"$.metadata.name\"}",
		// 						},
		// 					},
		// 					Raw: "{\"metadata\":{\"name\":\"$.metadata.name\"}}",
		// 				},
		// 			},
		// 		}))
		// 		s, err := ag.Evaluate(state)

		// 		Expect(err).NotTo(HaveOccurred())
		// 		Expect(s.Object).To(Equal(&object.Object{
		// 			Unstructured: unstructured.Unstructured{
		// 				Object: ObjectContent{
		// 					"apiVersion": "dcontroller.github.io/v1alpha1",
		// 					"kind":       "view",
		// 					"metadata": ObjectContent{
		// 						"name":      "name",
		// 						"namespace": "",
		// 					},
		// 				},
		// 			},
		// 			View: "view",
		// 		}))
		// 	})

		// 	It("should evaluate a projection expression with multiple fields", func() {
		// 		jsonData := `{"@project":{"metadata":{"name":"$.metadata.name","namespace":"$.metadata.namespace"}}}`
		// 		var ag Aggregation
		// 		err := json.Unmarshal([]byte(jsonData), &ag)
		// 		Expect(err).NotTo(HaveOccurred())
		// 		s, err := ag.Evaluate(state)
		// 		Expect(err).NotTo(HaveOccurred())
		// 		raw, ok := s.Object.Object["metadata"]
		// 		Expect(ok).To(BeTrue())
		// 		meta, ok := raw.(ObjectContent)
		// 		Expect(ok).To(BeTrue())
		// 		Expect(meta["namespace"]).To(Equal("default"))
		// 		Expect(meta["name"]).To(Equal("name"))
		// 	})

		// 	It("should evaluate a projection expression that copies a subtree", func() {
		// 		jsonData := `{"@project":{"metadata":"$.metadata"}}`
		// 		var ag Aggregation
		// 		err := json.Unmarshal([]byte(jsonData), &ag)
		// 		Expect(err).NotTo(HaveOccurred())
		// 		s, err := ag.Evaluate(state)
		// 		Expect(err).NotTo(HaveOccurred())
		// 		raw, ok := s.Object.Object["metadata"]
		// 		Expect(ok).To(BeTrue())
		// 		meta, ok := raw.(ObjectContent)
		// 		Expect(ok).To(BeTrue())
		// 		Expect(meta["namespace"]).To(Equal("default"))
		// 		Expect(meta["name"]).To(Equal("name"))
		// 	})

		// 	It("should err for a projection that drops .metadata.name", func() {
		// 		jsonData := `{"@project":{"spec":"$.spec"}}`
		// 		var ag Aggregation
		// 		err := json.Unmarshal([]byte(jsonData), &ag)
		// 		Expect(err).NotTo(HaveOccurred())
		// 		_, err = ag.Evaluate(state)
		// 		Expect(err).To(HaveOccurred())
		// 	})

		// 	It("should err for a projection that asks for a non-existent field", func() {
		// 		jsonData := `{"@project":{"x": "$.spec.x"}}`
		// 		var ag Aggregation
		// 		err := json.Unmarshal([]byte(jsonData), &ag)
		// 		Expect(err).NotTo(HaveOccurred())
		// 		_, err = ag.Evaluate(state)
		// 		Expect(err).To(HaveOccurred())
		// 	})
	})
})

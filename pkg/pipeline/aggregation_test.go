package pipeline

import (
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"hsnlab/dcontroller-runtime/pkg/cache"
	"hsnlab/dcontroller-runtime/pkg/object"
)

var _ = Describe("Aggregations", func() {
	var objs []*object.Object
	var eng Engine

	BeforeEach(func() {
		objs = []*object.Object{
			object.New("view").WithName("default", "name").
				WithContent(Unstructured{
					"spec": Unstructured{
						"a": int64(1),
						"b": Unstructured{"c": int64(2)},
					},
					"c": "c",
				}),
			object.New("view").WithName("default", "name2").
				WithContent(Unstructured{
					"spec": Unstructured{
						"a": int64(2),
						"b": Unstructured{"c": int64(3)},
					},
					"d": "d",
				}),
		}
		eng = NewDefaultEngine("view", emptyView, logger)
	})

	Describe("Evaluating select aggregations", func() {
		It("should evaluate true select expression", func() {
			jsonData := `{"@aggregate":[{"@select":{"@eq":["$.metadata.name","name"]}}]}`
			var ag Aggregation
			err := json.Unmarshal([]byte(jsonData), &ag)
			Expect(err).NotTo(HaveOccurred())

			res, err := ag.Evaluate(eng, cache.Delta{Type: cache.Upserted, Object: objs[0]})
			Expect(err).NotTo(HaveOccurred())
			Expect(res.IsUnchanged()).To(BeFalse())
			Expect(res).To(Equal(cache.Delta{Type: cache.Added, Object: objs[0]}))

			res, err = ag.Evaluate(eng, cache.Delta{Type: cache.Added, Object: objs[1]})
			Expect(err).NotTo(HaveOccurred())
			Expect(res.IsUnchanged()).To(BeTrue())

			res, err = ag.Evaluate(eng, cache.Delta{Type: cache.Upserted, Object: objs[0]})
			Expect(err).NotTo(HaveOccurred())
			Expect(res.Type).To(Equal(cache.Updated))
			Expect(res.IsUnchanged()).To(BeFalse())
		})

		It("should evaluate a false select expression", func() {
			jsonData := `{"@aggregate":[{"@select":{"@eq":["$.spec.b.c",1]}}]}`
			var ag Aggregation
			err := json.Unmarshal([]byte(jsonData), &ag)
			Expect(err).NotTo(HaveOccurred())

			res, err := ag.Evaluate(eng, cache.Delta{Type: cache.Upserted, Object: objs[0]})
			Expect(err).NotTo(HaveOccurred())
			Expect(res.IsUnchanged()).To(BeTrue())

			res, err = ag.Evaluate(eng, cache.Delta{Type: cache.Upserted, Object: objs[1]})
			Expect(err).NotTo(HaveOccurred())
			Expect(res.IsUnchanged()).To(BeTrue())
		})

		It("should evaluate an inverted false select expression", func() {
			jsonData := `{"@aggregate":[{"@select":{"@not":{"@eq":["$.spec.b.c",1]}}}]}`
			var ag Aggregation
			err := json.Unmarshal([]byte(jsonData), &ag)
			Expect(err).NotTo(HaveOccurred())

			res, err := ag.Evaluate(eng, cache.Delta{Type: cache.Upserted, Object: objs[0]})
			Expect(err).NotTo(HaveOccurred())
			Expect(res.IsUnchanged()).To(BeFalse())
			Expect(res).To(Equal(cache.Delta{Type: cache.Added, Object: objs[0]}))

			res, err = ag.Evaluate(eng, cache.Delta{Type: cache.Upserted, Object: objs[1]})
			Expect(err).NotTo(HaveOccurred())
			Expect(res.IsUnchanged()).To(BeFalse())
			Expect(res).To(Equal(cache.Delta{Type: cache.Added, Object: objs[1]}))
		})

		It("should not err for a select expression referring to a nonexistent field", func() {
			jsonData := `{"@aggregate":[{"@select":{"@eq":["$.spec.x",true]}}]}`
			var ag Aggregation
			err := json.Unmarshal([]byte(jsonData), &ag)
			Expect(err).NotTo(HaveOccurred())

			res, err := ag.Evaluate(eng, cache.Delta{Type: cache.Upserted, Object: objs[0]})
			Expect(err).NotTo(HaveOccurred())
			Expect(res.IsUnchanged()).To(BeTrue())
		})
	})

	Describe("Evaluating projection aggregations", func() {
		It("should evaluate a simple projection expression", func() {
			jsonData := `{"@aggregate":[{"@project":{"metadata":{"name":"$.metadata.name"}}}]}`
			var ag Aggregation
			err := json.Unmarshal([]byte(jsonData), &ag)
			Expect(err).NotTo(HaveOccurred())
			Expect(ag.Expressions).To(HaveLen(1))
			Expect(ag.Expressions[0]).To(Equal(Expression{
				Op: "@project",
				Arg: &Expression{
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
				Raw: ag.Expressions[0].Raw,
			}))

			res, err := ag.Evaluate(eng, cache.Delta{Type: cache.Updated, Object: objs[0]})
			Expect(err).NotTo(HaveOccurred())

			Expect(res.Type).To(Equal(cache.Updated))
			Expect(res.Object).To(Equal(&object.Object{
				Unstructured: unstructured.Unstructured{
					Object: Unstructured{
						"apiVersion": "dcontroller.github.io/v1alpha1",
						"kind":       "view",
						"metadata": Unstructured{
							"name": "name",
						},
					},
				},
				View: "view",
			}))
		})

		It("should evaluate a projection expression with multiple fields", func() {
			jsonData := `{"@aggregate":[{"@project":{"metadata":{"name":"$.metadata.name","namespace":"$.metadata.namespace"}}}]}`
			var ag Aggregation
			err := json.Unmarshal([]byte(jsonData), &ag)
			Expect(err).NotTo(HaveOccurred())

			res, err := ag.Evaluate(eng, cache.Delta{Type: cache.Updated, Object: objs[0]})
			Expect(err).NotTo(HaveOccurred())

			Expect(res.Type).To(Equal(cache.Updated))
			raw, ok := res.Object.Object["metadata"]
			Expect(ok).To(BeTrue())
			meta, ok := raw.(Unstructured)
			Expect(ok).To(BeTrue())
			Expect(meta["namespace"]).To(Equal("default"))
			Expect(meta["name"]).To(Equal("name"))
		})

		It("should evaluate a projection expression that copies a subtree", func() {
			jsonData := `{"@aggregate":[{"@project":{"metadata":"$.metadata"}}]}`
			var ag Aggregation
			err := json.Unmarshal([]byte(jsonData), &ag)
			Expect(err).NotTo(HaveOccurred())

			res, err := ag.Evaluate(eng, cache.Delta{Type: cache.Updated, Object: objs[0]})
			Expect(err).NotTo(HaveOccurred())

			raw, ok := res.Object.Object["metadata"]
			Expect(ok).To(BeTrue())
			meta, ok := raw.(Unstructured)
			Expect(ok).To(BeTrue())
			Expect(meta["namespace"]).To(Equal("default"))
			Expect(meta["name"]).To(Equal("name"))
		})

		It("should err for a projection that drops .metadata.name", func() {
			jsonData := `{"@aggregate":[{"@project":{"spec":"$.spec"}}]}`
			var ag Aggregation
			err := json.Unmarshal([]byte(jsonData), &ag)

			_, err = ag.Evaluate(eng, cache.Delta{Type: cache.Updated, Object: objs[0]})
			Expect(err).To(HaveOccurred())
		})

		It("should err for a projection that asks for a non-existent field", func() {
			jsonData := `{"@aggregate":[{"@project":{"x": "$.spec.x"}}]}`
			var ag Aggregation
			err := json.Unmarshal([]byte(jsonData), &ag)
			Expect(err).NotTo(HaveOccurred())

			_, err = ag.Evaluate(eng, cache.Delta{Type: cache.Updated, Object: objs[0]})
			Expect(err).To(HaveOccurred())
		})
	})
})

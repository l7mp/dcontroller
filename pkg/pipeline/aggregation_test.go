package pipeline

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/yaml"

	opv1a1 "hsnlab/dcontroller/pkg/api/operator/v1alpha1"
	"hsnlab/dcontroller/pkg/cache"
	"hsnlab/dcontroller/pkg/expression"
	"hsnlab/dcontroller/pkg/object"
)

var _ = Describe("Aggregations", func() {
	var objs []object.Object
	var eng Engine

	BeforeEach(func() {
		objs = []object.Object{object.NewViewObject("view"), object.NewViewObject("view")}
		object.SetContent(objs[0], unstruct{
			"spec": unstruct{
				"a": int64(1),
				"b": unstruct{"c": int64(2)},
			},
			"c": "c",
		})
		object.SetName(objs[0], "default", "name")
		object.SetContent(objs[1], unstruct{
			"spec": unstruct{
				"a": int64(2),
				"b": unstruct{"c": int64(3)},
			},
			"d": "d",
		})
		object.SetName(objs[1], "default", "name2")
		eng = NewDefaultEngine("view", emptyView, logger)
	})

	Describe("Evaluating select aggregations", func() {
		It("should evaluate true select expression", func() {
			jsonData := `{"@aggregate":[{"@select":{"@eq":["$.metadata.name","name"]}}]}`
			ag := newAggregation(eng, []byte(jsonData))

			res, err := ag.Evaluate(cache.Delta{Type: cache.Upserted, Object: objs[0]})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(1))
			Expect(res[0].IsUnchanged()).To(BeFalse())
			Expect(res[0]).To(Equal(cache.Delta{Type: cache.Added, Object: objs[0]}))

			res, err = ag.Evaluate(cache.Delta{Type: cache.Added, Object: objs[1]})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(BeEmpty())
		})

		It("should evaluate a false select expression", func() {
			jsonData := `{"@aggregate":[{"@select":{"@eq":["$.spec.b.c",1]}}]}`
			ag := newAggregation(eng, []byte(jsonData))

			res, err := ag.Evaluate(cache.Delta{Type: cache.Upserted, Object: objs[0]})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(BeEmpty())

			res, err = ag.Evaluate(cache.Delta{Type: cache.Upserted, Object: objs[1]})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(BeEmpty())
		})

		It("should evaluate an inverted false select expression", func() {
			jsonData := `{"@aggregate":[{"@select":{"@not":{"@eq":["$.spec.b.c",1]}}}]}`
			ag := newAggregation(eng, []byte(jsonData))

			res, err := ag.Evaluate(cache.Delta{Type: cache.Upserted, Object: objs[0]})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(1))
			Expect(res[0].IsUnchanged()).To(BeFalse())
			Expect(res[0]).To(Equal(cache.Delta{Type: cache.Added, Object: objs[0]}))

			res, err = ag.Evaluate(cache.Delta{Type: cache.Upserted, Object: objs[1]})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(1))
			Expect(res[0].IsUnchanged()).To(BeFalse())
			Expect(res[0]).To(Equal(cache.Delta{Type: cache.Added, Object: objs[1]}))
		})

		It("should not err for a select expression referring to a nonexistent field", func() {
			jsonData := `{"@aggregate":[{"@select":{"@eq":["$.spec.x",true]}}]}`
			ag := newAggregation(eng, []byte(jsonData))

			res, err := ag.Evaluate(cache.Delta{Type: cache.Upserted, Object: objs[0]})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(BeEmpty())
		})
	})

	Describe("Evaluating projection aggregations", func() {
		It("should evaluate a simple projection expression", func() {
			jsonData := `{"@aggregate":[{"@project":{"metadata":{"name":"$.metadata.name"}}}]}`
			ag := newAggregation(eng, []byte(jsonData))

			Expect(ag.Expressions).To(HaveLen(1))
			Expect(ag.Expressions[0]).To(Equal(expression.Expression{
				Op: "@project",
				Arg: &expression.Expression{
					Op: "@dict",
					Literal: map[string]expression.Expression{
						"metadata": {
							Op: "@dict",
							Literal: map[string]expression.Expression{
								"name": {
									Op:      "@string",
									Literal: "$.metadata.name",
								},
							},
						},
					},
				},
			}))

			res, err := ag.Evaluate(cache.Delta{Type: cache.Updated, Object: objs[0]})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(1))
			Expect(res[0].Type).To(Equal(cache.Added))
			Expect(res[0].Object).To(Equal(&unstructured.Unstructured{
				Object: unstruct{
					"apiVersion": "view.dcontroller.io/v1alpha1",
					"kind":       "view",
					"metadata": unstruct{
						"name": "name",
					},
				},
			}))
		})

		It("should evaluate a projection expression with multiple fields", func() {
			jsonData := `{"@aggregate":[{"@project":{"metadata":{"name":"$.metadata.name","namespace":"$.metadata.namespace"}}}]}`
			ag := newAggregation(eng, []byte(jsonData))

			res, err := ag.Evaluate(cache.Delta{Type: cache.Updated, Object: objs[0]})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(1))
			Expect(res[0].Type).To(Equal(cache.Added))

			obj := res[0].Object
			raw, ok := obj.Object["metadata"]
			Expect(ok).To(BeTrue())
			meta, ok := raw.(unstruct)
			Expect(ok).To(BeTrue())
			Expect(meta["namespace"]).To(Equal("default"))
			Expect(meta["name"]).To(Equal("name"))
		})

		It("should evaluate a projection expression that copies a subtree", func() {
			jsonData := `{"@aggregate":[{"@project":{"metadata":"$.metadata"}}]}`
			ag := newAggregation(eng, []byte(jsonData))

			res, err := ag.Evaluate(cache.Delta{Type: cache.Updated, Object: objs[0]})
			Expect(err).NotTo(HaveOccurred())

			Expect(res).To(HaveLen(1))
			obj := res[0].Object
			raw, ok := obj.Object["metadata"]
			Expect(ok).To(BeTrue())
			meta, ok := raw.(unstruct)
			Expect(ok).To(BeTrue())
			Expect(meta["namespace"]).To(Equal("default"))
			Expect(meta["name"]).To(Equal("name"))
		})

		It("should evaluate a projection expression that alters the name", func() {
			jsonData := `
'@aggregate':
  - '@project':
      metadata:
        name:
          '@concat':
            - $.metadata.name
            - ':'
            - $.c
        namespace: $.metadata.namespace`
			ag := newAggregation(eng, []byte(jsonData))

			res, err := ag.Evaluate(cache.Delta{Type: cache.Updated, Object: objs[0]})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(1))
			Expect(res[0].Type).To(Equal(cache.Added))
			obj := res[0].Object
			Expect(obj.GetNamespace()).To(Equal("default"))
			Expect(obj.GetName()).To(Equal("name:c"))

			newObj := object.DeepCopy(objs[0])
			Expect(unstructured.SetNestedField(newObj.UnstructuredContent(), "d", "c")).
				NotTo(HaveOccurred())
			val, ok, err := unstructured.NestedFieldNoCopy(newObj.UnstructuredContent(), "c")
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeTrue())
			Expect(val).To(Equal("d"))

			// this should remove name:c and add name:d
			res, err = ag.Evaluate(cache.Delta{Type: cache.Updated, Object: newObj})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(2))
			Expect(res[0].Type).To(Equal(cache.Deleted))
			obj = res[0].Object
			Expect(obj.GetNamespace()).To(Equal("default"))
			Expect(obj.GetName()).To(Equal("name:c"))
			Expect(res[1].Type).To(Equal(cache.Added))
			obj = res[1].Object
			Expect(obj.GetNamespace()).To(Equal("default"))
			Expect(obj.GetName()).To(Equal("name:d"))

		})

		It("should err for a projection that drops .metadata.name", func() {
			jsonData := `{"@aggregate":[{"@project":{"spec":"$.spec"}}]}`
			ag := newAggregation(eng, []byte(jsonData))

			_, err := ag.Evaluate(cache.Delta{Type: cache.Updated, Object: objs[0]})
			Expect(err).To(HaveOccurred())
		})

		It("should err for a projection that asks for a non-existent field", func() {
			jsonData := `{"@aggregate":[{"@project":{"x": "$.spec.x"}}]}`
			ag := newAggregation(eng, []byte(jsonData))

			_, err := ag.Evaluate(cache.Delta{Type: cache.Updated, Object: objs[0]})
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("Evaluating aggregations on native Unstructured objects", func() {
		It("should evaluate a simple projection expression", func() {
			jsonData := `{"@aggregate":[{"@project":{"metadata":"$.metadata"}}]}`
			ag := newAggregation(eng, []byte(jsonData))

			Expect(ag.Expressions).To(HaveLen(1))

			obj := &unstructured.Unstructured{}
			obj.SetGroupVersionKind(schema.GroupVersionKind{Group: "testgroup", Version: "v1", Kind: "testkind"})
			obj.SetName("test-name")
			obj.SetNamespace("test-ns")

			res, err := ag.Evaluate(cache.Delta{Type: cache.Updated, Object: obj})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(1))
			Expect(res[0].Type).To(Equal(cache.Added))
			Expect(res[0].Object).To(Equal(&unstructured.Unstructured{
				Object: unstruct{
					"apiVersion": "view.dcontroller.io/v1alpha1",
					"kind":       "view",
					"metadata": unstruct{
						"namespace": "test-ns",
						"name":      "test-name",
					},
				},
			}))
		})

		It("should evaluate true select expression", func() {
			jsonData := `{"@aggregate":[{"@select":{"@eq":["$.spec.b",1]}}]}`
			ag := newAggregation(eng, []byte(jsonData))

			gvk := schema.GroupVersionKind{Group: "testgroup", Version: "v1", Kind: "testkind"}

			// Add object
			obj := &unstructured.Unstructured{}
			obj.SetUnstructuredContent(map[string]any{"spec": map[string]any{"b": int64(1)}})
			obj.SetGroupVersionKind(gvk)
			obj.SetName("test-name")
			obj.SetNamespace("test-ns")

			res, err := ag.Evaluate(cache.Delta{Type: cache.Upserted, Object: obj})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(1))
			Expect(res[0].IsUnchanged()).To(BeFalse())
			resObj := object.NewViewObject("view")
			object.SetContent(resObj, obj.UnstructuredContent())
			object.SetName(resObj, "test-ns", "test-name")
			Expect(res[0]).To(Equal(cache.Delta{Type: cache.Added, Object: resObj}))

			Expect(eng.(*defaultEngine).baseViewStore).To(HaveKey(gvk))
			store := eng.(*defaultEngine).baseViewStore[gvk]
			Expect(store.List()).To(HaveLen(1))
			x, ok, err := store.Get(obj)
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeTrue())
			Expect(x).To(Equal(obj))

			// Update will remove the object from the view
			oldObj := object.DeepCopy(obj)
			obj.SetUnstructuredContent(map[string]any{"spec": map[string]any{"b": int64(2)}})
			obj.SetGroupVersionKind(gvk)
			obj.SetName("test-name") // the previous call removes namespace/name
			obj.SetNamespace("test-ns")
			res, err = ag.Evaluate(cache.Delta{Type: cache.Upserted, Object: obj})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(1))
			Expect(res[0].IsUnchanged()).To(BeFalse())
			resObj = object.NewViewObject("view")
			object.SetContent(resObj, oldObj.UnstructuredContent())
			object.SetName(resObj, "test-ns", "test-name")
			Expect(res[0]).To(Equal(cache.Delta{Type: cache.Deleted, Object: resObj}))

			Expect(store.List()).To(HaveLen(1)) // contains the changed base object
			x, ok, err = store.Get(obj)
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeTrue())
			Expect(x).To(Equal(obj))

			// re-introduce object into the view
			obj.SetUnstructuredContent(map[string]any{"spec": map[string]any{"b": int64(1)}})
			obj.SetGroupVersionKind(gvk)
			obj.SetName("test-name") // the previous call removes namespace/name
			obj.SetNamespace("test-ns")
			res, err = ag.Evaluate(cache.Delta{Type: cache.Upserted, Object: obj})
			Expect(res).To(HaveLen(1))
			Expect(err).NotTo(HaveOccurred())
			Expect(res[0].IsUnchanged()).To(BeFalse())
			resObj = object.NewViewObject("view")
			object.SetContent(resObj, obj.UnstructuredContent())
			object.SetName(resObj, "test-ns", "test-name")
			Expect(res[0]).To(Equal(cache.Delta{Type: cache.Added, Object: resObj}))

			Expect(store.List()).To(HaveLen(1))
			x, ok, err = store.Get(obj)
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeTrue())
			Expect(x).To(Equal(obj))

			// add another object into the view
			obj2 := &unstructured.Unstructured{}
			obj2.SetUnstructuredContent(map[string]any{"spec": map[string]any{"b": int64(1)}})
			obj2.SetGroupVersionKind(gvk)
			obj2.SetName("test-name-2") // the previous call removes namespace/name
			obj2.SetNamespace("test-ns")
			res, err = ag.Evaluate(cache.Delta{Type: cache.Upserted, Object: obj2})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(1))
			Expect(res[0].IsUnchanged()).To(BeFalse())
			resObj = object.NewViewObject("view")
			object.SetContent(resObj, obj2.UnstructuredContent())
			object.SetName(resObj, "test-ns", "test-name-2")
			Expect(res[0]).To(Equal(cache.Delta{Type: cache.Added, Object: resObj}))

			Expect(store.List()).To(HaveLen(2))
			x, ok, err = store.Get(obj)
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeTrue())
			Expect(x).To(Equal(obj))
			x, ok, err = store.Get(obj2)
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeTrue())
			Expect(x).To(Equal(obj2))

			// remove first object
			res, err = ag.Evaluate(cache.Delta{Type: cache.Deleted, Object: obj})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(1))
			Expect(res[0].IsUnchanged()).To(BeFalse())
			resObj = object.NewViewObject("view")
			object.SetContent(resObj, obj.UnstructuredContent())
			object.SetName(resObj, "test-ns", "test-name")
			Expect(res[0]).To(Equal(cache.Delta{Type: cache.Deleted, Object: resObj}))

			// doesn't really change anything
			obj.SetUnstructuredContent(map[string]any{"spec": map[string]any{"b": int64(3)}})
			obj.SetName("test-name") // the previous call removes namespace/name
			obj.SetNamespace("test-ns")
			res, err = ag.Evaluate(cache.Delta{Type: cache.Upserted, Object: obj})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(BeEmpty())
		})
	})

	Describe("Evaluating demultiplexer aggregations", func() {
		It("should evaluate a simple @demux expression", func() {
			obj := object.NewViewObject("view")
			// must have a valid name
			object.SetContent(obj, unstruct{
				"spec": unstruct{
					"list": []any{int64(1), "a", true},
				},
			})
			object.SetName(obj, "default", "name")

			jsonData := `{"@aggregate":[{"@demux": "$.spec.list"}]}`
			ag := newAggregation(eng, []byte(jsonData))
			Expect(ag.Expressions).To(HaveLen(1))

			res, err := ag.Evaluate(cache.Delta{Type: cache.Added, Object: obj})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(3))

			Expect(res[0].Type).To(Equal(cache.Added))
			Expect(res[0].Object).To(Equal(&unstructured.Unstructured{
				Object: unstruct{
					"apiVersion": "view.dcontroller.io/v1alpha1",
					"kind":       "view",
					"metadata": unstruct{
						"name":      "name-0",
						"namespace": "default",
					},
					"spec": unstruct{
						"list": int64(1),
					},
				},
			}))

			Expect(res[1].Type).To(Equal(cache.Added))
			Expect(res[1].Object).To(Equal(&unstructured.Unstructured{
				Object: unstruct{
					"apiVersion": "view.dcontroller.io/v1alpha1",
					"kind":       "view",
					"metadata": unstruct{
						"name":      "name-1",
						"namespace": "default",
					},
					"spec": unstruct{
						"list": "a",
					},
				},
			}))

			Expect(res[2].Type).To(Equal(cache.Added))
			Expect(res[2].Object).To(Equal(&unstructured.Unstructured{
				Object: unstruct{
					"apiVersion": "view.dcontroller.io/v1alpha1",
					"kind":       "view",
					"metadata": unstruct{
						"name":      "name-2",
						"namespace": "default",
					},
					"spec": unstruct{
						"list": true,
					},
				},
			}))

		})

	})
})

func newAggregation(eng Engine, data []byte) *Aggregation {
	var a opv1a1.Aggregation
	err := yaml.Unmarshal(data, &a)
	Expect(err).NotTo(HaveOccurred())
	return NewAggregation(eng, &a)
}

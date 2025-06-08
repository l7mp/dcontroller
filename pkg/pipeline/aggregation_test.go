package pipeline

import (
	"slices"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/yaml"

	opv1a1 "github.com/l7mp/dcontroller/pkg/api/operator/v1alpha1"
	"github.com/l7mp/dcontroller/pkg/cache"
	"github.com/l7mp/dcontroller/pkg/expression"
	"github.com/l7mp/dcontroller/pkg/object"
)

var gvk = schema.GroupVersionKind{Group: "view.dcontroller.io", Version: "v1alpha1", Kind: "view"}

var _ = Describe("Aggregations", func() {
	var objs []object.Object

	BeforeEach(func() {
		objs = []object.Object{object.NewViewObject("view"), object.NewViewObject("view")}
		object.SetContent(objs[0], map[string]any{
			"spec": map[string]any{
				"a": int64(1),
				"b": map[string]any{"c": int64(2)},
			},
			"c": "c",
		})
		object.SetName(objs[0], "default", "name")
		object.SetContent(objs[1], map[string]any{
			"spec": map[string]any{
				"a": int64(2),
				"b": map[string]any{"c": int64(3)},
			},
			"d": "d",
		})
		object.SetName(objs[1], "default", "name2")
	})

	Describe("Evaluating select aggregations", func() {
		It("should evaluate true select expression", func() {
			jsonData := `{"@aggregate":[{"@select":{"@eq":["$.metadata.name","name"]}}]}`
			p, err := newAggregation(jsonData)
			Expect(err).NotTo(HaveOccurred())

			res, err := p.Evaluate(cache.Delta{Type: cache.Upserted, Object: objs[0]})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(1))
			Expect(res[0].IsUnchanged()).To(BeFalse())
			Expect(res[0]).To(Equal(cache.Delta{Type: cache.Upserted, Object: objs[0]}))

			res, err = p.Evaluate(cache.Delta{Type: cache.Upserted, Object: objs[1]})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(BeEmpty())
		})

		It("should evaluate a false select expression", func() {
			jsonData := `{"@aggregate":[{"@select":{"@eq":["$.spec.b.c",1]}}]}`
			ag, err := newAggregation(jsonData)
			Expect(err).NotTo(HaveOccurred())

			res, err := ag.Evaluate(cache.Delta{Type: cache.Upserted, Object: objs[0]})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(BeEmpty())

			res, err = ag.Evaluate(cache.Delta{Type: cache.Upserted, Object: objs[1]})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(BeEmpty())
		})

		It("should evaluate an inverted false select expression", func() {
			jsonData := `{"@aggregate":[{"@select":{"@not":{"@eq":["$.spec.b.c",1]}}}]}`
			ag, err := newAggregation(jsonData)
			Expect(err).NotTo(HaveOccurred())

			res, err := ag.Evaluate(cache.Delta{Type: cache.Upserted, Object: objs[0]})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(1))
			Expect(res[0].IsUnchanged()).To(BeFalse())
			Expect(res[0]).To(Equal(cache.Delta{Type: cache.Upserted, Object: objs[0]}))

			res, err = ag.Evaluate(cache.Delta{Type: cache.Upserted, Object: objs[1]})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(1))
			Expect(res[0].IsUnchanged()).To(BeFalse())
			Expect(res[0]).To(Equal(cache.Delta{Type: cache.Upserted, Object: objs[1]}))
		})

		It("should not err for a select expression referring to a nonexistent field", func() {
			jsonData := `{"@aggregate":[{"@select":{"@eq":["$.spec.x",true]}}]}`
			ag, err := newAggregation(jsonData)
			Expect(err).NotTo(HaveOccurred())

			res, err := ag.Evaluate(cache.Delta{Type: cache.Upserted, Object: objs[0]})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(BeEmpty())
		})
	})

	Describe("Evaluating projection aggregations", func() {
		It("should evaluate a simple projection expression", func() {
			jsonData := `{"@aggregate":[{"@project":{"metadata":{"name":"$.metadata.name"}}}]}`
			var a opv1a1.Aggregation
			err := yaml.Unmarshal([]byte(jsonData), &a)
			Expect(err).NotTo(HaveOccurred())
			Expect(a.Expressions).To(HaveLen(1))
			Expect(a.Expressions[0]).To(Equal(expression.Expression{
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

			ag, err := newAggregation(jsonData)
			Expect(err).NotTo(HaveOccurred())
			res, err := ag.Evaluate(cache.Delta{Type: cache.Upserted, Object: objs[0]})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(1))
			Expect(res[0].Type).To(Equal(cache.Upserted))
			Expect(res[0].Object).To(Equal(&unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": "view.dcontroller.io/v1alpha1",
					"kind":       "view",
					"metadata": map[string]any{
						"name": "name",
					},
				},
			}))
		})

		It("should evaluate a projection expression with multiple fields", func() {
			jsonData := `{"@aggregate":[{"@project":{"metadata":{"name":"$.metadata.name","namespace":"$.metadata.namespace"}}}]}`
			ag, err := newAggregation(jsonData)
			Expect(err).NotTo(HaveOccurred())

			res, err := ag.Evaluate(cache.Delta{Type: cache.Upserted, Object: objs[0]})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(1))
			Expect(res[0].Type).To(Equal(cache.Upserted))

			obj := res[0].Object
			raw, ok := obj.Object["metadata"]
			Expect(ok).To(BeTrue())
			meta, ok := raw.(map[string]any)
			Expect(ok).To(BeTrue())
			Expect(meta["namespace"]).To(Equal("default"))
			Expect(meta["name"]).To(Equal("name"))
		})

		It("should evaluate a projection expression that copies a subtree", func() {
			jsonData := `{"@aggregate":[{"@project":{"metadata":"$.metadata"}}]}`
			ag, err := newAggregation(jsonData)
			Expect(err).NotTo(HaveOccurred())

			res, err := ag.Evaluate(cache.Delta{Type: cache.Upserted, Object: objs[0]})
			Expect(err).NotTo(HaveOccurred())

			Expect(res).To(HaveLen(1))
			obj := res[0].Object
			raw, ok := obj.Object["metadata"]
			Expect(ok).To(BeTrue())
			meta, ok := raw.(map[string]any)
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
			ag, err := newAggregation(jsonData)
			Expect(err).NotTo(HaveOccurred())

			res, err := ag.Evaluate(cache.Delta{Type: cache.Upserted, Object: objs[0]})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(1))
			Expect(res[0].Type).To(Equal(cache.Upserted))
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
			res, err = ag.Evaluate(cache.Delta{Type: cache.Upserted, Object: newObj})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(2))
			Expect(res[0].Type).To(Equal(cache.Deleted))
			obj = res[0].Object
			Expect(obj.GetNamespace()).To(Equal("default"))
			Expect(obj.GetName()).To(Equal("name:c"))
			Expect(res[1].Type).To(Equal(cache.Upserted))
			obj = res[1].Object
			Expect(obj.GetNamespace()).To(Equal("default"))
			Expect(obj.GetName()).To(Equal("name:d"))

		})

		It("should evaluate a projection expression that contains a list of setters", func() {
			jsonData := `
'@aggregate':
  - '@project':
      - metadata:
          name: name
          namespace: default
      - spec: 123`
			ag, err := newAggregation(jsonData)
			Expect(err).NotTo(HaveOccurred())

			res, err := ag.Evaluate(cache.Delta{Type: cache.Upserted, Object: objs[0]})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(1))
			Expect(res[0].Type).To(Equal(cache.Upserted))
			obj := res[0].Object
			Expect(obj.GetNamespace()).To(Equal("default"))
			Expect(obj.GetName()).To(Equal("name"))
			Expect(obj).To(Equal(&unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": "view.dcontroller.io/v1alpha1",
					"kind":       "view",
					"metadata": map[string]any{
						"namespace": "default",
						"name":      "name",
					},
					"spec": int64(123),
				},
			}))
		})

		It("should evaluate a projection expression that contains a list of JSONpath setters", func() {
			jsonData := `
'@aggregate':
  - '@project':
      - $.metadata.name: $.metadata.name
      - $.metadata.namespace: "default"
      - $.spec.a: 123
      - $.spec.b: $.spec.b`
			ag, err := newAggregation(jsonData)
			Expect(err).NotTo(HaveOccurred())

			res, err := ag.Evaluate(cache.Delta{Type: cache.Upserted, Object: objs[0]})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(1))
			Expect(res[0].Type).To(Equal(cache.Upserted))
			obj := res[0].Object
			Expect(obj.GetNamespace()).To(Equal("default"))
			Expect(obj.GetName()).To(Equal("name"))
			Expect(obj).To(Equal(&unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": "view.dcontroller.io/v1alpha1",
					"kind":       "view",
					"metadata": map[string]any{
						"namespace": "default",
						"name":      "name",
					},
					"spec": map[string]any{
						"a": int64(123),
						"b": map[string]any{"c": int64(2)},
					},
				},
			}))
		})

		It("should evaluate a projection expression that contains a list of mixed (fix/JSONpath) setters", func() {
			jsonData := `
'@aggregate':
  - '@project':
      - {metadata: {name: name2}}
      - $.metadata.namespace: "default2"
      - $.spec.a: $.spec.b`
			ag, err := newAggregation(jsonData)
			Expect(err).NotTo(HaveOccurred())

			res, err := ag.Evaluate(cache.Delta{Type: cache.Upserted, Object: objs[0]})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(1))
			Expect(res[0].Type).To(Equal(cache.Upserted))
			obj := res[0].Object
			Expect(obj.GetNamespace()).To(Equal("default2"))
			Expect(obj.GetName()).To(Equal("name2"))
			Expect(obj).To(Equal(&unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": "view.dcontroller.io/v1alpha1",
					"kind":       "view",
					"metadata": map[string]any{
						"namespace": "default2",
						"name":      "name2",
					},
					"spec": map[string]any{
						"a": map[string]any{"c": int64(2)},
					},
				},
			}))
		})

		It("should collapse multiple adds that yield the same object name to an update", func() {
			jsonData := `
'@aggregate':
  - '@project':
      $.metadata.name: "fixed"`
			ag, err := newAggregation(jsonData)
			Expect(err).NotTo(HaveOccurred())

			res, err := ag.Evaluate(cache.Delta{Type: cache.Upserted, Object: objs[0]})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(1))

			Expect(res[0]).To(Equal(
				cache.Delta{
					Type: cache.Upserted,
					Object: &unstructured.Unstructured{
						Object: map[string]any{
							"apiVersion": "view.dcontroller.io/v1alpha1",
							"kind":       "view",
							"metadata": map[string]any{
								"name": "fixed",
							},
						},
					},
				}))

			res, err = ag.Evaluate(cache.Delta{Type: cache.Upserted, Object: objs[1]})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(1))

			Expect(res[0]).To(Equal(
				cache.Delta{
					Type: cache.Upserted,
					Object: &unstructured.Unstructured{
						Object: map[string]any{
							"apiVersion": "view.dcontroller.io/v1alpha1",
							"kind":       "view",
							"metadata": map[string]any{
								"name": "fixed",
							},
						},
					},
				}))
		})

		It("should err for a projection that drops .metadata.name", func() {
			jsonData := `{"@aggregate":[{"@project":{"spec":"$.spec"}}]}`
			ag, err := newAggregation(jsonData)
			Expect(err).NotTo(HaveOccurred())

			_, err = ag.Evaluate(cache.Delta{Type: cache.Upserted, Object: objs[0]})
			Expect(err).To(HaveOccurred())
		})

		It("should err for a projection that asks for a non-existent field", func() {
			jsonData := `{"@aggregate":[{"@project":{"x": "$.spec.x"}}]}`
			ag, err := newAggregation(jsonData)
			Expect(err).NotTo(HaveOccurred())

			_, err = ag.Evaluate(cache.Delta{Type: cache.Upserted, Object: objs[0]})
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("Evaluating aggregations on native Unstructured objects", func() {
		It("should evaluate a simple projection expression", func() {
			jsonData := `{"@aggregate":[{"@project":{"metadata":"$.metadata"}}]}`
			ag, err := newAggregation(jsonData)
			Expect(err).NotTo(HaveOccurred())

			obj := object.New()
			obj.SetGroupVersionKind(gvk)
			obj.SetName("test-name")
			obj.SetNamespace("test-ns")

			res, err := ag.Evaluate(cache.Delta{Type: cache.Upserted, Object: obj})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(1))
			Expect(res[0].Type).To(Equal(cache.Upserted))
			Expect(res[0].Object).To(Equal(&unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": "view.dcontroller.io/v1alpha1",
					"kind":       "view",
					"metadata": map[string]any{
						"namespace": "test-ns",
						"name":      "test-name",
					},
				},
			}))
		})

		It("should evaluate true select expression", func() {
			jsonData := `{"@aggregate":[{"@select":{"@eq":["$.spec.b",1]}}]}`
			ag, err := newAggregation(jsonData)
			Expect(err).NotTo(HaveOccurred())

			// Add object
			obj := object.New()
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
			Expect(res[0]).To(Equal(cache.Delta{Type: cache.Upserted, Object: resObj}))

			p, ok := ag.(*Pipeline)
			Expect(ok).To(BeTrue())
			Expect(p.sourceCache).To(HaveKey(gvk))
			store := p.sourceCache[gvk]
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
			Expect(res[0]).To(Equal(cache.Delta{Type: cache.Upserted, Object: resObj}))

			Expect(store.List()).To(HaveLen(1))
			x, ok, err = store.Get(obj)
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeTrue())
			Expect(x).To(Equal(obj))

			// add another object into the view
			obj2 := object.New()
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
			Expect(res[0]).To(Equal(cache.Delta{Type: cache.Upserted, Object: resObj}))

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
			obj.SetGroupVersionKind(gvk)
			res, err = ag.Evaluate(cache.Delta{Type: cache.Upserted, Object: obj})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(BeEmpty())
		})
	})

	Describe("Evaluating demultiplexer/unwind aggregations", func() {
		It("should evaluate a simple demux expression", func() {
			obj := object.NewViewObject("view")
			// must have a valid name
			object.SetContent(obj, map[string]any{
				"spec": map[string]any{
					"list": []any{int64(1), "a", true},
				},
			})
			object.SetName(obj, "default", "name")

			jsonData := `{"@aggregate":[{"@unwind": "$.spec.list"}]}`
			ag, err := newAggregation(jsonData)
			Expect(err).NotTo(HaveOccurred())

			res, err := ag.Evaluate(cache.Delta{Type: cache.Upserted, Object: obj})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(3))

			Expect(res).To(ContainElement(
				cache.Delta{
					Type: cache.Upserted,
					Object: &unstructured.Unstructured{
						Object: map[string]any{
							"apiVersion": "view.dcontroller.io/v1alpha1",
							"kind":       "view",
							"metadata": map[string]any{
								"name":      "name-0",
								"namespace": "default",
							},
							"spec": map[string]any{
								"list": int64(1),
							},
						},
					},
				}))

			Expect(res).To(ContainElement(
				cache.Delta{
					Type: cache.Upserted,
					Object: &unstructured.Unstructured{
						Object: map[string]any{
							"apiVersion": "view.dcontroller.io/v1alpha1",
							"kind":       "view",
							"metadata": map[string]any{
								"name":      "name-1",
								"namespace": "default",
							},
							"spec": map[string]any{
								"list": "a",
							},
						},
					},
				}))

			Expect(res).To(ContainElement(
				cache.Delta{
					Type: cache.Upserted,
					Object: &unstructured.Unstructured{
						Object: map[string]any{
							"apiVersion": "view.dcontroller.io/v1alpha1",
							"kind":       "view",
							"metadata": map[string]any{
								"name":      "name-2",
								"namespace": "default",
							},
							"spec": map[string]any{
								"list": true,
							},
						},
					},
				}))
		})

		It("should evaluate a nested demux expression", func() {
			obj := object.NewViewObject("view")
			// must have a valid name
			object.SetContent(obj, map[string]any{
				"spec": map[string]any{
					"list": []any{
						[]any{int64(1), int64(2), int64(3)},
						[]any{int64(5), int64(6)},
					},
				},
			})
			object.SetName(obj, "default", "name")

			jsonData := `{"@aggregate":[{"@unwind": "$.spec.list"}, {"@unwind": "$.spec.list"}]}`
			ag, err := newAggregation(jsonData)
			Expect(err).NotTo(HaveOccurred())

			res, err := ag.Evaluate(cache.Delta{Type: cache.Upserted, Object: obj})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(5))

			Expect(res).To(ContainElement(
				cache.Delta{
					Type: cache.Upserted,
					Object: &unstructured.Unstructured{
						Object: map[string]any{
							"apiVersion": "view.dcontroller.io/v1alpha1",
							"kind":       "view",
							"metadata": map[string]any{
								"name":      "name-0-0",
								"namespace": "default",
							},
							"spec": map[string]any{
								"list": int64(1),
							},
						},
					},
				}))

			Expect(res).To(ContainElement(
				cache.Delta{
					Type: cache.Upserted,
					Object: &unstructured.Unstructured{
						Object: map[string]any{
							"apiVersion": "view.dcontroller.io/v1alpha1",
							"kind":       "view",
							"metadata": map[string]any{
								"name":      "name-0-1",
								"namespace": "default",
							},
							"spec": map[string]any{
								"list": int64(2),
							},
						},
					},
				}))

			Expect(res).To(ContainElement(
				cache.Delta{
					Type: cache.Upserted,
					Object: &unstructured.Unstructured{
						Object: map[string]any{
							"apiVersion": "view.dcontroller.io/v1alpha1",
							"kind":       "view",
							"metadata": map[string]any{
								"name":      "name-0-2",
								"namespace": "default",
							},
							"spec": map[string]any{
								"list": int64(3),
							},
						},
					},
				}))

			Expect(res).To(ContainElement(
				cache.Delta{
					Type: cache.Upserted,
					Object: &unstructured.Unstructured{
						Object: map[string]any{
							"apiVersion": "view.dcontroller.io/v1alpha1",
							"kind":       "view",
							"metadata": map[string]any{
								"name":      "name-1-0",
								"namespace": "default",
							},
							"spec": map[string]any{
								"list": int64(5),
							},
						},
					},
				}))

			Expect(res).To(ContainElement(
				cache.Delta{
					Type: cache.Upserted,
					Object: &unstructured.Unstructured{
						Object: map[string]any{
							"apiVersion": "view.dcontroller.io/v1alpha1",
							"kind":       "view",
							"metadata": map[string]any{
								"name":      "name-1-1",
								"namespace": "default",
							},
							"spec": map[string]any{
								"list": int64(6),
							},
						},
					},
				}))
		})

		It("a demux expression pointing to a nonexistent key should return a nil delta", func() {
			obj := object.NewViewObject("view")
			// must have a valid name
			object.SetContent(obj, map[string]any{
				"spec": map[string]any{},
			})
			object.SetName(obj, "default", "name")

			jsonData := `{"@aggregate":[{"@unwind": "$.spec.list"}]}`
			ag, err := newAggregation(jsonData)
			Expect(err).NotTo(HaveOccurred())

			res, err := ag.Evaluate(cache.Delta{Type: cache.Added, Object: obj})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(BeEmpty())
		})

		It("should evaluate a demux expression with an empty list to a nil delta", func() {
			obj := object.NewViewObject("view")
			// must have a valid name
			object.SetContent(obj, map[string]any{
				"spec": map[string]any{
					"list": []any{},
				},
			})
			object.SetName(obj, "default", "name")

			jsonData := `{"@aggregate":[{"@unwind": "$.spec.list"}]}`
			ag, err := newAggregation(jsonData)
			Expect(err).NotTo(HaveOccurred())

			res, err := ag.Evaluate(cache.Delta{Type: cache.Added, Object: obj})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(BeEmpty())
		})

		It("should evaluate an update with demux expressions that set the object name", func() {
			obj := object.NewViewObject("view")
			// must have a valid name
			object.SetContent(obj, map[string]any{
				"spec": map[string]any{
					"list": []any{"a", "b", "c"},
				},
			})
			object.SetName(obj, "default", "name")

			jsonData := `{"@aggregate":[{"@unwind":"$.spec.list"},{"@project":{"metadata":{"name":"$.spec.list"}}}]}`
			ag, err := newAggregation(jsonData)
			Expect(err).NotTo(HaveOccurred())

			res, err := ag.Evaluate(cache.Delta{Type: cache.Upserted, Object: obj})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(3))

			Expect(res).To(ContainElement(
				cache.Delta{
					Type: cache.Upserted,
					Object: &unstructured.Unstructured{
						Object: map[string]any{
							"apiVersion": "view.dcontroller.io/v1alpha1",
							"kind":       "view",
							"metadata": map[string]any{
								"name": "a",
							},
						},
					},
				}))

			Expect(res).To(ContainElement(
				cache.Delta{
					Type: cache.Upserted,
					Object: &unstructured.Unstructured{
						Object: map[string]any{
							"apiVersion": "view.dcontroller.io/v1alpha1",
							"kind":       "view",
							"metadata": map[string]any{
								"name": "b",
							},
						},
					},
				}))

			Expect(res).To(ContainElement(
				cache.Delta{
					Type: cache.Upserted,
					Object: &unstructured.Unstructured{
						Object: map[string]any{
							"apiVersion": "view.dcontroller.io/v1alpha1",
							"kind":       "view",
							"metadata": map[string]any{
								"name": "c",
							},
						},
					},
				}))

			// update the list
			object.SetContent(obj, map[string]any{
				"spec": map[string]any{
					"list": []any{"c", "d"},
				},
			})
			object.SetName(obj, "default", "name")

			res, err = ag.Evaluate(cache.Delta{Type: cache.Upserted, Object: obj})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(3))

			Expect(res).To(ContainElement(
				cache.Delta{
					Type: cache.Deleted,
					Object: &unstructured.Unstructured{
						Object: map[string]any{
							"apiVersion": "view.dcontroller.io/v1alpha1",
							"kind":       "view",
							"metadata": map[string]any{
								"name": "a",
							},
						},
					},
				}))

			Expect(res).To(ContainElement(
				cache.Delta{
					Type: cache.Deleted,
					Object: &unstructured.Unstructured{
						Object: map[string]any{
							"apiVersion": "view.dcontroller.io/v1alpha1",
							"kind":       "view",
							"metadata": map[string]any{
								"name": "b",
							},
						},
					},
				}))

			Expect(res).To(ContainElement(
				cache.Delta{
					Type: cache.Upserted,
					Object: &unstructured.Unstructured{
						Object: map[string]any{
							"apiVersion": "view.dcontroller.io/v1alpha1",
							"kind":       "view",
							"metadata": map[string]any{
								"name": "d",
							},
						},
					},
				}))
		})
	})

	Describe("Evaluating multiplexer aggregations", func() {
		It("should evaluate a raw mux expression", func() {
			jsonData := `{"@aggregate":[{"@gather":["$.metadata.namespace","$.spec.a"]}]}`
			ag, err := newAggregation(jsonData)
			Expect(err).NotTo(HaveOccurred())

			res, err := ag.Evaluate(cache.Delta{Type: cache.Added, Object: objs[0]})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(1))

			Expect(res[0]).To(Equal(
				cache.Delta{
					Type: cache.Upserted,
					Object: &unstructured.Unstructured{
						Object: map[string]any{
							"apiVersion": "view.dcontroller.io/v1alpha1",
							"kind":       "view",
							"metadata": map[string]any{
								"name":      "name",
								"namespace": "default",
							},
							"spec": map[string]any{
								"a": []any{int64(1)},
								"b": map[string]any{"c": int64(2)},
							},
							"c": "c",
						},
					},
				}))

			res, err = ag.Evaluate(cache.Delta{Type: cache.Upserted, Object: objs[1]})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(1))

			// must sort
			Expect(res[0].Object).NotTo(BeNil())
			Expect(res[0].Object.Object["spec"].(map[string]any)).NotTo(BeNil())
			Expect(res[0].Object.Object["spec"].(map[string]any)["a"]).NotTo(BeNil())
			sortAnyInt64(res[0].Object.Object["spec"].(map[string]any)["a"].([]any))

			Expect(res[0]).To(Equal(
				cache.Delta{
					Type: cache.Upserted,
					Object: &unstructured.Unstructured{
						Object: map[string]any{
							"apiVersion": "view.dcontroller.io/v1alpha1",
							"kind":       "view",
							"metadata": map[string]any{
								"name":      "name",
								"namespace": "default",
							},
							"spec": map[string]any{
								"a": []any{int64(1), int64(2)},
								"b": map[string]any{"c": int64(2)},
							},
							"c": "c",
						},
					},
				}))

			obj := objs[0].DeepCopy()
			Expect(unstructured.SetNestedField(obj.UnstructuredContent(), int64(3), "spec", "a")).
				NotTo(HaveOccurred())

			res, err = ag.Evaluate(cache.Delta{Type: cache.Updated, Object: obj})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(1))

			Expect(res[0].Object).NotTo(BeNil())
			Expect(res[0].Object.Object["spec"].(map[string]any)).NotTo(BeNil())
			Expect(res[0].Object.Object["spec"].(map[string]any)["a"]).NotTo(BeNil())
			sortAnyInt64(res[0].Object.Object["spec"].(map[string]any)["a"].([]any))

			Expect(res[0]).To(Equal(
				cache.Delta{
					Type: cache.Upserted,
					Object: &unstructured.Unstructured{
						Object: map[string]any{
							"apiVersion": "view.dcontroller.io/v1alpha1",
							"kind":       "view",
							"metadata": map[string]any{
								"name":      "name",
								"namespace": "default",
							},
							"spec": map[string]any{
								"a": []any{int64(2), int64(3)},
								"b": map[string]any{"c": int64(2)},
							},
							"c": "c",
						},
					},
				}))

			res, err = ag.Evaluate(cache.Delta{Type: cache.Deleted, Object: obj})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(1))

			Expect(res[0]).To(Equal(
				cache.Delta{
					Type: cache.Upserted,
					Object: &unstructured.Unstructured{
						Object: map[string]any{
							"apiVersion": "view.dcontroller.io/v1alpha1",
							"kind":       "view",
							"metadata": map[string]any{
								"name":      "name",
								"namespace": "default",
							},
							"spec": map[string]any{
								"a": []any{int64(2)},
								"b": map[string]any{"c": int64(2)},
							},
							"c": "c",
						},
					},
				}))

			res, err = ag.Evaluate(cache.Delta{Type: cache.Deleted, Object: objs[1]})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(1))

			Expect(res[0]).To(Equal(
				cache.Delta{
					Type: cache.Deleted,
					Object: &unstructured.Unstructured{
						Object: map[string]any{
							"apiVersion": "view.dcontroller.io/v1alpha1",
							"kind":       "view",
							"metadata": map[string]any{
								"name":      "name",
								"namespace": "default",
							},
							"spec": map[string]any{
								"a": []any{int64(2)},
								"b": map[string]any{"c": int64(2)},
							},
							"c": "c",
						},
					},
				}))
		})

		It("should evaluate a mux expression that updates the same object name", func() {
			jsonData := `
'@aggregate':
  - '@gather':
      - $.metadata.namespace
      - $.spec.a
  - '@project':
      metadata:
        name: "gathered"
        namespace: "default"
      spec:
        a: $.spec.a`
			ag, err := newAggregation(jsonData)
			Expect(err).NotTo(HaveOccurred())

			res, err := ag.Evaluate(cache.Delta{Type: cache.Added, Object: objs[0]})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(1))

			Expect(res[0].Type).To(Equal(cache.Upserted))
			Expect(res[0].Object).To(Equal(&unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": "view.dcontroller.io/v1alpha1",
					"kind":       "view",
					"metadata": map[string]any{
						"name":      "gathered",
						"namespace": "default",
					},
					"spec": map[string]any{
						"a": []any{int64(1)},
					},
				},
			}))

			res, err = ag.Evaluate(cache.Delta{Type: cache.Upserted, Object: objs[1]})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(1))

			Expect(res[0].Type).To(Equal(cache.Upserted))
			Expect(res[0].Object).To(Equal(&unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": "view.dcontroller.io/v1alpha1",
					"kind":       "view",
					"metadata": map[string]any{
						"name":      "gathered",
						"namespace": "default",
					},
					"spec": map[string]any{
						"a": []any{int64(1), int64(2)},
					},
				},
			}))

			res, err = ag.Evaluate(cache.Delta{Type: cache.Deleted, Object: objs[1]})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(1))

			Expect(res[0].Type).To(Equal(cache.Upserted))
			Expect(res[0].Object).To(Equal(&unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": "view.dcontroller.io/v1alpha1",
					"kind":       "view",
					"metadata": map[string]any{
						"name":      "gathered",
						"namespace": "default",
					},
					"spec": map[string]any{
						"a": []any{int64(1)},
					},
				},
			}))

			res, err = ag.Evaluate(cache.Delta{Type: cache.Deleted, Object: objs[0]})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(1))

			Expect(res[0].Type).To(Equal(cache.Deleted))
			Expect(res[0].Object).To(Equal(&unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": "view.dcontroller.io/v1alpha1",
					"kind":       "view",
					"metadata": map[string]any{
						"name":      "gathered",
						"namespace": "default",
					},
					"spec": map[string]any{
						"a": []any{int64(1)},
					},
				},
			}))
		})

		It("should allow a demux followd by a mux", func() {
			jsonData := `
'@aggregate':
  - '@unwind': $.spec.list
  - '@gather':
      - $.metadata.namespace
      - $.spec.list
  - '@project':
      metadata:
        name: "gathered"
        namespace: "default"
      spec: $.spec`
			ag, err := newAggregation(jsonData)
			Expect(err).NotTo(HaveOccurred())

			obj := object.DeepCopy(objs[0])
			Expect(unstructured.SetNestedMap(obj.UnstructuredContent(), map[string]any{},
				"spec")).NotTo(HaveOccurred()) // clean up spec
			Expect(unstructured.SetNestedSlice(obj.UnstructuredContent(), []any{"a", "b"},
				"spec", "list")).NotTo(HaveOccurred())
			res, err := ag.Evaluate(cache.Delta{Type: cache.Added, Object: obj})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(1))
			Expect(res[0].Type).To(Equal(cache.Upserted))

			// unfortunately, the list may come back in any order so we cannot just
			// deepequal
			list, ok, err := unstructured.NestedSlice(res[0].Object.UnstructuredContent(), "spec", "list")
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeTrue())
			Expect(list).To(ConsistOf("a", "b"))
			Expect(unstructured.SetNestedSlice(res[0].Object.UnstructuredContent(), []any{"a", "b"},
				"spec", "list")).NotTo(HaveOccurred())
			Expect(res[0].Object).To(Equal(&unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": "view.dcontroller.io/v1alpha1",
					"kind":       "view",
					"metadata": map[string]any{
						"name":      "gathered",
						"namespace": "default",
					},
					"spec": map[string]any{
						"list": []any{"a", "b"},
					},
				},
			}))

			res, err = ag.Evaluate(cache.Delta{Type: cache.Deleted, Object: obj})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(1))
			Expect(res[0].Type).To(Equal(cache.Deleted))
			Expect(res[0].Object).To(Equal(&unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": "view.dcontroller.io/v1alpha1",
					"kind":       "view",
					"metadata": map[string]any{
						"name":      "gathered",
						"namespace": "default",
					},
					"spec": map[string]any{
						"list": []any{"a", "b"},
					},
				},
			}))
		})

		It("should yield an empty delta for a mux expression using an invalid obj id", func() {
			jsonData := `
'@aggregate':
  - '@gather':
      - $.x.y.z
      - $.spec.a`
			ag, err := newAggregation(jsonData)
			Expect(err).NotTo(HaveOccurred())

			res, err := ag.Evaluate(cache.Delta{Type: cache.Added, Object: objs[0]})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(BeEmpty())
		})

		It("should yield an empty delta for a mux expression using an invalid obj elem", func() {
			jsonData := `
'@aggregate':
  - '@gather':
      - $.metadata.name
      - $.spec.q`
			ag, err := newAggregation(jsonData)
			Expect(err).NotTo(HaveOccurred())

			res, err := ag.Evaluate(cache.Delta{Type: cache.Added, Object: objs[0]})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(BeEmpty())
		})
	})
})

func newAggregation(data string) (Evaluator, error) {
	var a opv1a1.Aggregation
	err := yaml.Unmarshal([]byte(data), &a)
	if err != nil {
		return nil, err
	}
	p, err := NewPipeline(gvk.Kind, []schema.GroupVersionKind{gvk}, opv1a1.Pipeline{Aggregation: &a}, logger)
	if err != nil {
		return nil, err
	}
	return p, nil
}

func sortAnyInt64(l []any) {
	slices.SortFunc(l, func(a, b any) int {
		switch {
		case a.(int64) < b.(int64):
			return -1
		case a.(int64) == b.(int64):
			return 0
		default:
			return 1
		}
	})
}

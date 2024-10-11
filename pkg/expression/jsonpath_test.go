package expression

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/json"

	"hsnlab/dcontroller/pkg/object"
)

var _ = Describe("JSONPath", func() {
	var obj1, obj2 object.Object

	BeforeEach(func() {
		obj1 = object.NewViewObject("testview1")
		object.SetContent(obj1, Unstructured{
			"spec": Unstructured{
				"a": int64(1),
				"b": Unstructured{"c": int64(2)},
				"x": []any{int64(1), int64(2), int64(3), int64(4), int64(5)},
			},
		})
		object.SetName(obj1, "default", "name")

		obj2 = object.NewViewObject("testview2")
		object.SetContent(obj2, Unstructured{
			"metadata": Unstructured{
				"namespace": "default2",
				"name":      "name",
			},
			"spec": []any{
				Unstructured{
					"name": "name1",
					"a":    int64(1),
					"b":    Unstructured{"c": int64(2)},
				}, Unstructured{
					"name": "name2",
					"a":    int64(2),
					"b":    Unstructured{"d": int64(3)},
				},
			},
		})
	})

	Describe("Evaluating JSONPath expressions", func() {
		It("should evaluate a JSONPath expression on Kubernetes object", func() {
			input := &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "testnamespace",
					Name:      "testservice-ok",
				},
				Spec: corev1.ServiceSpec{
					Type:     corev1.ServiceTypeLoadBalancer,
					Selector: map[string]string{"app": "dummy"},
					Ports: []corev1.ServicePort{
						{
							Name:     "udp-ok",
							Protocol: corev1.ProtocolUDP,
							Port:     1,
						},
						{
							Name:     "tcp-ok",
							Protocol: corev1.ProtocolTCP,
							Port:     2,
						},
					},
				},
				Status: corev1.ServiceStatus{
					LoadBalancer: corev1.LoadBalancerStatus{
						Ingress: []corev1.LoadBalancerIngress{{
							IP: "1.2.3.4",
							Ports: []corev1.PortStatus{{
								Port:     1,
								Protocol: corev1.ProtocolUDP,
							}, {
								Port:     2,
								Protocol: corev1.ProtocolTCP,
							}},
						}},
					},
				},
			}

			obj, err := object.NewViewObjectFromNativeObject("Service", input)
			Expect(err).NotTo(HaveOccurred())

			res, err := GetJSONPathExp(`$.metadata.name`, obj.UnstructuredContent())
			Expect(err).NotTo(HaveOccurred())
			s, err := AsString(res)
			Expect(err).NotTo(HaveOccurred())
			Expect(s).To(Equal("testservice-ok"))

			res, err = GetJSONPathExp(`$["metadata"]["namespace"]`, obj.UnstructuredContent())
			Expect(err).NotTo(HaveOccurred())
			s, err = AsString(res)
			Expect(err).NotTo(HaveOccurred())
			Expect(s).To(Equal("testnamespace"))

			res, err = GetJSONPathExp(`$.metadata`, obj.UnstructuredContent())
			Expect(err).NotTo(HaveOccurred())
			d, ok := res.(Unstructured)
			Expect(ok).To(BeTrue())
			Expect(d).To(HaveKey("namespace"))
			Expect(d["namespace"]).To(Equal("testnamespace"))
			Expect(d).To(HaveKey("name"))
			Expect(d["name"]).To(Equal("testservice-ok"))

			res, err = GetJSONPathExp(`$.spec.ports[1].port`, obj.UnstructuredContent())
			Expect(err).NotTo(HaveOccurred())
			i, err := AsInt(res)
			Expect(err).NotTo(HaveOccurred())
			Expect(i).To(Equal(int64(2)))

			res, err = GetJSONPathExp(`$.spec.ports[?(@.name == 'udp-ok')].protocol`,
				obj.UnstructuredContent())
			Expect(err).NotTo(HaveOccurred())
			s, err = AsString(res)
			Expect(err).NotTo(HaveOccurred())
			Expect(s).To(Equal("UDP"))
		})

		It("should evaluate an escaped JSONPath expression on Kubernetes object", func() {
			obj, err := object.NewViewObjectFromNativeObject("Service", &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "testnamespace",
					Name:      "testservice-ok",
				},
				Spec: corev1.ServiceSpec{},
			})
			Expect(err).NotTo(HaveOccurred())

			obj.SetAnnotations(map[string]string{
				"kubernetes.io/service-name":  "example", // actually used for enspointslices
				"kubernetes.io[service-name]": "weirdness",
			})

			// must use the alternative form
			res, err := GetJSONPathExp(`$["metadata"]["annotations"]["kubernetes.io/service-name"]`,
				obj.UnstructuredContent())
			Expect(err).NotTo(HaveOccurred())
			s, err := AsString(res)
			Expect(err).NotTo(HaveOccurred())
			Expect(s).To(Equal("example"))

			res, err = GetJSONPathExp(`$["metadata"]["annotations"]["kubernetes.io[service-name]"]`,
				obj.UnstructuredContent())
			Expect(err).NotTo(HaveOccurred())
			s, err = AsString(res)
			Expect(err).NotTo(HaveOccurred())
			Expect(s).To(Equal("weirdness"))
		})

		It("should evaluate a JSONPath expression", func() {
			jsonPath := "$.metadata.name"
			exp := Expression{Op: "@string", Literal: jsonPath}

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(res).To(Equal("name"))
		})

		It("should deserialize and evaluate an int JSONPath expression", func() {
			jsonData := `"$.spec.a"`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			Expect(exp).To(Equal(Expression{Op: "@string", Literal: "$.spec.a"}))

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(res).To(Equal(int64(1)))
		})

		It("should deserialize and evaluate an alternative form of the int JSONPath expression", func() {
			jsonData := `"$[\"spec\"][\"a\"]"`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			Expect(exp).To(Equal(Expression{Op: "@string", Literal: "$[\"spec\"][\"a\"]"}))

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(res).To(Equal(int64(1)))
		})

		It("should deserialize and evaluate a string JSONPath expression", func() {
			jsonData := `"$.metadata.namespace"`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			Expect(exp).To(Equal(Expression{Op: "@string", Literal: "$.metadata.namespace"}))

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(res).To(Equal("default"))
		})

		It("should deserialize and evaluate a complex JSONPath expression", func() {
			jsonData := `"$.spec.b"`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			Expect(exp).To(Equal(Expression{Op: "@string", Literal: "$.spec.b"}))

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(res).To(Equal(Unstructured{"c": int64(2)}))
		})

		It("should deserialize and evaluate a full JSONPath expression", func() {
			jsonData := `"$"`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			Expect(exp).To(Equal(Expression{Op: "@string", Literal: "$"}))

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(res).To(Equal(Unstructured{
				"apiVersion": "view.dcontroller.io/v1alpha1",
				"kind":       "testview1",
				"metadata": Unstructured{
					"name":      "name",
					"namespace": "default",
				},
				"spec": Unstructured{
					"a": int64(1),
					"b": Unstructured{"c": int64(2)},
					"x": []any{int64(1), int64(2), int64(3), int64(4), int64(5)},
				},
			}))
		})

		It("should deserialize and evaluate a list search JSONPath expression with deref", func() {
			jsonData := `"$.spec[?(@.name == 'name1')].b"`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			Expect(exp).To(Equal(Expression{
				Op:      "@string",
				Literal: "$.spec[?(@.name == 'name1')].b",
			}))

			ctx := EvalCtx{Object: obj2.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(res).To(Equal(Unstructured{"c": int64(2)}))
		})

		It("should deserialize and evaluate a list search JSONPath expression returning a list", func() {
			jsonData := `"$.spec[?(@.name == 'name2')]"`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			ctx := EvalCtx{Object: obj2.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(res).To(Equal(Unstructured{
				"name": "name2",
				"a":    int64(2),
				"b":    Unstructured{"d": int64(3)},
			}))
		})

		It("should deserialize and evaluate a list search JSONPath expression returning a list", func() {
			jsonData := `"$.spec[?(@.name in ['name1', 'name2'])].b.d"` // we return the last match
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			ctx := EvalCtx{Object: obj2.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(res).To(Equal(int64(3)))
		})

		It("should deserialize and evaluate a JSONPath expression used as a key in a map", func() {
			jsonData := `{"$.y[3]":12}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			d, ok := res.(Unstructured)
			Expect(ok).To(BeTrue())
			Expect(d).To(HaveKey("y"))
			Expect(d["y"]).To(Equal([]any{nil, nil, nil, int64(12)}))
		})

		It("should deserialize and evaluate a root JSONPath expression used both as as a key and a value in a map", func() {
			jsonData := `{"$.y.z":"$.spec.b"}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(res).To(Equal(Unstructured{
				"y": Unstructured{
					"z": Unstructured{"c": int64(2)},
				},
			}))
		})

		It("should deserialize and evaluate a JSONPath expression used both as as a key and a value in a map", func() {
			jsonData := `{"q":{"$.y.z":"$.spec.b"}}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(res).To(Equal(Unstructured{
				"q": Unstructured{
					"y": Unstructured{
						"z": Unstructured{"c": int64(2)},
					},
				},
			}))
		})

		It("should deserialize and evaluate a standard root ref JSONPath expression as a right value", func() {
			jsonData := `{"a":"$"}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			d, ok := res.(Unstructured)
			Expect(ok).To(BeTrue())
			Expect(d).To(HaveKey("a"))
			Expect(d["a"]).To(Equal(obj1.UnstructuredContent()))
		})

		It("should deserialize and evaluate a non-standard root ref JSONPath expression as a right value", func() {
			jsonData := `{"a":"$."}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			d, ok := res.(Unstructured)
			Expect(ok).To(BeTrue())
			Expect(d).To(HaveKey("a"))
			Expect(d["a"]).To(Equal(obj1.UnstructuredContent()))
		})

		It("should deserialize and evaluate a root ref JSONPath expression as a left value", func() {
			jsonData := `{"$.":{"a":"b"}}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			d, ok := res.(Unstructured)
			Expect(ok).To(BeTrue())
			Expect(d).To(Equal(map[string]any{"a": "b"}))
		})

		It("should err for a root ref JSONPath expression trying to copy a non-map right value", func() {
			jsonData := `{"$.":"a"}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			_, err = exp.Evaluate(ctx)
			Expect(err).To(HaveOccurred())
		})

		It("should deserialize and evaluate a JSONpath copy expression", func() {
			jsonData := `{"$.": {"a":1}}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			d, ok := res.(Unstructured)
			Expect(ok).To(BeTrue())
			Expect(d).To(Equal(map[string]any{"a": int64(1)}))
		})

		It("should deserialize and evaluate a setter with multiple JSONPath expressions", func() {
			jsonData := `{"$.spec.y":"aaa","$.spec.b.d":12}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			d, ok := res.(Unstructured)
			Expect(ok).To(BeTrue())

			Expect(d).To(HaveKey("spec"))
			Expect(d["spec"]).To(HaveKey("y"))
			Expect(d["spec"].(Unstructured)["y"]).To(Equal("aaa"))

			Expect(d["spec"]).To(HaveKey("b"))
			Expect(d["spec"].(Unstructured)["b"]).To(Equal(map[string]any{"d": int64(12)}))
		})

		It("should deserialize and evaluate a setter with multiple JSONPath expressions", func() {
			jsonData := `{"$.spec.y":"aaa","$.spec.b.c":"$.spec.b.c","$.spec.b.d":12}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			d, ok := res.(Unstructured)
			Expect(ok).To(BeTrue())

			Expect(d).To(HaveKey("spec"))
			Expect(d["spec"]).To(HaveKey("y"))
			Expect(d["spec"].(Unstructured)["y"]).To(Equal("aaa"))

			Expect(d["spec"]).To(HaveKey("b"))
			Expect(d["spec"].(Unstructured)["b"]).To(Equal(map[string]any{"c": int64(2), "d": int64(12)}))
		})

		It("should create and evaluate a JSONPath getter expression constructor", func() {
			exp := NewJSONPathGetExpression("$.spec.b.c")
			res, err := exp.Evaluate(EvalCtx{Object: obj1.UnstructuredContent(), Log: logger})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(int64(2)))
		})

		It("should create and evaluate a JSONPath setter expression constructor", func() {
			exp, err := NewJSONPathSetExpression("$.spec.b.d", "aaa")
			Expect(err).NotTo(HaveOccurred())
			res, err := exp.Evaluate(EvalCtx{Object: obj1.UnstructuredContent(), Log: logger})
			Expect(err).NotTo(HaveOccurred())
			obj, err := AsObject(res)
			Expect(err).NotTo(HaveOccurred())
			s, ok, err := unstructured.NestedString(obj, "spec", "b", "d")
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeTrue())
			Expect(s).To(Equal("aaa"))
		})
	})
})

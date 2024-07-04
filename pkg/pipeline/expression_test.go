package pipeline

import (
	"encoding/json"
	"reflect"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/jsonpath"

	"hsnlab/dcontroller-runtime/pkg/object"
)

var _ = Describe("Expressions", func() {
	var state = State{
		Object: object.New("view").WithName("default", "name").
			WithContent(map[string]any{"spec": map[string]any{"a": 1, "b": map[string]any{"c": 2}}}),
		Log: logger,
	}

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

	})

	Describe("Evaluating JSONpath expressions", func() {
		It("should evaluate a JSONpath expression on Kubernetes object", func() {
			input := corev1.Service{
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

			jsonPath := "{.metadata.name}"

			jsonExp, err := RelaxedJSONPathExpression(jsonPath)
			Expect(err).NotTo(HaveOccurred())

			j := jsonpath.New("JSONpathParser")
			err = j.Parse(jsonExp)
			Expect(err).NotTo(HaveOccurred())

			values, err := j.FindResults(input)
			Expect(err).NotTo(HaveOccurred())

			Expect(values).To(HaveLen(1))
			Expect(values[0]).To(HaveLen(1))
			Expect(values[0][0].Kind()).To(Equal(reflect.String))
			Expect(values[0][0].String()).To(Equal("testservice-ok"))
		})

		It("should evaluate a JSONpath expression", func() {
			jsonPath := "$.metadata.name"
			exp := Expression{Op: "@string", Literal: jsonPath, Raw: jsonPath}
			res, err := exp.EvalStringExp(state, jsonPath)
			Expect(err).NotTo(HaveOccurred())

			Expect(res).To(Equal("name"))
		})

		It("should deserialize and evaluate an int JSONpath expression", func() {
			jsonData := `"$.spec.a"`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			Expect(exp).To(Equal(Expression{Op: "@string", Literal: "$.spec.a", Raw: jsonData}))

			res, err := exp.Evaluate(state)
			Expect(err).NotTo(HaveOccurred())

			Expect(res).To(Equal(1))
		})

		It("should deserialize and evaluate a string JSONpath expression", func() {
			jsonData := `"$.apiVersion"`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			Expect(exp).To(Equal(Expression{Op: "@string", Literal: "$.apiVersion", Raw: jsonData}))

			res, err := exp.Evaluate(state)
			Expect(err).NotTo(HaveOccurred())

			Expect(res).To(Equal("dcontroller.github.io/v1alpha1"))
		})

		It("should deserialize and evaluate a complex JSONpath expression", func() {
			jsonData := `"$.spec.b"`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			Expect(exp).To(Equal(Expression{Op: "@string", Literal: "$.spec.b", Raw: jsonData}))

			res, err := exp.Evaluate(state)
			Expect(err).NotTo(HaveOccurred())

			Expect(res).To(Equal(map[string]any{"c": 2}))
		})

		It("should deserialize and evaluate a full JSONpath expression", func() {
			jsonData := `"$."`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			Expect(exp).To(Equal(Expression{Op: "@string", Literal: "$.", Raw: jsonData}))

			res, err := exp.Evaluate(state)
			Expect(err).NotTo(HaveOccurred())

			Expect(res).To(Equal(map[string]any{
				"apiVersion": "dcontroller.github.io/v1alpha1",
				"kind":       "view",
				"metadata": map[string]any{
					"name":      "name",
					"namespace": "default",
				},
				"spec": map[string]any{
					"a": 1,
					"b": map[string]any{"c": 2},
				},
			}))
		})
	})

	Describe("Evaluating compound expressions", func() {
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

		It("should deserialize and evaluate a compound JSONpath expression", func() {
			jsonData := `{"@lt": ["$.spec.a", "$.spec.b.c"]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			Expect(exp).To(Equal(Expression{
				Op: "@lt",
				Args: []Expression{
					{Op: "@string", Args: nil, Raw: "\"$.spec.a\"", Literal: "$.spec.a"},
					{Op: "@string", Args: nil, Raw: "\"$.spec.b.c\"", Literal: "$.spec.b.c"},
				},
				Raw: jsonData,
			}))

			res, err := exp.Evaluate(state)
			Expect(err).NotTo(HaveOccurred())
			Expect(reflect.ValueOf(res).Kind()).To(Equal(reflect.Bool))
			Expect(reflect.ValueOf(res).Bool()).To(Equal(true))
		})

	})
})

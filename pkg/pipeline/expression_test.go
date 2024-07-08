package pipeline

import (
	"encoding/json"
	"fmt"
	"reflect"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/jsonpath"

	"hsnlab/dcontroller-runtime/pkg/object"
)

var _ = Describe("Expressions", func() {
	var state = &State{
		Object: object.New("view").WithName("default", "name").
			WithContent(map[string]any{"spec": map[string]any{
				"a": int64(1),
				"b": map[string]any{"c": int64(2)},
				"x": []any{int64(1), int64(2), int64(3), int64(4), int64(5)},
			}}),
		Log: logger,
	}

	var state2 = &State{
		Object: object.New("view").WithName("default", "name").
			WithContent(map[string]any{"spec": []any{
				map[string]any{
					"name": "name1",
					"a":    int64(1),
					"b":    map[string]any{"c": int64(2)},
				}, map[string]any{
					"name": "name2",
					"a":    int64(2),
					"b":    map[string]any{"d": int64(3)},
				}}}),
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
			Expect(exp).To(Equal(Expression{Op: "@int", Literal: int64(10), Raw: jsonData}))

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

			Expect(res).To(Equal(int64(1)))
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

			Expect(res).To(Equal(map[string]any{"c": int64(2)}))
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
					"a": int64(1),
					"b": map[string]any{"c": int64(2)},
					"x": []any{int64(1), int64(2), int64(3), int64(4), int64(5)},
				},
			}))
		})

		It("should deserialize and evaluate a list search JSONpath expression with deref", func() {
			jsonData := `"$.spec[?(@.name == 'name1')].b"`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			Expect(exp).To(Equal(Expression{
				Op:      "@string",
				Literal: "$.spec[?(@.name == 'name1')].b",
				Raw:     jsonData,
			}))

			res, err := exp.Evaluate(state2)
			Expect(err).NotTo(HaveOccurred())

			Expect(res).To(Equal(map[string]any{"c": int64(2)}))
		})

		It("should deserialize and evaluate a list search JSONpath expression returning a list", func() {
			jsonData := `"$.spec[?(@.name == 'name2')]"`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			res, err := exp.Evaluate(state2)
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(map[string]any{
				"name": "name2",
				"a":    int64(2),
				"b":    map[string]any{"d": int64(3)},
			}))
		})

		It("should deserialize and evaluate a list search JSONpath expression returning a list", func() {
			jsonData := `"$.spec[?(@.name in ['name1', 'name2'])].b.d"` // we return the last match
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			res, err := exp.Evaluate(state2)
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(int64(3)))
		})

		// FIt("should deserialize and evaluate a list search JSONpath key", func() {
		// 	jsonData := `{"spec":{"$.[?(@.name == 'name2')].name":"name3"}}`
		// 	var exp Expression
		// 	err := json.Unmarshal([]byte(jsonData), &exp)
		// 	Expect(err).NotTo(HaveOccurred())
		// 	Expect(exp).To(Equal(Expression{
		// 		Op: "@dict",
		// 		Literal: map[string]Expression{
		// 			"spec": {
		// 				Op: "@dict",
		// 				Literal: map[string]Expression{
		// 					"$.[?(@.name == 'name2')].name": {
		// 						Op:      "@string",
		// 						Literal: "name3",
		// 						Raw:     "\"name3\"",
		// 					},
		// 				},
		// 				Raw: "{\"$.[?(@.name == 'name2')].name\":\"name3\"}",
		// 			},
		// 		},
		// 		Raw: "{\"spec\":{\"$.[?(@.name == 'name2')].name\":\"name3\"}}",
		// 	}))
		// 	res, err := exp.Evaluate(state2)
		// 	Expect(err).NotTo(HaveOccurred())
		// 	Expect(res).To(Equal(map[string]any{
		// 		"spec":
		// 		"name": "name3",
		// 		"a":    int64(2),
		// 		"b":    map[string]any{"d": int64(3)},
		// 	}))
		// })
	})

	Describe("Evaluating compound expressions", func() {
		It("should deserialize and evaluate a compound literal expression", func() {
			jsonData := `{"@eq": [10, 10]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			Expect(exp).To(Equal(Expression{
				Op: "@eq",
				Arg: &Expression{
					Op: "@list",
					Literal: []Expression{
						{Op: "@int", Literal: int64(10), Raw: "10"},
						{Op: "@int", Literal: int64(10), Raw: "10"},
					},
					Raw: "[10, 10]",
				},
				Raw: jsonData,
			}))

			res, err := exp.Evaluate(state)
			Expect(err).NotTo(HaveOccurred())
			Expect(reflect.ValueOf(res).Kind()).To(Equal(reflect.Bool),
				fmt.Sprintf("kind mismatch: %v != %v",
					reflect.ValueOf(res).Kind(), reflect.Int64))
			Expect(reflect.ValueOf(res).Bool()).To(Equal(true))
		})

		It("should deserialize and err for a malformed expression", func() {
			jsonData := `{"@eq":10}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			Expect(exp).To(Equal(Expression{
				Op: "@eq",
				Arg: &Expression{
					Op:      "@int",
					Literal: int64(10),
					Raw:     "10",
				},
				Raw: jsonData,
			}))

			_, err = exp.Evaluate(state)
			Expect(err).To(HaveOccurred())
		})

		It("should deserialize and evaluate a multi-level compound literal expression", func() {
			jsonData := `{"@and": [{"@eq": [10, 10]}, {"@lt": [1, 2]}]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			Expect(exp).To(Equal(Expression{
				Op: "@and",
				Arg: &Expression{
					Op: "@list",
					Literal: []Expression{{
						Op: "@eq",
						Arg: &Expression{
							Op: "@list",
							Literal: []Expression{
								{Op: "@int", Literal: int64(10), Raw: "10"},
								{Op: "@int", Literal: int64(10), Raw: "10"},
							},
							Raw: "[10, 10]",
						},
						Raw: `{"@eq": [10, 10]}`,
					}, {
						Op: "@lt",
						Arg: &Expression{
							Op: "@list",
							Literal: []Expression{
								{Op: "@int", Literal: int64(1), Raw: "1"},
								{Op: "@int", Literal: int64(2), Raw: "2"},
							},
							Raw: "[1, 2]",
						},
						Raw: `{"@lt": [1, 2]}`,
					}},
					Raw: "[{\"@eq\": [10, 10]}, {\"@lt\": [1, 2]}]",
				},
				Raw: jsonData,
			}))

			res, err := exp.Evaluate(state)
			Expect(err).NotTo(HaveOccurred())
			Expect(reflect.ValueOf(res).Kind()).To(Equal(reflect.Bool))
			Expect(reflect.ValueOf(res).Bool()).To(Equal(true))
		})

		It("should deserialize and evaluate a compound bool expression", func() {
			jsonData := `{"@bool":{"@eq":[10,10]}}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			Expect(exp).To(Equal(Expression{
				Op: "@bool",
				Arg: &Expression{
					Op: "@eq",
					Arg: &Expression{
						Op: "@list",
						Literal: []Expression{
							{Op: "@int", Literal: int64(10), Raw: "10"},
							{Op: "@int", Literal: int64(10), Raw: "10"},
						},
						Raw: "[10,10]",
					},
					Raw: `{"@eq":[10,10]}`,
				},
				Raw: jsonData,
			}))

			res, err := exp.Evaluate(state)
			Expect(err).NotTo(HaveOccurred())
			Expect(reflect.ValueOf(res).Kind()).To(Equal(reflect.Bool))
			Expect(reflect.ValueOf(res).Bool()).To(Equal(true))
		})

		It("should deserialize and evaluate a compound bool expression", func() {
			jsonData := `{"@bool":{"@or":[false, true, true, false]}}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			res, err := exp.Evaluate(state)
			Expect(err).NotTo(HaveOccurred())
			Expect(reflect.ValueOf(res).Kind()).To(Equal(reflect.Bool))
			Expect(reflect.ValueOf(res).Bool()).To(Equal(true))
		})

		It("should deserialize and evaluate a compound list expression", func() {
			jsonData := `{"@list":[{"@eq":[10,10]},{"@and":[true,false]}]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			res, err := exp.Evaluate(state)
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal([]any{true, false}))
		})

		It("should deserialize and evaluate a compound map expression", func() {
			jsonData := `{"@dict":{"x":{"a":1,"b":2}}}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			res, err := exp.Evaluate(state)
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(map[string]any{"x": map[string]any{"a": int64(1), "b": int64(2)}}))
		})

		It("should deserialize and evaluate a multi-level compound literal expression", func() {
			jsonData := `{"@and": [{"@eq": [10, 10]}, {"@lt": [1, 2]}]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
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
			res, err := exp.Evaluate(state)
			Expect(err).NotTo(HaveOccurred())
			Expect(reflect.ValueOf(res).Kind()).To(Equal(reflect.Bool))
			Expect(reflect.ValueOf(res).Bool()).To(Equal(true))
		})

		It("should deserialize and evaluate a compound JSONpath expression", func() {
			//jsonData := `{"@eq": [{"@len": "$.spec.x"}, 5]}`
			jsonData := `{"@eq": [{"@len": ["$.spec.x"]}, 5]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			//Expect(exp).To(Equal("aaaaaaaaa"))
			res, err := exp.Evaluate(state)
			Expect(err).NotTo(HaveOccurred())
			Expect(reflect.ValueOf(res).Kind()).To(Equal(reflect.Bool))
			Expect(reflect.ValueOf(res).Bool()).To(Equal(true))
		})

		It("should deserialize and evaluate a JSONpath list expression", func() {
			jsonData := `{"@eq": [{"@len": ["$.spec.x"]}, 5]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			//Expect(exp).To(Equal("aaaaaaaaa"))
			res, err := exp.Evaluate(state)
			Expect(err).NotTo(HaveOccurred())
			Expect(reflect.ValueOf(res).Kind()).To(Equal(reflect.Bool))
			Expect(reflect.ValueOf(res).Bool()).To(Equal(true))
		})

		It("should deserialize and evaluate a JSONpath list expression without the arg being a list", func() {
			jsonData := `{"@eq": [{"@last": "$.spec.x"}, 5]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			res, err := exp.Evaluate(state)
			Expect(err).NotTo(HaveOccurred())
			Expect(reflect.ValueOf(res).Kind()).To(Equal(reflect.Bool))
			Expect(reflect.ValueOf(res).Bool()).To(Equal(true))
		})
	})

	Describe("Evaluating list expressions", func() {
		It("should deserialize and evaluate a simple list expression", func() {
			jsonData := `{"@first":[1,2,3]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			res, err := exp.Evaluate(state)
			Expect(err).NotTo(HaveOccurred())
			kind := reflect.ValueOf(res).Kind()
			Expect(kind == reflect.Int64).To(BeTrue(),
				fmt.Sprintf("kind mismatch: %v != %v", kind, reflect.Int64))
			Expect(res).To(Equal(int64(1)))
		})

		It("should deserialize and evaluate a stacked list expression", func() {
			jsonData := `{"@list":[1,2,3]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			res, err := exp.Evaluate(state)
			Expect(err).NotTo(HaveOccurred())
			kind := reflect.ValueOf(res).Kind()
			Expect(kind == reflect.Slice || kind == reflect.Array).To(BeTrue(),
				fmt.Sprintf("kind mismatch: %v != %v", kind, reflect.Int64))
			Expect(res).To(Equal([]any{int64(1), int64(2), int64(3)}))
		})

		It("should deserialize and evaluate a simple list expression", func() {
			jsonData := `{"@last":[1,2,3]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			res, err := exp.Evaluate(state)
			Expect(err).NotTo(HaveOccurred())
			kind := reflect.ValueOf(res).Kind()
			Expect(kind == reflect.Int64).To(BeTrue())
			Expect(res).To(Equal(int64(3)))
		})

		It("should deserialize and evaluate a simple list expression", func() {
			jsonData := `{"@sum":[1,2,3]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			res, err := exp.Evaluate(state)
			Expect(err).NotTo(HaveOccurred())
			kind := reflect.ValueOf(res).Kind()
			Expect(kind == reflect.Int64).To(BeTrue())
			Expect(res).To(Equal(int64(6)))
		})

		It("should deserialize and evaluate a simple list expression", func() {
			jsonData := `{"@len":[1,2,3]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			Expect(exp).To(Equal(Expression{
				Op: "@len",
				Arg: &Expression{
					Op: "@list",
					Literal: []Expression{
						{Op: "@int", Raw: "1", Literal: int64(1)},
						{Op: "@int", Raw: "2", Literal: int64(2)},
						{Op: "@int", Raw: "3", Literal: int64(3)},
					},
					Raw: "[1,2,3]",
				},
				Raw: jsonData,
			}))

			res, err := exp.Evaluate(state)
			Expect(err).NotTo(HaveOccurred())
			kind := reflect.ValueOf(res).Kind()
			Expect(kind == reflect.Int64).To(BeTrue())
			Expect(res).To(Equal(int64(3)))
		})

		It("should deserialize and evaluate a literal list expression", func() {
			jsonData := `{"dummy":[1,2,3]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			Expect(exp).To(Equal(Expression{
				Op: "@dict",
				Literal: map[string]Expression{
					"dummy": Expression{
						Op: "@list",
						Literal: []Expression{
							{Op: "@int", Raw: "1", Literal: int64(1)},
							{Op: "@int", Raw: "2", Literal: int64(2)},
							{Op: "@int", Raw: "3", Literal: int64(3)},
						},
						Raw: "[1,2,3]",
					},
				},
				Raw: jsonData,
			}))

			res, err := exp.Evaluate(state)
			Expect(err).NotTo(HaveOccurred())
			kind := reflect.ValueOf(res).Kind()
			Expect(kind == reflect.Map).To(BeTrue(),
				fmt.Sprintf("%q is not a map", kind))
			Expect(reflect.ValueOf(res).Interface().(map[string]any)).
				To(Equal(map[string]any{"dummy": []any{int64(1), int64(2), int64(3)}}))
		})

		It("should deserialize and evaluate a compound list expression", func() {
			jsonData := `{"dummy":[1,2,{"@sub":[6,3]}]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			res, err := exp.Evaluate(state)
			Expect(err).NotTo(HaveOccurred())
			kind := reflect.ValueOf(res).Kind()
			Expect(kind == reflect.Map).To(BeTrue(),
				fmt.Sprintf("%q is not a map", kind))
			Expect(reflect.ValueOf(res).Interface().(map[string]any)).
				To(Equal(map[string]any{"dummy": []any{int64(1), int64(2), int64(3)}}))
		})

		It("should deserialize and evaluate a literal list expression with multiple keys", func() {
			jsonData := `{"dummy":[1,2,3],"another-dummy":"a"}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			res, err := exp.Evaluate(state)
			Expect(err).NotTo(HaveOccurred())
			kind := reflect.ValueOf(res).Kind()
			Expect(kind == reflect.Map).To(BeTrue(),
				fmt.Sprintf("%q is not a map", kind))
			Expect(reflect.ValueOf(res).Interface().(map[string]any)).
				To(Equal(map[string]any{"dummy": []any{int64(1), int64(2), int64(3)}, "another-dummy": "a"}))
		})

		It("should deserialize and evaluate a mixed list expression", func() {
			jsonData := `{"dummy":[1,2,3],"x":{"@eq":["a","b"]}}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			// Expect(exp).To(Equal("x"))
			res, err := exp.Evaluate(state)
			Expect(err).NotTo(HaveOccurred())
			kind := reflect.ValueOf(res).Kind()
			Expect(kind == reflect.Map).To(BeTrue(),
				fmt.Sprintf("%q is not a map", kind))
			Expect(res).To(Equal(map[string]any{"dummy": []any{int64(1), int64(2), int64(3)}, "x": false}))
		})

		It("should deserialize and evaluate a compound list expression", func() {
			jsonData := `{"another-dummy":[{"a":1,"b":2.2},{"x":[1,2,3]}]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			res, err := exp.Evaluate(state)
			Expect(err).NotTo(HaveOccurred())
			kind := reflect.ValueOf(res).Kind()
			Expect(kind == reflect.Map).To(BeTrue(),
				fmt.Sprintf("%q is not a map", kind))
			Expect(reflect.ValueOf(res).Interface().(map[string]any)).
				To(Equal(map[string]any{"another-dummy": []any{
					map[string]any{"a": int64(1), "b": 2.2},
					map[string]any{"x": []any{int64(1), int64(2), int64(3)}},
				}}))
		})
	})

	Describe("Evaluating literal map expressions", func() {
		It("should deserialize and evaluate a constant literal map expression", func() {
			jsonData := `{"a":1, "b":{"c":"x"}}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			Expect(exp).To(Equal(Expression{
				Op: "@dict",
				Literal: map[string]Expression{
					"a": Expression{Op: "@int", Raw: "1", Literal: int64(1)},
					"b": Expression{
						Op:  "@dict",
						Raw: "{\"c\":\"x\"}",
						Literal: map[string]Expression{
							"c": Expression{Op: "@string", Raw: "\"x\"", Literal: "x"},
						},
					},
				},
				Raw: jsonData,
			}))

			res, err := exp.Evaluate(state)
			Expect(err).NotTo(HaveOccurred())
			Expect(reflect.ValueOf(res).Kind()).To(Equal(reflect.Map))
			Expect(res).To(Equal(map[string]any{"a": int64(1), "b": map[string]any{"c": "x"}}))
		})

		It("should deserialize and evaluate a compound literal map expression", func() {
			jsonData := `{"a":1.1,"b":{"@sum":[1,2]},"c":{"@concat":["ab","ba"]}}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			Expect(exp.Op).To(Equal("@dict"))
			Expect(reflect.ValueOf(exp.Literal).MapIndex(reflect.ValueOf("a")).Interface().(Expression)).
				To(Equal(Expression{Op: "@float", Raw: "1.1", Literal: 1.1}))
			Expect(reflect.ValueOf(exp.Literal).MapIndex(reflect.ValueOf("b")).Interface().(Expression)).
				To(Equal(Expression{
					Op: "@sum",
					Arg: &Expression{
						Op: "@list",
						Literal: []Expression{
							{Op: "@int", Raw: "1", Literal: int64(1)},
							{Op: "@int", Raw: "2", Literal: int64(2)},
						},
						Raw: "[1,2]",
					},
					Raw: `{"@sum":[1,2]}`,
				}))
			Expect(reflect.ValueOf(exp.Literal).MapIndex(reflect.ValueOf("c")).Interface().(Expression)).
				To(Equal(Expression{
					Op: "@concat",
					Arg: &Expression{
						Op: "@list",
						Literal: []Expression{
							{Op: "@string", Raw: `"ab"`, Literal: "ab"},
							{Op: "@string", Raw: `"ba"`, Literal: "ba"},
						},
						Raw: "[\"ab\",\"ba\"]",
					},
					Raw: `{"@concat":["ab","ba"]}`,
				}))

			res, err := exp.Evaluate(state)
			Expect(err).NotTo(HaveOccurred())
			Expect(reflect.ValueOf(res).Kind()).To(Equal(reflect.Map))
			Expect(res).To(Equal(map[string]any{"a": 1.1, "b": int64(3), "c": "abba"}))
		})
	})

	Describe("Evaluating cornercases", func() {
		// this should err but it doesn't: maybe it is right that it does not fail?
		// It("should deserialize and err for a malformed map expression", func() {
		// 	jsonData := `{"dummy":1.1,"@eq":["a","b"]}`
		// 	var exp Expression
		// 	err := json.Unmarshal([]byte(jsonData), &exp)
		// 	Expect(err).NotTo(HaveOccurred())
		// 	//Expect(exp).To(Equal("x"))
		// 	res, err := exp.Evaluate(state)
		// 	Expect(err).NotTo(HaveOccurred())
		// 	kind := reflect.ValueOf(res).Kind()
		// 	Expect(kind == reflect.Map).To(BeTrue(),
		// 		fmt.Sprintf("%q is not a map", kind))
		// 	Expect(res).To(Equal(map[string]any{"dummy": []any{int64(1), int64(2), int64(3)}}))
		// })

		It("should deserialize and evaluate complex artithmetic expressions", func() {
			jsonData := `{"@mul":[{"@sum":[1,4]}, {"@sub":[{"@div":[10,2]},1]}]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			res, err := exp.Evaluate(state)
			Expect(err).NotTo(HaveOccurred())
			Expect(reflect.ValueOf(res).Kind()).To(Equal(reflect.Int64))
			Expect(res).To(Equal(int64(20)))
		})

		It("should deserialize and evaluate complex numeric expressions with incompatible types", func() {
			jsonData := `{"@mul":[{"@sum":[1,4]}, {"@sub":[{"@div":[10,2.5]},1]}]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			res, err := exp.Evaluate(state)
			Expect(err).NotTo(HaveOccurred())
			Expect(reflect.ValueOf(res).Kind()).To(Equal(reflect.Float64))
			Expect(res).To(Equal(float64(15)))
		})

		It("should deserialize and evaluate an expression inside a literal map", func() {
			jsonData := `{"a":1,"b":{"c":{"@eq":[1,1]}}}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			res, err := exp.Evaluate(state)
			Expect(err).NotTo(HaveOccurred())
			Expect(reflect.ValueOf(res).Kind()).To(Equal(reflect.Map))
			Expect(res).To(Equal(map[string]any{"a": int64(1), "b": map[string]any{"c": true}}))
		})

		It("should deserialize and err for simple invalid comparisons", func() {
			jsonData := `{"@add":[1,"b"]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			_, err = exp.Evaluate(state)
			Expect(err).To(HaveOccurred())
		})

		It("should deserialize and err for compex invalid comparisons", func() {
			jsonData := `{"@eq":[{"@concat":["ab","ba"]}, {"@mul":[1,1]}]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			_, err = exp.Evaluate(state)
			Expect(err).To(HaveOccurred())
		})
	})
})

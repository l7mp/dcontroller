package expression

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/grokify/mogo/encoding/base36"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap/zapcore"
	"k8s.io/apimachinery/pkg/util/json"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/yaml"

	"github.com/l7mp/dcontroller/pkg/object"
)

var (
	loglevel = -10
	logger   = zap.New(zap.UseFlagOptions(&zap.Options{
		Development:     true,
		DestWriter:      GinkgoWriter,
		StacktraceLevel: zapcore.Level(3),
		TimeEncoder:     zapcore.RFC3339NanoTimeEncoder,
		Level:           zapcore.Level(loglevel),
	}))
)

func TestPipeline(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Expression")
}

var _ = Describe("Expressions", func() {
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

	Describe("Evaluating terminal expressions", func() {
		It("should deserialize and evaluate a bool literal expression", func() {
			jsonData := "true"
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			Expect(exp).To(Equal(Expression{Op: "@bool", Literal: true}))

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(reflect.ValueOf(res).Kind()).To(Equal(reflect.Bool))
			Expect(reflect.ValueOf(res).Bool()).To(BeTrue())
		})

		It("should deserialize and evaluate an integer literal expression", func() {
			jsonData := "10"
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			Expect(exp).To(Equal(Expression{Op: "@int", Literal: int64(10)}))

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(reflect.ValueOf(res).Kind()).To(Equal(reflect.Int64))
			Expect(reflect.ValueOf(res).Int()).To(Equal(int64(10)))
		})

		It("should deserialize and evaluate a float literal expression", func() {
			jsonData := "10.12"
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			Expect(exp).To(Equal(Expression{Op: "@float", Literal: 10.12}))

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(reflect.ValueOf(res).Kind()).To(Equal(reflect.Float64))
			Expect(reflect.ValueOf(res).Float()).To(Equal(10.12))
		})

		It("should deserialize and evaluate a string literal expression", func() {
			jsonData := `"a10"`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			Expect(exp).To(Equal(Expression{Op: "@string", Literal: "a10"}))

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(reflect.ValueOf(res).Kind()).To(Equal(reflect.String))
			Expect(reflect.ValueOf(res).String()).To(Equal("a10"))
		})

		It("should deserialize and evaluate a @now expression", func() {
			jsonData := `"@now"`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			Expect(exp).To(Equal(Expression{
				Op: "@now",
			}))

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(res).NotTo(BeNil())
			Expect(reflect.ValueOf(res).Kind()).To(Equal(reflect.String))

			t, err := time.Parse(time.RFC3339, res.(string))
			Expect(err).NotTo(HaveOccurred())
			Expect(t).To(BeTemporally("~", time.Now(), time.Second))
		})
	})

	Describe("Evaluating compound expressions", func() {
		It("should deserialize and evaluate a nested @now expression", func() {
			jsonData := `{"time": "@now"}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			Expect(exp).To(Equal(Expression{
				Op: "@dict",
				Literal: map[string]Expression{
					"time": {
						Op: "@now",
					},
				},
			}))

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(res).NotTo(BeNil())
			Expect(res).To(HaveKey("time"))

			t, err := time.Parse(time.RFC3339, res.(map[string]any)["time"].(string))
			Expect(err).NotTo(HaveOccurred())
			Expect(t).To(BeTemporally("~", time.Now(), time.Second))
		})

		It("should deserialize and evaluate a nil expression", func() {
			jsonData := `{"@isnil": 1}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			Expect(exp).To(Equal(Expression{
				Op:  "@isnil",
				Arg: &Expression{Op: "@int", Literal: int64(1)},
			}))

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(reflect.ValueOf(res).Kind()).To(Equal(reflect.Bool))
			Expect(reflect.ValueOf(res).Bool()).To(BeFalse())
		})

		It("should deserialize and evaluate an @exists expression", func() {
			jsonData := `{"@exists": "$.metadata.annotations.ann"}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			Expect(exp).To(Equal(Expression{
				Op:  "@exists",
				Arg: &Expression{Op: "@string", Literal: "$.metadata.annotations.ann"},
			}))

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(reflect.ValueOf(res).Kind()).To(Equal(reflect.Bool))
			Expect(reflect.ValueOf(res).Bool()).To(BeFalse())
		})

		It("should deserialize and evaluate a bool expression", func() {
			jsonData := `{"@not": false}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			Expect(exp).To(Equal(Expression{
				Op:  "@not",
				Arg: &Expression{Op: "@bool", Literal: false},
			}))

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(reflect.ValueOf(res).Kind()).To(Equal(reflect.Bool))
			Expect(reflect.ValueOf(res).Bool()).To(BeTrue())
		})

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
						{Op: "@int", Literal: int64(10)},
						{Op: "@int", Literal: int64(10)},
					},
				},
			}))

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(reflect.ValueOf(res).Kind()).To(Equal(reflect.Bool),
				fmt.Sprintf("kind mismatch: %v != %v",
					reflect.ValueOf(res).Kind(), reflect.Int64))
			Expect(reflect.ValueOf(res).Bool()).To(BeTrue())
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
				},
			}))

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			_, err = exp.Evaluate(ctx)
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
								{Op: "@int", Literal: int64(10)},
								{Op: "@int", Literal: int64(10)},
							},
						},
					}, {
						Op: "@lt",
						Arg: &Expression{
							Op: "@list",
							Literal: []Expression{
								{Op: "@int", Literal: int64(1)},
								{Op: "@int", Literal: int64(2)},
							},
						},
					}},
				},
			}))

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(reflect.ValueOf(res).Kind()).To(Equal(reflect.Bool))
			Expect(reflect.ValueOf(res).Bool()).To(BeTrue())
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
							{Op: "@int", Literal: int64(10)},
							{Op: "@int", Literal: int64(10)},
						},
					},
				},
			}))

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(reflect.ValueOf(res).Kind()).To(Equal(reflect.Bool))
			Expect(reflect.ValueOf(res).Bool()).To(BeTrue())
		})

		It("should deserialize and evaluate a compound bool expression", func() {
			jsonData := `{"@bool":{"@or":[false, true, true, false]}}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(reflect.ValueOf(res).Kind()).To(Equal(reflect.Bool))
			Expect(reflect.ValueOf(res).Bool()).To(BeTrue())
		})

		It("should deserialize and evaluate a compound list expression", func() {
			jsonData := `{"@list":[{"@eq":[10,10]},{"@and":[true,false]}]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal([]any{true, false}))
		})

		It("should deserialize and evaluate a compound map expression", func() {
			jsonData := `{"@dict":{"x":{"a":1,"b":2}}}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(Unstructured{"x": Unstructured{"a": int64(1), "b": int64(2)}}))
		})

		It("should deserialize and evaluate a multi-level compound literal expression", func() {
			jsonData := `{"@and": [{"@eq": [10, 10]}, {"@lt": [1, 2]}]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(reflect.ValueOf(res).Kind()).To(Equal(reflect.Bool))
			Expect(reflect.ValueOf(res).Bool()).To(BeTrue())
		})

		It("should shortcut a boolean @and expression", func() {
			jsonData := `{"@and":[true, {"@eq": [10, 11]}, "$.$$"]}` // last arg blows up
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(reflect.ValueOf(res).Kind()).To(Equal(reflect.Bool))
			Expect(reflect.ValueOf(res).Bool()).To(BeFalse())
		})

		It("should shortcut a boolean @or expression", func() {
			jsonData := `{"@or":[false, {"@eq": [10, 10]}, "$.$$"]}` // last arg blows up
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(reflect.ValueOf(res).Kind()).To(Equal(reflect.Bool))
			Expect(reflect.ValueOf(res).Bool()).To(BeTrue())
		})

		It("should deserialize and evaluate a compound JSONPath expression", func() {
			jsonData := `{"@lt": ["$.spec.a", "$.spec.b.c"]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(reflect.ValueOf(res).Kind()).To(Equal(reflect.Bool))
			Expect(reflect.ValueOf(res).Bool()).To(BeTrue())
		})

		It("should deserialize and evaluate a compound JSONPath expression", func() {
			jsonData := `{"@eq": [{"@len": ["$.spec.x"]}, 5]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(reflect.ValueOf(res).Kind()).To(Equal(reflect.Bool))
			Expect(reflect.ValueOf(res).Bool()).To(BeTrue())
		})

		It("should deserialize and evaluate a JSONPath list expression", func() {
			jsonData := `{"@eq": [{"@len": ["$.spec.x"]}, 5]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(reflect.ValueOf(res).Kind()).To(Equal(reflect.Bool))
			Expect(reflect.ValueOf(res).Bool()).To(BeTrue())
		})

		It("should deserialize and evaluate a simple list expression", func() {
			jsonData := `{"@sum":[1,2,3]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			kind := reflect.ValueOf(res).Kind()
			Expect(kind).To(Equal(reflect.Int64))
			Expect(res).To(Equal(int64(6)))
		})

		It("should deserialize and evaluate an @in expression", func() {
			jsonData := `{"@in":[1,[1,2,3]]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			kind := reflect.ValueOf(res).Kind()
			Expect(kind).To(Equal(reflect.Bool))
			Expect(res).To(BeTrue())
		})

		It("should deserialize and evaluate an @in expression", func() {
			jsonData := `{"@in":[4,[1,2,3]]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			kind := reflect.ValueOf(res).Kind()
			Expect(kind).To(Equal(reflect.Bool))
			Expect(res).To(BeFalse())
		})

		It("should deserialize and evaluate an @in expression", func() {
			jsonData := `{"@in":["a",[1,2,3]]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			kind := reflect.ValueOf(res).Kind()
			Expect(kind).To(Equal(reflect.Bool))
			Expect(res).To(BeFalse())
		})

		It("should deserialize and evaluate an @in expression", func() {
			jsonData := `{"@in":["nginx",["apache","nginx","nginx"]]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			kind := reflect.ValueOf(res).Kind()
			Expect(kind).To(Equal(reflect.Bool))
			Expect(res).To(BeTrue())
		})
	})

	Describe("Evaluating list expressions", func() {
		It("should deserialize and evaluate a stacked list expression", func() {
			jsonData := `{"@list":[1,2,3]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			kind := reflect.ValueOf(res).Kind()
			Expect(kind == reflect.Slice || kind == reflect.Array).To(BeTrue(),
				fmt.Sprintf("kind mismatch: %v != %v", kind, reflect.Int64))
			Expect(res).To(Equal([]any{int64(1), int64(2), int64(3)}))
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
						{Op: "@int", Literal: int64(1)},
						{Op: "@int", Literal: int64(2)},
						{Op: "@int", Literal: int64(3)},
					},
				},
			}))

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			kind := reflect.ValueOf(res).Kind()
			Expect(kind).To(Equal(reflect.Int64))
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
					"dummy": {
						Op: "@list",
						Literal: []Expression{
							{Op: "@int", Literal: int64(1)},
							{Op: "@int", Literal: int64(2)},
							{Op: "@int", Literal: int64(3)},
						},
					},
				},
			}))

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			kind := reflect.ValueOf(res).Kind()
			Expect(kind).To(Equal(reflect.Map))
			Expect(reflect.ValueOf(res).Interface().(Unstructured)).
				To(Equal(Unstructured{"dummy": []any{int64(1), int64(2), int64(3)}}))
		})

		It("should deserialize and evaluate a literal list expression with multiple keys", func() {
			jsonData := `{"dummy":[1,2,3],"another-dummy":"a"}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			kind := reflect.ValueOf(res).Kind()
			Expect(kind).To(Equal(reflect.Map))
			Expect(reflect.ValueOf(res).Interface().(Unstructured)).
				To(Equal(Unstructured{"dummy": []any{int64(1), int64(2), int64(3)}, "another-dummy": "a"}))
		})

		It("should deserialize and evaluate a mixed list expression", func() {
			jsonData := `{"dummy":[1,2,3],"x":{"@eq":["a","b"]}}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			kind := reflect.ValueOf(res).Kind()
			Expect(kind).To(Equal(reflect.Map))
			Expect(res).To(Equal(Unstructured{"dummy": []any{int64(1), int64(2), int64(3)}, "x": false}))
		})

		It("should deserialize and evaluate a compound list expression", func() {
			jsonData := `{"another-dummy":[{"a":1,"b":2.2},{"x":[1,2,3]}]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			kind := reflect.ValueOf(res).Kind()
			Expect(kind).To(Equal(reflect.Map))
			Expect(reflect.ValueOf(res).Interface().(Unstructured)).
				To(Equal(Unstructured{"another-dummy": []any{
					Unstructured{"a": int64(1), "b": 2.2},
					Unstructured{"x": []any{int64(1), int64(2), int64(3)}},
				}}))
		})
	})

	// Describe("Evaluating @merge", func() {
	// 	It("should merge basic objects", func() {
	// 		for _, tc := range []struct {
	// 			js  string
	// 			res any
	// 		}{
	// 			{
	// 				js:  `{"@merge":[1, true]}`,
	// 				res: true,
	// 			},
	// 			{
	// 				js:  `{"@merge":[1, 2]}`,
	// 				res: int64(2),
	// 			},
	// 			{
	// 				js:  `{"@merge":[1, 2.1]}`,
	// 				res: float64(2.1),
	// 			},
	// 			{
	// 				js:  `{"@merge":[1, "a"]}`,
	// 				res: "a",
	// 			},
	// 			{
	// 				js:  `{"@merge":[1, ["a","b"]]}`,
	// 				res: []any{"a", "b"},
	// 			},
	// 			{
	// 				js:  `{"@merge":[["a","b"],["c","d"]]}`,
	// 				res: []any{"a", "b", "c", "d"},
	// 			},
	// 			{
	// 				js: `{"@merge":[{"a":"aaa"},{"a":{"d":12}}]}`,
	// 				res: map[string]any{
	// 					"a": map[string]any{"d": int64(12)},
	// 				},
	// 			},
	// 			{
	// 				js: `{"@merge":[["a","aaa"],{"a":{"d":12}}]}`,
	// 				res: map[string]any{
	// 					"a": map[string]any{"d": int64(12)},
	// 				},
	// 			},
	// 			{
	// 				js: `{"@merge":[{"a":"aaa"},{"b":{"d":12}}]}`,
	// 				res: map[string]any{
	// 					"a": "aaa",
	// 					"b": map[string]any{"d": int64(12)},
	// 				},
	// 			},
	// 		} {
	// 			var exp Expression
	// 			err := json.Unmarshal([]byte(tc.js), &exp)
	// 			Expect(err).NotTo(HaveOccurred())

	// 			res, err := exp.Evaluate(EvalCtx{Object: nil, Log: logger})
	// 			Expect(err).NotTo(HaveOccurred())
	// 			Expect(res).To(Equal(tc.res))
	// 		}
	// 	})

	// 	It("should merge two setters with a JSONPath setter", func() {
	// 		jsonData := `{"@merge":[{"spec":1},{"spec":{"a":"aaa"}},{"$.spec.b.d":12}]}`
	// 		var exp Expression
	// 		err := json.Unmarshal([]byte(jsonData), &exp)
	// 		Expect(err).NotTo(HaveOccurred())

	// 		ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
	// 		res, err := exp.Evaluate(ctx)
	// 		Expect(err).NotTo(HaveOccurred())

	// 		d, ok := res.(Unstructured)
	// 		Expect(ok).To(BeTrue())

	// 		Expect(d).To(HaveKey("spec"))
	// 		Expect(d["spec"]).To(HaveKey("a"))
	// 		Expect(d["spec"].(Unstructured)["a"]).To(Equal("aaa"))

	// 		Expect(d["spec"]).To(HaveKey("b"))
	// 		Expect(d["spec"].(Unstructured)["b"]).To(Equal(map[string]any{"d": int64(12)}))
	// 	})

	// 	It("should merge two JSONPath setters that modify the same field", func() {
	// 		jsonData := `{"@merge":[{"spec":[1,2]},{"$.spec":{"@sum":"$.spec"}},{"$.spec":{"@concat":["spec-","$.spec"]}}]}`
	// 		var exp Expression
	// 		err := json.Unmarshal([]byte(jsonData), &exp)
	// 		Expect(err).NotTo(HaveOccurred())

	// 		ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
	// 		res, err := exp.Evaluate(ctx)
	// 		Expect(err).NotTo(HaveOccurred())

	// 		d, ok := res.(Unstructured)
	// 		Expect(ok).To(BeTrue())

	// 		Expect(d).To(HaveKey("spec"))
	// 		Expect(d["spec"]).To(Equal("spec-3"))
	// 	})
	// })

	Describe("Evaluating label selectors", func() {
		It("should evaluate a @selector expression on a literal labelset", func() {
			jsonData := `{"@selector":[{"matchLabels":{"app":"nginx"}},{"app":"nginx"}]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			v, err := AsBool(res)
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(BeTrue())
		})

		It("should evaluate a @selector expression using the short form", func() {
			jsonData := `{"@selector":[{"app":"nginx"},{"app":"nginx"}]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			v, err := AsBool(res)
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(BeTrue())
		})

		It("should evaluate a @selector expression on an object's labels", func() {
			jsonData := `{"@selector":[{"app":"nginx"},"$.metadata.labels"]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			obj := object.DeepCopy(obj1)
			obj.SetLabels(map[string]string{"app": "nginx"})
			ctx := EvalCtx{Object: obj.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			v, err := AsBool(res)
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(BeTrue())

			obj.SetLabels(map[string]string{"app": "apache"})
			ctx = EvalCtx{Object: obj.UnstructuredContent(), Log: logger}
			res, err = exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			v, err = AsBool(res)
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(BeFalse())
		})

		It("should evaluate a @selector containing a matchLabels expression on an object's labels", func() {
			jsonData := `{"@selector":[{"matchLabels":{"app":"nginx"}},"$.metadata.labels"]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			obj := object.DeepCopy(obj1)
			obj.SetLabels(map[string]string{"app": "nginx"})
			ctx := EvalCtx{Object: obj.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			v, err := AsBool(res)
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(BeTrue())

			obj.SetLabels(map[string]string{"app": "apache"})
			ctx = EvalCtx{Object: obj.UnstructuredContent(), Log: logger}
			res, err = exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			v, err = AsBool(res)
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(BeFalse())
		})

		It("should evaluate a @selector expression with a complex match expression", func() {
			jsonData := `{"@selector":[{"matchExpressions":[{"key":"app","operator":"In","values":["nginx","httpd"]}]},"$.metadata.labels"]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			obj := object.DeepCopy(obj1)
			obj.SetLabels(map[string]string{"app": "nginx"})
			ctx := EvalCtx{Object: obj.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			v, err := AsBool(res)
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(BeTrue())

			obj.SetLabels(map[string]string{"app": "apache"})
			ctx = EvalCtx{Object: obj.UnstructuredContent(), Log: logger}
			res, err = exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			v, err = AsBool(res)
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(BeFalse())
		})

		It("should evaluate a @selector expression on an object's labels", func() {
			jsonData := `{"@selector":[{"matchLabels":{"app":"nginx"},"matchExpressions":[{"key":"env","operator":"In","values":["production", "staging"]},{"key":"version","operator": "Exists"}]},"$.metadata.labels"]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			obj := object.DeepCopy(obj1)
			obj.SetLabels(map[string]string{"app": "nginx", "env": "production", "version": "v2"})
			ctx := EvalCtx{Object: obj.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())
			v, err := AsBool(res)
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(BeTrue())

			obj.SetLabels(map[string]string{"app": "apache", "env": "production", "version": "v2"})
			ctx = EvalCtx{Object: obj.UnstructuredContent(), Log: logger}
			res, err = exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())
			v, err = AsBool(res)
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(BeFalse())

			obj.SetLabels(map[string]string{"app": "nginx"})
			ctx = EvalCtx{Object: obj.UnstructuredContent(), Log: logger}
			res, err = exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())
			v, err = AsBool(res)
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(BeFalse())

			obj.SetLabels(map[string]string{"app": "nginx", "env": "staging", "version": "v3"})
			ctx = EvalCtx{Object: obj.UnstructuredContent(), Log: logger}
			res, err = exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())
			v, err = AsBool(res)
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(BeTrue())
		})

		It("should evaluate a @hash expression", func() {
			jsonData := `{"@hash":"$.spec"}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())
			v, err := AsString(res)
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(Equal("dorzm8"))

			js, err := json.Marshal(obj1.Object["spec"])
			Expect(err).NotTo(HaveOccurred())
			h := base36.Md5Base36(string(js))
			Expect(v).To(Equal(h[0:6]))

			obj := object.DeepCopy(obj1)
			obj.Object["spec"].(map[string]any)["a"] = int64(2)

			ctx = EvalCtx{Object: obj.UnstructuredContent(), Log: logger}
			res, err = exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())
			v, err = AsString(res)
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(Equal("9pxacx"))

			js, err = json.Marshal(obj.Object["spec"])
			Expect(err).NotTo(HaveOccurred())
			h = base36.Md5Base36(string(js))
			Expect(v).To(Equal(h[0:6]))
		})
	})

	Describe("Evaluating conditional commands", func() {
		// @cond
		It("should evaluate a basic true @cond expression", func() {
			jsonData := `{"@cond":[true, 1, 2]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			v, err := AsInt(res)
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(Equal(int64(1)))
		})

		It("should evaluate a basic false @cond expression", func() {
			jsonData := `{"@cond":[false, 1, 2]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			v, err := AsInt(res)
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(Equal(int64(2)))
		})

		It("should evaluate a true @cond expression and shortcut", func() {
			jsonData := `{"@cond":[{"@eq":["$.spec.a", 1]},"$.spec.b.c","$.$$"]}` // "else" branch blows up
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			v, err := AsInt(res)
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(Equal(int64(2)))
		})

		It("should evaluate a true @cond expression and shortcut", func() {
			jsonData := `{"@cond":[{"@eq":["$.spec.a", "x"]},"$.$$","$.spec.b.c"]}` // "true" branch blows up
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			v, err := AsInt(res)
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(Equal(int64(2)))
		})

		It("should evaluate a complex @cond expression", func() {
			jsonData := `{"@cond":[{"@eq":["$.spec.a",2]},"$.$$",{"@cond":[{"@eq":["$.metadata.name","name"]},"$.spec.a","$.$$"]}]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			v, err := AsInt(res)
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(Equal(int64(1)))
		})

		// @definedOr
		It("should evaluate a basic true @definedOr expression", func() {
			jsonData := `{"@definedOr":[1, 2]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			v, err := AsInt(res)
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(Equal(int64(1)))
		})

		It("should evaluate a basic false @definedOr expression", func() {
			jsonData := `{"@definedOr":["$.dummy", 2]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			v, err := AsInt(res)
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(Equal(int64(2)))
		})

		It("should evaluate a true @definedOr expression using JSONpath", func() {
			jsonData := `{"@definedOr":["$.metadata.namespace", 2]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			v, err := AsString(res)
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(Equal("default"))
		})

		It("should evaluate a false @definedOr expression using JSONpath", func() {
			jsonData := `{"@definedOr":["$.dummy", "$.metadata.namespace"]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			v, err := AsString(res)
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(Equal("default"))
		})
	})

	Describe("Evaluating list commands", func() {
		// @filter
		It("should evaluate a @filter expression on a literal list", func() {
			jsonData := `{"@filter":[{"@eq": ["$$",12]}, [12, 23]]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			vs, err := AsList(res)
			Expect(err).NotTo(HaveOccurred())
			Expect(vs).To(Equal([]any{int64(12)}))
		})

		It("should evaluate a @filter expression with a non-standard root ref on a literal list", func() {
			jsonData := `{"@filter":[{"@eq": ["$$.",12]}, [12, 23]]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			vs, err := AsList(res)
			Expect(err).NotTo(HaveOccurred())
			Expect(vs).To(Equal([]any{int64(12)}))
		})

		It("should evaluate a @filter expression with an object and a local context ", func() {
			jsonData := `{"@filter":[{"@eq":["$.metadata.namespace","$$.metadata.namespace"]},[{"metadata":{"namespace":"default"}},{"metadata":{"namespace":"default2"}},12]]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			vs, err := AsList(res)
			Expect(err).NotTo(HaveOccurred())
			Expect(vs).To(Equal([]any{Unstructured{"metadata": Unstructured{"namespace": "default"}}}))
		})

		// @map
		It("should evaluate a @map expression on a literal list", func() {
			jsonData := `{"@map":[{"x": 1}, [12, 23]]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			vs, err := AsList(res)
			Expect(err).NotTo(HaveOccurred())
			Expect(vs).To(Equal([]any{map[string]any{"x": int64(1)}, map[string]any{"x": int64(1)}}))
		})

		It("should evaluate a @map expression over an arithmetic expression on a literal list", func() {
			jsonData := `{"@map":[{"@lte":["$$",17]}, [12, 23]]}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			vs, err := AsList(res)
			Expect(err).NotTo(HaveOccurred())
			Expect(vs).To(Equal([]any{true, false}))
		})

		It("should evaluate a @map expression generating a list", func() {
			jsonData := `
'@filter':
  - '@exists': "$$"
  - "$.spec.x"`
			var exp Expression
			err := yaml.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			vs, err := AsList(res)
			Expect(err).NotTo(HaveOccurred())
			Expect(vs).To(Equal([]any{int64(1), int64(2), int64(3), int64(4), int64(5)}))
		})

		// It("should evaluate stacked @map expressions", func() {
		// 	jsonData := `{"@map":[{"@lte":["$",2]},{"@first":[{"@map":["$.spec.x"]}]}]}`
		// 	var exp Expression
		// 	err := json.Unmarshal([]byte(jsonData), &exp)
		// 	Expect(err).NotTo(HaveOccurred())

		// 	res, err := exp.Evaluate(eng3)
		// 	Expect(err).NotTo(HaveOccurred())

		// 	vs, err := asList(res)
		// 	Expect(err).NotTo(HaveOccurred())
		// 	Expect(vs).To(Equal([]any{true, true, false, false, false}))
		// })

		// It("should evaluate compound @map plus @filter expressions", func() {
		// 	jsonData := `{"@filter":[{"@lte":["$",2]},{"@first":[{"@map":["$.spec.x"]}]}]}`
		// 	var exp Expression
		// 	err := json.Unmarshal([]byte(jsonData), &exp)
		// 	Expect(err).NotTo(HaveOccurred())

		// 	res, err := exp.Evaluate(eng3)
		// 	Expect(err).NotTo(HaveOccurred())

		// 	vs, err := asList(res)
		// 	Expect(err).NotTo(HaveOccurred())
		// 	Expect(vs).To(Equal([]any{int64(1), int64(2)}))
		// })

		// // @fold
		// It("should evaluate a simple @fold expression", func() {
		// 	jsonData := `{"@fold":[{"@filter":{"@eq":["$.metadata.namespace","default2"]}},` +
		// 		`{"@map":{"$.metadata.name":"$.metadata.namespace"}}]}`
		// 	var exp Expression
		// 	err := json.Unmarshal([]byte(jsonData), &exp)
		// 	Expect(err).NotTo(HaveOccurred())

		// 	res, err := exp.Evaluate(eng3)
		// 	Expect(err).NotTo(HaveOccurred())

		// 	objs, err := asObjectList(eng3.view, res)
		// 	Expect(err).NotTo(HaveOccurred())
		// 	Expect(objs).To(HaveLen(1))
		// 	Expect(objs[0]).To(Equal(map[string]any{
		// 		"metadata": map[string]any{"name": "default2"},
		// 	}))
		// })

		// It("should evaluate an expression that folds multiple objects", func() {
		// 	jsonData := `{"@len":{"@map":"$"}}`
		// 	var exp Expression
		// 	err := json.Unmarshal([]byte(jsonData), &exp)
		// 	Expect(err).NotTo(HaveOccurred())

		// 	res, err := exp.Evaluate(eng3)
		// 	Expect(err).NotTo(HaveOccurred())
		// 	Expect(res).To(Equal(int64(2)))
		// })

		// It("should evaluate an expression that folds multiple objects into a valid object", func() {
		// 	jsonData := `{"metadata":{"name":"some-name"},"len":{"@len":{"@map":"$"}}}`
		// 	var exp Expression
		// 	err := json.Unmarshal([]byte(jsonData), &exp)
		// 	Expect(err).NotTo(HaveOccurred())

		// 	res, err := exp.Evaluate(eng3)
		// 	Expect(err).NotTo(HaveOccurred())
		// 	Expect(res).To(Equal(map[string]any{
		// 		"metadata": map[string]any{"name": "some-name"},
		// 		"len":      int64(2),
		// 	}))
		// })
	})

	Describe("Evaluating literal @dict expressions", func() {
		It("should deserialize and evaluate a constant literal map expression", func() {
			jsonData := `{"a":1, "b":{"c":"x"}}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())
			Expect(exp).To(Equal(Expression{
				Op: "@dict",
				Literal: map[string]Expression{
					"a": {Op: "@int", Literal: int64(1)},
					"b": {
						Op: "@dict",
						Literal: map[string]Expression{
							"c": {Op: "@string", Literal: "x"},
						},
					},
				},
			}))

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(reflect.ValueOf(res).Kind()).To(Equal(reflect.Map))
			Expect(res).To(Equal(Unstructured{"a": int64(1), "b": Unstructured{"c": "x"}}))
		})

		It("should deserialize and evaluate a compound literal map expression", func() {
			jsonData := `{"a":1.1,"b":{"@sum":[1,2]},"c":{"@concat":["ab","ba"]}}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			Expect(exp.Op).To(Equal("@dict"))
			Expect(reflect.ValueOf(exp.Literal).MapIndex(reflect.ValueOf("a")).Interface().(Expression)).
				To(Equal(Expression{Op: "@float", Literal: 1.1}))
			Expect(reflect.ValueOf(exp.Literal).MapIndex(reflect.ValueOf("b")).Interface().(Expression)).
				To(Equal(Expression{
					Op: "@sum",
					Arg: &Expression{
						Op: "@list",
						Literal: []Expression{
							{Op: "@int", Literal: int64(1)},
							{Op: "@int", Literal: int64(2)},
						},
					},
				}))
			Expect(reflect.ValueOf(exp.Literal).MapIndex(reflect.ValueOf("c")).Interface().(Expression)).
				To(Equal(Expression{
					Op: "@concat",
					Arg: &Expression{
						Op: "@list",
						Literal: []Expression{
							{Op: "@string", Literal: "ab"},
							{Op: "@string", Literal: "ba"},
						},
					},
				}))

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(reflect.ValueOf(res).Kind()).To(Equal(reflect.Map))
			Expect(res).To(Equal(Unstructured{"a": 1.1, "b": int64(3), "c": "abba"}))
		})
	})

	Describe("Evaluating cornercases", func() {
		It("should deserialize and evaluate an expression inside a literal map", func() {
			jsonData := `{"a":1,"b":{"c":{"@eq":[1,1]}}}`
			var exp Expression
			err := json.Unmarshal([]byte(jsonData), &exp)
			Expect(err).NotTo(HaveOccurred())

			ctx := EvalCtx{Object: obj1.UnstructuredContent(), Log: logger}
			res, err := exp.Evaluate(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(reflect.ValueOf(res).Kind()).To(Equal(reflect.Map))
			Expect(res).To(Equal(Unstructured{"a": int64(1), "b": Unstructured{"c": true}}))
		})
	})
})

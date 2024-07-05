package pipeline

import (
	"reflect"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Converters", func() {
	// dummy expression for testing
	var exp = Expression{Op: "test-op", Arg: &Expression{Literal: "<test-literal>"}, Raw: "<test-expression>"}
	Describe("Bool conversion", func() {
		It("should read a false bool", func() {
			var x any = false
			v, err := exp.asBool(x)
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(Equal(false))
		})
		It("should read a true bool", func() {
			var x any = true
			v, err := exp.asBool(x)
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(Equal(true))
		})
		It("should err for invalid bool", func() {
			var x any = 12
			_, err := exp.asBool(x)
			Expect(err).To(HaveOccurred())
		})
		It("should read a bool list", func() {
			var xs any = []any{false, true, true}
			vs, err := exp.asBoolList(xs)
			Expect(err).NotTo(HaveOccurred())
			Expect(vs).To(Equal([]bool{false, true, true}))
		})
		It("should err for invalid bool list", func() {
			var xs any = []any{false, 12, "a"}
			_, err := exp.asBoolList(xs)
			Expect(err).To(HaveOccurred())
		})
		It("should read a binary bool list", func() {
			var xs any = []any{false, true}
			vs, err := exp.asBinaryBoolList(xs)
			Expect(err).NotTo(HaveOccurred())
			Expect(vs).To(Equal([]bool{false, true}))
		})
		It("should err for invalid binary bool list", func() {
			var xs any = []any{false, true, false}
			_, err := exp.asBinaryBoolList(xs)
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("String conversion", func() {
		It("should read a string", func() {
			var x any = "foo"
			v, err := exp.asString(x)
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(Equal("foo"))
		})
		It("should err for invalid string", func() {
			var x any = 12
			_, err := exp.asString(x)
			Expect(err).To(HaveOccurred())
		})
		It("should read a string list", func() {
			var xs any = []any{"a", "x", "12"}
			vs, err := exp.asStringList(xs)
			Expect(err).NotTo(HaveOccurred())
			Expect(vs).To(Equal([]string{"a", "x", "12"}))
		})
		It("should err for invalid string list", func() {
			var xs any = []any{false, 12, "a"}
			_, err := exp.asStringList(xs)
			Expect(err).To(HaveOccurred())
		})
		It("should read a binary string list", func() {
			var xs any = []any{"a", "b"}
			vs, err := exp.asBinaryStringList(xs)
			Expect(err).NotTo(HaveOccurred())
			Expect(vs).To(Equal([]string{"a", "b"}))
		})
		It("should err for invalid binary string list", func() {
			var xs any = []any{"a", "b", "c"}
			_, err := exp.asBinaryStringList(xs)
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("Int conversion", func() {
		It("should read a int", func() {
			var x any = 12
			v, err := exp.asInt(x)
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(Equal(int64(12)))
		})
		It("should read a int64", func() {
			var x any = int64(12)
			v, err := exp.asInt(x)
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(Equal(int64(12)))
		})
		It("should err for float", func() {
			var x any = 12.23
			_, err := exp.asInt(x)
			Expect(err).To(HaveOccurred())
		})
		It("should err for invalid int", func() {
			var x any = "as"
			_, err := exp.asInt(x)
			Expect(err).To(HaveOccurred())
		})
		It("should read an int list", func() {
			var xs any = []any{12, 24, 36}
			vs, err := exp.asIntList(xs)
			Expect(err).NotTo(HaveOccurred())
			Expect(vs).To(Equal([]int64{12, 24, 36}))
		})
		It("should err for invalid int list", func() {
			var xs any = []any{false, 12, "a"}
			_, err := exp.asIntList(xs)
			Expect(err).To(HaveOccurred())
		})
		It("should read a binary int list", func() {
			var xs any = []any{12, 24}
			vs, err := exp.asBinaryIntList(xs)
			Expect(err).NotTo(HaveOccurred())
			Expect(vs).To(Equal([]int64{12, 24}))
		})
		It("should err for invalid binary int list", func() {
			var xs any = []any{12, 24, 36}
			_, err := exp.asBinaryIntList(xs)
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("Float conversion", func() {
		It("should read an int", func() {
			var x any = 12
			v, err := exp.asFloat(x)
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(Equal(float64(12)))
		})
		It("should read a float", func() {
			var x any = 12.12
			v, err := exp.asFloat(x)
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(Equal(float64(12.12)))
		})
		It("should read a float64", func() {
			var x any = float64(12.21)
			v, err := exp.asFloat(x)
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(Equal(float64(12.21)))
		})
		It("should err for invalid float", func() {
			var x any = "as"
			_, err := exp.asFloat(x)
			Expect(err).To(HaveOccurred())
		})
		It("should err for invalid float", func() {
			var x any = map[string]int{"a": 1}
			_, err := exp.asFloat(x)
			Expect(err).To(HaveOccurred())
		})
		It("should read an float list", func() {
			var xs any = []any{12.12, 24, -36.12}
			vs, err := exp.asFloatList(xs)
			Expect(err).NotTo(HaveOccurred())
			Expect(vs).To(Equal([]float64{12.12, 24, -36.12}))
		})
		It("should err for invalid float list", func() {
			var xs any = []any{false, 12, "a"}
			_, err := exp.asFloatList(xs)
			Expect(err).To(HaveOccurred())
		})
		It("should read a binary float list", func() {
			var xs any = []any{12.12, 24}
			vs, err := exp.asBinaryFloatList(xs)
			Expect(err).NotTo(HaveOccurred())
			Expect(vs).To(Equal([]float64{12.12, 24}))
		})
		It("should err for invalid binary float list", func() {
			var xs any = []any{12, 24, 36.21}
			_, err := exp.asBinaryFloatList(xs)
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("Int-or-float conversion", func() {
		It("should read an int", func() {
			var x any = 12
			v, _, kind, err := exp.asIntOrFloat(x)
			Expect(err).NotTo(HaveOccurred())
			Expect(kind).To(Equal(reflect.Int64))
			Expect(v).To(Equal(int64(12)))
		})
		It("should read a float", func() {
			var x any = 12.12
			_, v, kind, err := exp.asIntOrFloat(x)
			Expect(err).NotTo(HaveOccurred())
			Expect(kind).To(Equal(reflect.Float64))
			Expect(v).To(Equal(float64(12.12)))
		})
		It("should err for invalid int or float", func() {
			var x any = "as"
			_, _, _, err := exp.asIntOrFloat(x)
			Expect(err).To(HaveOccurred())
		})
		It("should read an int list", func() {
			var xs any = []any{12, 24, -36}
			vs, _, kind, err := exp.asIntOrFloatList(xs)
			Expect(err).NotTo(HaveOccurred())
			Expect(kind).To(Equal(reflect.Int64))
			Expect(vs).To(Equal([]int64{12, 24, -36}))
		})
		It("should read a float list", func() {
			var xs any = []any{12.1, 24.1, -36.1}
			_, vs, kind, err := exp.asIntOrFloatList(xs)
			Expect(err).NotTo(HaveOccurred())
			Expect(kind).To(Equal(reflect.Float64))
			Expect(vs).To(Equal([]float64{12.1, 24.1, -36.1}))
		})
		It("should read a mixed list", func() {
			var xs any = []any{12, 24, -36.1}
			_, vs, kind, err := exp.asIntOrFloatList(xs)
			Expect(err).NotTo(HaveOccurred())
			Expect(kind).To(Equal(reflect.Float64))
			Expect(vs).To(Equal([]float64{12, 24, -36.1}))
		})
		It("should err for invalid list", func() {
			var xs any = []any{false, 12, "a"}
			_, _, _, err := exp.asIntOrFloatList(xs)
			Expect(err).To(HaveOccurred())
		})
		It("should read a binary float list", func() {
			var xs any = []any{12.12, 24}
			_, vs, kind, err := exp.asBinaryIntOrFloatList(xs)
			Expect(err).NotTo(HaveOccurred())
			Expect(kind).To(Equal(reflect.Float64))
			Expect(vs).To(Equal([]float64{12.12, 24}))
		})
		It("should err for invalid binary float list", func() {
			var xs any = []any{12, 24, 36.21}
			_, _, _, err := exp.asBinaryIntOrFloatList(xs)
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("Generic lists", func() {
		It("should read a generic list with homogeneous elems", func() {
			var xs any = []any{false, true, true}
			vs, err := exp.asList(xs)
			Expect(err).NotTo(HaveOccurred())
			Expect(vs).To(Equal([]any{false, true, true}))
		})
		It("should read a generic list with heteroneeous elems", func() {
			var xs any = []any{true, 1, "a"}
			vs, err := exp.asList(xs)
			Expect(err).NotTo(HaveOccurred())
			Expect(vs).To(Equal([]any{true, 1, "a"}))
		})
		It("should err for invalid list", func() {
			var xs any = 12
			_, err := exp.asList(xs)
			Expect(err).To(HaveOccurred())
		})
		It("should err for int list", func() {
			var xs any = []int{12, 24}
			_, err := exp.asList(xs)
			Expect(err).To(HaveOccurred())
		})
		It("should err for nil", func() {
			var xs any = nil
			_, err := exp.asList(xs)
			Expect(err).To(HaveOccurred())
		})
	})

})

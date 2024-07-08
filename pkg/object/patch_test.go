package object

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Object patching", func() {
	Describe("Basic patching", func() {
		It("simple patch should work", func() {
			a := map[string]any{
				"a": int64(1),
				"b": int64(1),
				"c": true,
				"d": map[string]any{
					"x": int64(10),
					"y": "world",
				},
				"e": []any{int64(1), map[string]any{"three": int64(3)}, map[string]any{"three": int64(3)}},
			}

			b := map[string]any{
				"b": "updated",
				"c": false,
				"d": map[string]any{
					"y": "patched",
					"z": int64(20),
				},
				"e": []any{int64(4), "five", map[string]any{"six": int64(6)}, 3.14},
				"f": 3.14,
			}

			p := patch(a, b)
			m, ok := p.(map[string]any)
			Expect(ok).To(BeTrue())
			Expect(m).To(Equal(map[string]any{
				"a": int64(1),
				"b": "updated",
				"c": false,
				"d": map[string]any{
					"x": int64(10),
					"y": "patched",
					"z": int64(20),
				},
				"e": []any{int64(4), "five", map[string]any{"three": int64(3), "six": int64(6)}, 3.14},
				"f": 3.14,
			}))
		})
		It("deepCopy should work", func() {
			a := map[string]any{"map": int64(1)}
			b := map[string]any{"map": map[string]any{
				"b": "updated",
				"c": false,
				"d": map[string]any{
					"y": "patched",
					"z": int64(20),
				},
				"e": []any{int64(4), "five", map[string]any{"six": int64(6)}, 3.14},
				"f": 3.14,
			}}

			p := patch(a, b)
			m, ok := p.(map[string]any)
			Expect(ok).To(BeTrue())
			Expect(m).To(Equal(map[string]any{"map": map[string]any{
				"b": "updated",
				"c": false,
				"d": map[string]any{
					"y": "patched",
					"z": int64(20),
				},
				"e": []any{int64(4), "five", map[string]any{"six": int64(6)}, 3.14},
				"f": 3.14,
			}}))
		})

		It("list merging should work", func() {
			a := map[string]any{"map": []any{int64(4), "five", map[string]any{"three": int64(3), "six": int64(8)}, 3.14}}
			b := map[string]any{"map": []any{int64(4), "nine", map[string]any{"six": int64(6)}}}

			p := patch(a, b)
			m, ok := p.(map[string]any)
			Expect(ok).To(BeTrue())
			Expect(m).To(Equal(map[string]any{"map": []any{
				int64(4), "nine", map[string]any{"three": int64(3), "six": int64(6)}, 3.14,
			}}))
		})
	})

	Describe("Object patching", func() {
		It("should patch nil with map", func() {
			obj := New("view").WithContent(map[string]any{})
			patch := map[string]any{"a": int64(1), "b": map[string]any{"c": int64(2)}}
			err := obj.Patch(patch)
			Expect(err).NotTo(HaveOccurred())
			Expect(obj.UnstructuredContent()).To(Equal(map[string]any{
				"apiVersion": "dcontroller.github.io/v1alpha1",
				"kind":       "view",
				"a":          int64(1),
				"b":          map[string]any{"c": int64(2)},
			}))
		})

		It("should patch a literal with map", func() {
			obj := New("view")
			patch := map[string]any{"a": int64(1), "b": map[string]any{"c": int64(2)}}
			err := obj.Patch(patch)
			Expect(err).NotTo(HaveOccurred())
			Expect(obj.UnstructuredContent()).To(Equal(map[string]any{
				"apiVersion": "dcontroller.github.io/v1alpha1",
				"kind":       "view",
				"a":          int64(1),
				"b":          map[string]any{"c": int64(2)},
			}))
		})

		It("should patch a map with map", func() {
			obj := New("view").WithContent(map[string]any{"a": "x", "d": 1.1, "e": []any{int64(10), int64(20)}})
			patch := map[string]any{"a": []any{int64(10), int64(2), int64(3)}, "b": map[string]any{"c": int64(2), "e": "y"}}
			err := obj.Patch(patch)
			Expect(err).NotTo(HaveOccurred())
			Expect(obj.UnstructuredContent()).To(Equal(map[string]any{
				"apiVersion": "dcontroller.github.io/v1alpha1",
				"kind":       "view",
				"a":          []any{int64(10), int64(2), int64(3)},
				"b":          map[string]any{"c": int64(2), "e": "y"},
				"d":          float64(1.1),
				"e":          []any{int64(10), int64(20)},
			}))
		})
	})
})

package object

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Object patching", func() {
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

	// It("should patch a map with map", func() {
	// 	obj := New("view").WithContent(map[string]any{"a": "x", "d": 1.1, "e": []int64{10, 20}})
	// 	patch := map[string]any{"a": []int64{1, 2}, "b": map[string]any{"c": int64(2), "e": "y"}}
	// 	err := obj.Patch(patch)
	// 	Expect(err).NotTo(HaveOccurred())
	// 	Expect(obj.UnstructuredContent()).To(Equal(map[string]any{
	// 		"apiVersion": "dcontroller.github.io/v1alpha1",
	// 		"kind":       "view",
	// 		"a":          []int64{1, 2},
	// 		"b":          map[string]any{"c": int64(2)},
	// 		"d":          float64(1.1),
	// 		"e":          "y",
	// 	}))
	// })
})

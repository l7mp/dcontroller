package object

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestManager(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Object")
}

var _ = Describe("Object", func() {
	It("deepequal", func() {
		obj1 := NewViewObject("view1")
		obj2 := NewViewObject("view2")

		Expect(DeepEqual(obj1, obj2)).To(BeFalse())
		Expect(DeepEqual(obj1, obj1)).To(BeTrue())
		Expect(DeepEqual(obj2, obj2)).To(BeTrue())
	})

	It("setcontent", func() {
		obj := NewViewObject("view")
		SetContent(obj, map[string]any{"a": "x"})
		Expect(obj.UnstructuredContent()).To(Equal(map[string]any{
			"apiVersion": "view.dcontroller.io/v1alpha1",
			"kind":       "view",
			"a":          "x",
		}))
	})

	It("setname 1", func() {
		obj := NewViewObject("view")
		SetContent(obj, map[string]any{"a": "x"})
		SetName(obj, "ns", "obj")

		Expect(obj.UnstructuredContent()).To(Equal(map[string]any{
			"apiVersion": "view.dcontroller.io/v1alpha1",
			"kind":       "view",
			"metadata": map[string]any{
				"namespace": "ns",
				"name":      "obj",
			},
			"a": "x",
		}))
	})

	It("setname 2", func() {
		obj := NewViewObject("view")
		SetName(obj, "ns", "obj")
		SetContent(obj, map[string]any{"a": "x"})

		Expect(obj.UnstructuredContent()).To(Equal(map[string]any{
			"apiVersion": "view.dcontroller.io/v1alpha1",
			"kind":       "view",
			"metadata": map[string]any{
				"namespace": "ns",
				"name":      "obj",
			},
			"a": "x",
		}))
	})
})

var _ = Describe("Object", func() {
	It("newobjectlist", func() {
		list := NewViewObjectList("view")
		Expect(list.UnstructuredContent()).To(Equal(map[string]any{
			"apiVersion": "view.dcontroller.io/v1alpha1",
			"kind":       "view",
			"items":      []any{},
		}))
	})

	It("appendtoobjectlist", func() {
		list := NewViewObjectList("view")
		obj := NewViewObject("view")

		AppendToListItem(list, obj)

		Expect(list.UnstructuredContent()).To(Equal(map[string]any{
			"apiVersion": "view.dcontroller.io/v1alpha1",
			"kind":       "view",
			"items": []any{map[string]any{
				"apiVersion": "view.dcontroller.io/v1alpha1",
				"kind":       "view",
			}},
		}))
	})

})

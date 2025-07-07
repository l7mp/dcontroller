package composite

import (
	"github.com/l7mp/dcontroller/pkg/object"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Object", func() {
	It("newobjectlist", func() {
		list := NewViewObjectList("View")
		Expect(list.UnstructuredContent()).To(Equal(map[string]any{
			"apiVersion": "view.dcontroller.io/v1alpha1",
			"kind":       "ViewList",
			"items":      []any{},
		}))
	})

	It("appendtoobjectlist", func() {
		list := NewViewObjectList("View")
		obj := object.NewViewObject("View")

		AppendToListItem(list, obj)

		Expect(list.UnstructuredContent()).To(Equal(map[string]any{
			"apiVersion": "view.dcontroller.io/v1alpha1",
			"kind":       "ViewList",
			"items": []any{map[string]any{
				"apiVersion": "view.dcontroller.io/v1alpha1",
				"kind":       "View",
			}},
		}))
	})

})

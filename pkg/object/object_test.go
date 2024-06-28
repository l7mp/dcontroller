package object

import (
	apiv1 "hsnlab/dcontroller-runtime/pkg/api/v1"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var _ = Describe("Object", func() {
	It("show be created", func() {
		obj := New("view", "ns", "test-1", map[string]any{"a": 1})
		Expect(obj).NotTo(BeNil())
		Expect(obj.GetKind()).To(Equal("view"))
		Expect(obj.GetNamespace()).To(Equal("ns"))
		Expect(obj.GetName()).To(Equal("test-1"))
		Expect(obj.UnstructuredContent()).To(Equal(map[string]any{
			"metadata": map[string]any{
				"name":      "test-1",
				"namespace": "ns",
			},
			"a": 1,
		}))
	})

	It("show not let GVK to be updated", func() {
		obj := New("view", "ns", "test-1", map[string]any{})
		Expect(obj).NotTo(BeNil())

		obj.SetKind("dummy")
		Expect(obj.GetAPIVersion()).To(Equal(apiv1.GroupVersion.String()))
		Expect(obj.GetKind()).To(Equal("view"))

		obj.SetGroupVersionKind(schema.GroupVersionKind{Group: "dummy-group", Version: "dummy-version", Kind: "dummy-kind"})
		Expect(obj.GetAPIVersion()).To(Equal(apiv1.GroupVersion.String()))
		Expect(obj.GetKind()).To(Equal("view"))
	})
})

func TestObject(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Object")
}

package object

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

var _ = Describe("ObjecUID", func() {
	It("new-uid", func() {
		obj := NewViewObject("test", "view")
		obj.SetName("test")
		obj.SetNamespace("test")

		WithUID(obj)
		uid, ok, err := unstructured.NestedString(obj.UnstructuredContent(), "metadata", "uid")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())
		Expect(uid).NotTo(BeEmpty())
	})

	It("strip uid", func() {
		obj := NewViewObject("test", "view")
		obj.SetName("test")
		obj.SetNamespace("test")
		obj.SetUID(types.UID("UUUIIIDDD"))
		RemoveUID(obj)
		_, ok, err := unstructured.NestedString(obj.UnstructuredContent(), "metadata", "uid")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeFalse())
	})

	It("stable-uid", func() {
		obj := NewViewObject("test", "view")
		obj.SetName("test")
		obj.SetNamespace("test")

		WithUID(obj)
		uid, ok, err := unstructured.NestedString(obj.UnstructuredContent(), "metadata", "uid")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())

		RemoveUID(obj)
		WithUID(obj)
		uid2, ok, err := unstructured.NestedString(obj.UnstructuredContent(), "metadata", "uid")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())
		Expect(uid2).To(Equal(uid))
	})
})

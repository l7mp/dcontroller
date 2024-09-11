package object

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ client.Object = &ViewObject{}
var _ schema.ObjectKind = &ViewObject{}
var _ metav1.ListInterface = &ViewObject{}
var _ Unstructured = &ViewObject{}
var _ Object = &unstructured.Unstructured{}

var _ = Describe("Object", func() {
	It("deepequal", func() {
		obj1 := NewViewObject("view1")
		obj2 := NewViewObject("view2")

		Expect(DeepEqual(obj1, obj2)).To(BeFalse())
		Expect(DeepEqual(obj1, obj1)).To(BeTrue())
		Expect(DeepEqual(obj2, obj2)).To(BeTrue())
	})

	It("deepequal unstructured", func() {
		obj1 := &unstructured.Unstructured{}
		obj1.SetName("a")
		obj2 := &unstructured.Unstructured{}
		obj2.SetName("b")

		Expect(DeepEqual(obj1, obj2)).To(BeFalse())
		Expect(DeepEqual(obj1, obj1)).To(BeTrue())
		Expect(DeepEqual(obj2, obj2)).To(BeTrue())
	})

	It("deepequal viewobject w/ unstructured", func() {
		obj1 := NewViewObject("view1")
		obj1.SetName("a")
		obj2 := &unstructured.Unstructured{}
		obj2.SetName("a")

		Expect(DeepEqual(obj1, obj2)).To(BeFalse())
		Expect(DeepEqual(obj1, obj1)).To(BeTrue())
		Expect(DeepEqual(obj2, obj2)).To(BeTrue())
	})
})

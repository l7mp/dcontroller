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

var _ Object = &ViewObject{}
var _ Object = &unstructured.Unstructured{}

var _ client.ObjectList = &ViewObjectList{}
var _ ObjectList = &ViewObjectList{}
var _ ObjectList = &unstructured.UnstructuredList{}

var _ = Describe("Object", func() {
	It("deepequal viewobject", func() {
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

	It("deepcopy viewobject", func() {
		obj1 := NewViewObject("view1")
		obj2 := NewViewObject("")
		DeepCopyInto(obj1, obj2)
		Expect(DeepEqual(obj1, obj2)).To(BeTrue())

		obj3 := DeepCopy(obj1)
		Expect(DeepEqual(obj1, obj3)).To(BeTrue())
	})

	It("deepcopy unstructured", func() {
		obj1 := &unstructured.Unstructured{}
		obj1.SetUnstructuredContent(map[string]any{"spec": "x", "status": int64(12)})
		obj1.SetGroupVersionKind(schema.GroupVersionKind{Group: "testgroup", Version: "v1", Kind: "testkind"})
		obj1.SetNamespace("test-ns")
		obj1.SetName("test")
		obj2 := &unstructured.Unstructured{}
		DeepCopyInto(obj1, obj2)
		Expect(DeepEqual(obj1, obj2)).To(BeTrue())

		obj3 := DeepCopy(obj1)
		Expect(DeepEqual(obj1, obj3)).To(BeTrue())
	})

})

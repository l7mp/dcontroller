package v1alpha1

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestViewAPI(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "View API")
}

var _ = Describe("Views", func() {
	It("should handle views", func() {
		viewGVK := GroupVersionKind("test", "TestView")
		Expect(IsViewKind(viewGVK)).To(BeTrue())
		Expect(GetOperator(viewGVK)).To(Equal("test"))
		Expect(viewGVK.Kind).To(Equal("TestView"))
		Expect(HasViewGroupVersionKind(viewGVK.Group, viewGVK)).To(BeTrue())
		Expect(HasViewGroupVersion(viewGVK.Group, viewGVK.GroupVersion())).To(BeTrue())
	})
})

var _ = Describe("GVK mapping", func() {
	It("should make a full native->view->native roundtrip", func() {
		dep := &appsv1.Deployment{TypeMeta: metav1.TypeMeta{
			APIVersion: appsv1.SchemeGroupVersion.String(),
			Kind:       "Deployment",
		}}
		viewGVK := MapIntoView("test", dep.GroupVersionKind())
		Expect(IsViewKind(viewGVK)).To(BeTrue())
		Expect(GetOperator(viewGVK)).To(Equal("test"))
		depGVK, err := MapFromView(viewGVK)
		Expect(err).NotTo(HaveOccurred())
		Expect(IsViewKind(depGVK)).To(BeFalse())
		Expect(depGVK).To(Equal(dep.GroupVersionKind()))
	})

	It("should handle the core group", func() {
		pod := &corev1.Pod{TypeMeta: metav1.TypeMeta{
			APIVersion: corev1.SchemeGroupVersion.String(), // "v1"
			Kind:       "Pod",
		}}
		viewGVK := MapIntoView("test", pod.GroupVersionKind())
		Expect(IsViewKind(viewGVK)).To(BeTrue())
		Expect(GetOperator(viewGVK)).To(Equal("test"))
		podGVK, err := MapFromView(viewGVK)
		Expect(err).NotTo(HaveOccurred())
		Expect(IsViewKind(podGVK)).To(BeFalse())
		Expect(podGVK).To(Equal(pod.GroupVersionKind()))
	})
})

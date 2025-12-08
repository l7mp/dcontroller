package reconciler

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/l7mp/dcontroller/pkg/manager"
	"github.com/l7mp/dcontroller/pkg/object"
)

var _ = Describe("Utils - CreateOrUpdate and Update", func() {
	var (
		ctx    context.Context
		cancel context.CancelFunc
		mgr    *manager.FakeManager
		c      client.Client
	)

	BeforeEach(func() {
		ctx, cancel = context.WithCancel(context.Background())
		var err error
		mgr, err = manager.NewFakeManager(manager.Options{Logger: logger})
		Expect(err).NotTo(HaveOccurred())
		c = mgr.GetClient()
		go func() { mgr.Start(ctx) }()
	})

	AfterEach(func() {
		cancel()
	})

	Describe("Update function", func() {
		It("should update spec and status for view objects", func() {
			// Create initial view object.
			obj := object.NewViewObject("test", "UpdateTestView")
			obj.SetName("test-obj")
			obj.SetNamespace("default")
			obj.Object["spec"] = map[string]any{"field1": "value1"}
			obj.Object["status"] = map[string]any{"condition": "Initial"}

			err := c.Create(ctx, obj)
			Expect(err).NotTo(HaveOccurred())

			// Retrieve it.
			retrieved := object.NewViewObject("test", "UpdateTestView")
			retrieved.SetName("test-obj")
			retrieved.SetNamespace("default")
			Eventually(func() error {
				return c.Get(ctx, client.ObjectKeyFromObject(retrieved), retrieved)
			}, time.Second, 50*time.Millisecond).Should(Succeed())

			// Update both spec and status using our Update function.
			retrieved.Object["spec"] = map[string]any{"field1": "updated", "field2": "new"}
			retrieved.Object["status"] = map[string]any{"condition": "Updated", "phase": "Active"}

			err = Update(ctx, c, retrieved)
			Expect(err).NotTo(HaveOccurred())

			// Verify both spec and status were updated.
			retrieved2 := object.NewViewObject("test", "UpdateTestView")
			retrieved2.SetName("test-obj")
			retrieved2.SetNamespace("default")
			Eventually(func() bool {
				err := c.Get(ctx, client.ObjectKeyFromObject(retrieved2), retrieved2)
				if err != nil {
					return false
				}
				spec, ok := retrieved2.Object["spec"].(map[string]any)
				return ok && spec["field2"] == "new"
			}, time.Second, 50*time.Millisecond).Should(BeTrue())

			spec := retrieved2.Object["spec"].(map[string]any)
			Expect(spec["field1"]).To(Equal("updated"))
			Expect(spec["field2"]).To(Equal("new"))

			status := retrieved2.Object["status"].(map[string]any)
			Expect(status["condition"]).To(Equal("Updated"))
			Expect(status["phase"]).To(Equal("Active"))
		})

		It("should update spec and status for native Kubernetes objects", func() {
			// Create a Pod (native Kubernetes object with status subresource).
			podn := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod-update",
					Namespace: "default",
				},
				Spec: corev1.PodSpec{
					Containers:    []corev1.Container{{Name: "nginx", Image: "nginx"}},
					RestartPolicy: corev1.RestartPolicyOnFailure,
				},
				Status: corev1.PodStatus{
					QOSClass: corev1.PodQOSBurstable,
				},
			}

			pod := &unstructured.Unstructured{}
			content, err := runtime.DefaultUnstructuredConverter.ToUnstructured(podn)
			Expect(err).NotTo(HaveOccurred())
			pod.SetUnstructuredContent(content)
			pod.SetGroupVersionKind(schema.GroupVersionKind{
				Group:   "",
				Version: "v1",
				Kind:    "Pod",
			})

			err = c.Create(ctx, pod)
			Expect(err).NotTo(HaveOccurred())

			// Update spec and status using our Update function.
			unstructured.SetNestedField(pod.UnstructuredContent(), "Always", "spec", "restartPolicy")
			unstructured.SetNestedField(pod.UnstructuredContent(), "BestEffort", "status", "qosClass")

			err = Update(ctx, c, pod)
			Expect(err).NotTo(HaveOccurred())

			// Get from the object tracker: a normal get would go through the cache
			// first get should fail
			tracker := mgr.GetObjectTracker()
			gvr := schema.GroupVersionResource{
				Group:    "",
				Version:  "v1",
				Resource: "pods", // Resource does not equal Kind!
			}

			p := &corev1.Pod{}
			Eventually(func() bool {
				getFromTracker, err := tracker.Get(gvr, "default", "test-pod-update")
				if err != nil {
					return false
				}

				getFromClient, err := object.ConvertRuntimeObjectToClientObject(getFromTracker)
				if err != nil {
					return false
				}

				p = getFromClient.(*corev1.Pod)
				return p.Spec.RestartPolicy == corev1.RestartPolicyAlways
			}, time.Second, 50*time.Millisecond).Should(BeTrue())

			// Verify status was updated.
			Expect(p.Spec.RestartPolicy).To(Equal(corev1.RestartPolicyAlways))
			Expect(p.Status.QOSClass).To(Equal(corev1.PodQOSBestEffort))
		})
	})

	Describe("CreateOrUpdate function", func() {
		It("should create a new view object with spec and status", func() {
			obj := object.NewViewObject("test", "CreateOrUpdateView")
			obj.SetName("new-obj")
			obj.SetNamespace("default")
			obj.Object["spec"] = map[string]any{"field": "value"}
			obj.Object["status"] = map[string]any{"condition": "Ready"}

			result, err := CreateOrUpdate(ctx, c, obj, func() error {
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(controllerutil.OperationResultCreated))

			// Verify creation.
			retrieved := object.NewViewObject("test", "CreateOrUpdateView")
			retrieved.SetName("new-obj")
			retrieved.SetNamespace("default")
			Eventually(func() error {
				return c.Get(ctx, client.ObjectKeyFromObject(retrieved), retrieved)
			}, time.Second, 50*time.Millisecond).Should(Succeed())

			Expect(retrieved.Object["spec"]).To(Equal(map[string]any{"field": "value"}))
			Expect(retrieved.Object["status"]).To(Equal(map[string]any{"condition": "Ready"}))
		})

		It("should update existing view object with spec and status", func() {
			// Create initial object.
			obj := object.NewViewObject("test", "CreateOrUpdateView2")
			obj.SetName("existing-obj")
			obj.SetNamespace("default")
			obj.Object["spec"] = map[string]any{"field1": "value1"}
			obj.Object["status"] = map[string]any{"condition": "Initial"}

			err := c.Create(ctx, obj)
			Expect(err).NotTo(HaveOccurred())

			// Wait for it to be created.
			Eventually(func() error {
				return c.Get(ctx, client.ObjectKeyFromObject(obj), obj)
			}, time.Second, 50*time.Millisecond).Should(Succeed())

			// Update using CreateOrUpdate.
			obj2 := object.NewViewObject("test", "CreateOrUpdateView2")
			obj2.SetName("existing-obj")
			obj2.SetNamespace("default")

			result, err := CreateOrUpdate(ctx, c, obj2, func() error {
				obj2.Object["spec"] = map[string]any{"field1": "updated", "field2": "new"}
				obj2.Object["status"] = map[string]any{"condition": "Updated", "phase": "Active"}
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(controllerutil.OperationResultUpdated))

			// Verify update.
			retrieved := object.NewViewObject("test", "CreateOrUpdateView2")
			retrieved.SetName("existing-obj")
			retrieved.SetNamespace("default")
			Eventually(func() bool {
				err := c.Get(ctx, client.ObjectKeyFromObject(retrieved), retrieved)
				if err != nil {
					return false
				}
				spec, ok := retrieved.Object["spec"].(map[string]any)
				return ok && spec["field2"] == "new"
			}, time.Second, 50*time.Millisecond).Should(BeTrue())

			spec := retrieved.Object["spec"].(map[string]any)
			Expect(spec["field1"]).To(Equal("updated"))
			Expect(spec["field2"]).To(Equal("new"))

			status := retrieved.Object["status"].(map[string]any)
			Expect(status["condition"]).To(Equal("Updated"))
			Expect(status["phase"]).To(Equal("Active"))
		})

		It("should update spec and status for native Kubernetes objects", func() {
			// Create a Pod (native Kubernetes object with status subresource).
			podn := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod-update",
					Namespace: "default",
				},
				Spec: corev1.PodSpec{
					Containers:    []corev1.Container{{Name: "nginx", Image: "nginx"}},
					RestartPolicy: corev1.RestartPolicyOnFailure,
				},
				Status: corev1.PodStatus{
					QOSClass: corev1.PodQOSBurstable,
				},
			}

			pod := &unstructured.Unstructured{}
			content, err := runtime.DefaultUnstructuredConverter.ToUnstructured(podn)
			Expect(err).NotTo(HaveOccurred())
			pod.SetUnstructuredContent(content)
			pod.SetGroupVersionKind(schema.GroupVersionKind{
				Group:   "",
				Version: "v1",
				Kind:    "Pod",
			})

			err = c.Create(ctx, pod)
			Expect(err).NotTo(HaveOccurred())

			result, err := CreateOrUpdate(ctx, c, pod, func() error {
				unstructured.SetNestedField(pod.UnstructuredContent(), "Always", "spec", "restartPolicy")
				unstructured.SetNestedField(pod.UnstructuredContent(), "BestEffort", "status", "qosClass")
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(controllerutil.OperationResultUpdated))

			// Get from the object tracker: a normal get would go through the cache
			// first get should fail
			tracker := mgr.GetObjectTracker()
			gvr := schema.GroupVersionResource{
				Group:    "",
				Version:  "v1",
				Resource: "pods", // Resource does not equal Kind!
			}

			p := &corev1.Pod{}
			Eventually(func() bool {
				getFromTracker, err := tracker.Get(gvr, "default", "test-pod-update")
				if err != nil {
					return false
				}

				getFromClient, err := object.ConvertRuntimeObjectToClientObject(getFromTracker)
				if err != nil {
					return false
				}

				p = getFromClient.(*corev1.Pod)
				return len(p.Spec.Containers) == 1
			}, time.Second, 50*time.Millisecond).Should(BeTrue())

			// Verify status was updated.
			Expect(p.Spec.Containers).To(HaveLen(1))
			Expect(p.Spec.RestartPolicy).To(Equal(corev1.RestartPolicyAlways))
			Expect(p.Status.QOSClass).To(Equal(corev1.PodQOSBestEffort))
		})
	})
})

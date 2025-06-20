package apiserver

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap/zapcore"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	runtimeManager "sigs.k8s.io/controller-runtime/pkg/manager"

	viewv1a1 "github.com/l7mp/dcontroller/pkg/api/view/v1alpha1"
	"github.com/l7mp/dcontroller/pkg/cache"
	"github.com/l7mp/dcontroller/pkg/manager"
	"github.com/l7mp/dcontroller/pkg/object"
)

var (
	loglevel = -10
	logger   = zap.New(zap.UseFlagOptions(&zap.Options{
		Development:     true,
		DestWriter:      GinkgoWriter,
		StacktraceLevel: zapcore.Level(3),
		TimeEncoder:     zapcore.RFC3339NanoTimeEncoder,
		Level:           zapcore.Level(loglevel),
	}))
)

func TestManager(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "APIServer")
}

var _ = Describe("APIServer Integration", func() {
	var (
		apiServer     *APIServer
		mgr           *manager.FakeManager
		fakeClient    client.Client
		serverCtx     context.Context
		serverCancel  context.CancelFunc
		serverAddr    string
		restConfig    *rest.Config
		dynamicClient dynamic.Interface
		port          int
		obj           = &unstructured.Unstructured{
			Object: map[string]any{
				"apiVersion": "view.dcontroller.io/v1alpha1",
				"kind":       "TestView",
				"metadata": map[string]any{
					"namespace": "default",
					"name":      "test-view",
					"labels":    map[string]any{"app": "test"},
				},
				"a": "x",
			},
		}
	)

	BeforeEach(func() {
		serverCtx, serverCancel = context.WithCancel(context.Background())

		// Create mock manager
		var err error
		mgr, err = manager.NewFakeManager(runtimeManager.Options{Logger: logger})
		Expect(err).NotTo(HaveOccurred())

		// no discovery client - no native object support
		viewCache, ok := mgr.GetCache().(*cache.ViewCache)
		Expect(ok).To(BeTrue())
		Expect(viewCache).NotTo(BeNil())
		err = viewCache.Add(obj)
		Expect(err).NotTo(HaveOccurred())

		// Create API server at random port
		port = rand.IntN(5000) + (32768)
		apiServer, err = NewAPIServer(mgr, "127.0.0.1", port)
		Expect(err).NotTo(HaveOccurred())

		err = apiServer.RegisterGVK(schema.GroupVersionKind{
			Group:   viewv1a1.GroupVersion.Group,
			Version: viewv1a1.GroupVersion.Version,
			Kind:    "TestView",
		})
		Expect(err).NotTo(HaveOccurred())

		// Start the server
		serverDone := make(chan error, 1)
		go func() {
			defer GinkgoRecover()
			serverDone <- apiServer.Start(serverCtx)
		}()

		// Create REST config for the API server
		restConfig = &rest.Config{
			Host: fmt.Sprintf("https://%s:%d", serverAddr, port),
			TLSClientConfig: rest.TLSClientConfig{
				Insecure: true, // Skip TLS verification for testing
			},
		}

		// Create dynamic client
		dynamicClient, err = dynamic.NewForConfig(restConfig)
		Expect(err).NotTo(HaveOccurred())

		// Create fake runtime client
		fakeClient = mgr.GetClient()
	})

	AfterEach(func() {
		if apiServer != nil {
			apiServer.Shutdown()
		}
		serverCancel()
		time.Sleep(20 * time.Millisecond) // Give server time to shutdown
	})

	Describe("REST Operations", func() {
		var viewGVR schema.GroupVersionResource

		BeforeEach(func() {
			viewGVR = schema.GroupVersionResource{
				Group:    viewv1a1.GroupVersion.Group,
				Version:  viewv1a1.GroupVersion.Version,
				Resource: "TestView",
			}
		})

		It("should handle GET operations", func() {
			// Get existing ConfigMap
			obj, err := dynamicClient.Resource(viewGVR).
				Namespace("default").
				Get(context.TODO(), "test-view", metav1.GetOptions{})

			Expect(err).NotTo(HaveOccurred())
			Expect(obj.GetName()).To(Equal("test-view"))
			Expect(obj.GetNamespace()).To(Equal("default"))

			// Check the data
			data, found, err := unstructured.NestedStringMap(obj.Object, "data")
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(data["a"]).To(Equal("x"))
		})

		It("should handle LIST operations", func() {
			// List ConfigMaps
			list, err := dynamicClient.Resource(viewGVR).
				Namespace("default").
				List(context.TODO(), metav1.ListOptions{})

			Expect(err).NotTo(HaveOccurred())
			Expect(len(list.Items)).To(Equal(1))
			Expect(list.Items[0].GetName()).To(Equal("test-view"))
		})

		It("should handle CREATE operations", func() {
			// Create new ConfigMap
			newView := &unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": "view.dcontroller.io/v1alpha1",
					"kind":       "view",
					"metadata": map[string]any{
						"name":      "new-view",
						"namespace": "default",
					},
					"data": map[string]any{
						"newkey": "newvalue",
					},
				},
			}

			created, err := dynamicClient.Resource(viewGVR).
				Namespace("default").
				Create(context.TODO(), newView, metav1.CreateOptions{})

			Expect(err).NotTo(HaveOccurred())
			Expect(created.GetName()).To(Equal("new-view"))

			// Verify it was created in the fake client
			createdView := object.NewViewObject("TestView")
			err = fakeClient.Get(context.TODO(), client.ObjectKey{
				Name:      "new-view",
				Namespace: "default",
			}, createdView)
			Expect(err).NotTo(HaveOccurred())
			data, found, err := unstructured.NestedFieldNoCopy(createdView.Object, "newkey")
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(data).To(Equal("newvalue"))
		})

		It("should handle UPDATE operations", func() {
			// Get existing ConfigMap
			obj, err := dynamicClient.Resource(viewGVR).
				Namespace("default").
				Get(context.TODO(), "test-view", metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())

			// Update the data
			err = unstructured.SetNestedStringMap(obj.Object, map[string]string{
				"key":    "updated-value",
				"newkey": "another-value",
			}, "data")
			Expect(err).NotTo(HaveOccurred())

			// Update via API server
			updated, err := dynamicClient.Resource(viewGVR).
				Namespace("default").
				Update(context.TODO(), obj, metav1.UpdateOptions{})

			Expect(err).NotTo(HaveOccurred())

			// Verify the update
			data, found, err := unstructured.NestedStringMap(updated.Object, "data")
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(data["key"]).To(Equal("updated-value"))
			Expect(data["newkey"]).To(Equal("another-value"))

			// Verify it was updated in the fake client
			updatedView := object.NewViewObject("TestView")
			err = fakeClient.Get(context.TODO(), client.ObjectKey{
				Name:      "new-view",
				Namespace: "default",
			}, updatedView)
			Expect(err).NotTo(HaveOccurred())
			newData, found, err := unstructured.NestedFieldNoCopy(updatedView.Object, "data", "key")
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(newData).To(Equal("newvalue"))
			newData, found, err = unstructured.NestedFieldNoCopy(updatedView.Object, "data", "newkey")
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(newData).To(Equal("another-value"))
		})

		It("should handle DELETE operations", func() {
			// Delete the ConfigMap
			err := dynamicClient.Resource(viewGVR).
				Namespace("default").
				Delete(context.TODO(), "test-view", metav1.DeleteOptions{})

			Expect(err).NotTo(HaveOccurred())

			// Verify it was deleted from the fake client
			deletedView := object.NewViewObject("TestView")
			err = fakeClient.Get(context.TODO(), client.ObjectKey{
				Name:      "new-view",
				Namespace: "default",
			}, deletedView)
			Expect(err).To(HaveOccurred())
			Expect(client.IgnoreNotFound(err)).To(BeNil())
		})

		It("should handle 404 for non-existent view resources", func() {
			// Try to get non-existent ConfigMap
			_, err := dynamicClient.Resource(viewGVR).
				Namespace("default").
				Get(context.TODO(), "non-existent", metav1.GetOptions{})

			Expect(err).To(HaveOccurred())
			// Should be a 404 error
			Expect(err.Error()).To(ContainSubstring("not found"))
		})

		It("should err for native K8s resources", func() {
			// Try to get non-existent ConfigMap
			configMapGVR := schema.GroupVersionResource{
				Group:    "",
				Version:  "v1",
				Resource: "configmaps",
			}
			_, err := dynamicClient.Resource(configMapGVR).
				Namespace("default").
				Get(context.TODO(), "non-existent", metav1.GetOptions{})

			Expect(err).To(HaveOccurred())
			// Should be a 404 error
			Expect(err.Error()).To(ContainSubstring("not found"))
		})
	})

	Describe("List with Label Selectors", func() {
		It("should filter by labels", func() {
			viewGVR := schema.GroupVersionResource{
				Group:    "",
				Version:  "v1",
				Resource: "TestView",
			}

			// List with label selector
			list, err := dynamicClient.Resource(viewGVR).
				Namespace("default").
				List(context.TODO(), metav1.ListOptions{
					LabelSelector: "app=test",
				})

			Expect(err).NotTo(HaveOccurred())
			Expect(len(list.Items)).To(Equal(1))
			Expect(list.Items[0].GetName()).To(Equal("test-view"))

			// List with non-matching label selector
			list, err = dynamicClient.Resource(viewGVR).
				Namespace("default").
				List(context.TODO(), metav1.ListOptions{
					LabelSelector: "app=nonexistent",
				})

			Expect(err).NotTo(HaveOccurred())
			Expect(len(list.Items)).To(Equal(0))
		})
	})

	Describe("Multiple Resource Types", func() {
		It("should handle different resource types", func() {
			viewGVR2 := schema.GroupVersionResource{
				Group:    viewv1a1.GroupVersion.Group,
				Version:  viewv1a1.GroupVersion.Version,
				Resource: "TestView2",
			}

			newView := &unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": "view.dcontroller.io/v1alpha1",
					"kind":       "TestView",
					"metadata": map[string]any{
						"name":      "new-view2",
						"namespace": "default",
					},
					"data": map[string]any{
						"newkey2": "newvalue2",
					},
				},
			}

			created, err := dynamicClient.Resource(viewGVR2).
				Namespace("default").
				Create(context.TODO(), newView, metav1.CreateOptions{})

			Expect(err).NotTo(HaveOccurred())
			Expect(created.GetName()).To(Equal("new-view2"))

			// Verify it was created in the fake client and the old one still exists
			createdView := object.NewViewObject("TestView")
			err = fakeClient.Get(context.TODO(), client.ObjectKey{
				Name:      "new-view",
				Namespace: "default",
			}, createdView)
			Expect(err).NotTo(HaveOccurred())
			data, found, err := unstructured.NestedFieldNoCopy(createdView.Object, "newkey")
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(data).To(Equal("newvalue"))

			createdView = object.NewViewObject("TestView2")
			err = fakeClient.Get(context.TODO(), client.ObjectKey{
				Name:      "new-view2",
				Namespace: "default",
			}, createdView)
			Expect(err).NotTo(HaveOccurred())
			data, found, err = unstructured.NestedFieldNoCopy(createdView.Object, "newkey2")
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(data).To(Equal("newvalue2"))

			err = dynamicClient.Resource(viewGVR2).
				Namespace("default").
				Delete(context.TODO(), "test-view2", metav1.DeleteOptions{})
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("Custom Resource (View)", func() {
		It("should handle view resources", func() {
			viewGVR := schema.GroupVersionResource{
				Group:    viewv1a1.GroupVersion.Group,
				Version:  viewv1a1.GroupVersion.Version,
				Resource: "TestView", // resource.Name=Kind for views
			}

			// Create a test view
			testView := &unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": viewv1a1.GroupVersion.String(),
					"kind":       "TestView",
					"metadata": map[string]any{
						"name":      "test-view",
						"namespace": "default",
					},
					"spec": map[string]any{
						"selector": map[string]any{
							"matchLabels": map[string]any{
								"app": "test",
							},
						},
					},
				},
			}

			created, err := dynamicClient.Resource(viewGVR).
				Namespace("default").
				Create(context.TODO(), testView, metav1.CreateOptions{})

			Expect(err).NotTo(HaveOccurred())
			Expect(created.GetName()).To(Equal("test-view"))
			Expect(created.GetKind()).To(Equal("TestView"))
		})
	})

	Describe("Discovery", func() {
		It("should support API discovery", func() {
			discoveryClient, err := discovery.NewDiscoveryClientForConfig(restConfig)
			Expect(err).NotTo(HaveOccurred())

			// Get API groups
			groups, err := discoveryClient.ServerGroups()
			Expect(err).NotTo(HaveOccurred())

			// Should include our registered groups
			groupNames := make([]string, len(groups.Groups))
			for i, group := range groups.Groups {
				groupNames[i] = group.Name
			}

			Expect(groupNames).To(ContainElement("")) // Core group
			Expect(groupNames).To(ContainElement(viewv1a1.GroupVersion.Group))
		})
	})
})

package apiserver

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap/zapcore"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	fakediscovery "k8s.io/client-go/discovery/fake"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	fakeclient "k8s.io/client-go/testing"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrl "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	runtimeManager "sigs.k8s.io/controller-runtime/pkg/manager"

	viewv1a1 "github.com/l7mp/dcontroller/pkg/api/view/v1alpha1"
	"github.com/l7mp/dcontroller/pkg/composite"
	"github.com/l7mp/dcontroller/pkg/manager"
	"github.com/l7mp/dcontroller/pkg/object"
)

const (
	timeout  = 2 * time.Second
	interval = 100 * time.Millisecond
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
	pod  = &unstructured.Unstructured{}
	podn = &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "testpod",
			Namespace: "testns",
		},
		Spec: corev1.PodSpec{
			Containers:    []corev1.Container{{Name: "nginx", Image: "nginx"}},
			RestartPolicy: corev1.RestartPolicyOnFailure,
		},
	}
)

func TestAPIServer(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "APIServer")
}

var _ = Describe("APIServerUnitTest", func() {
	var (
		mgr                       runtimeManager.Manager
		server                    *APIServer
		testGroup, test2Group     string
		testViewGVK, test2ViewGVK schema.GroupVersionKind
		port                      int
	)

	BeforeEach(func() {
		testGroup = viewv1a1.Group("test")
		testViewGVK = viewv1a1.GroupVersionKind("test", "TestView")
		test2Group = viewv1a1.Group("test2")
		test2ViewGVK = viewv1a1.GroupVersionKind("test2", "TestView")

		var err error
		mgr, err = manager.NewFakeManager(runtimeManager.Options{Logger: logger})
		Expect(err).NotTo(HaveOccurred())

		port = rand.IntN(5000) + (32768)
		config, err := NewDefaultConfig("", port, true)
		Expect(err).NotTo(HaveOccurred())
		server, err = NewAPIServer(mgr, config)
		Expect(err).NotTo(HaveOccurred())
	})

	Describe("Lifecycle Management", func() {
		It("should start and shutdown gracefully", func() {
			ctx, cancel := context.WithCancel(context.Background())

			// Register a GVK first
			err := server.RegisterAPIGroup(testGroup, []schema.GroupVersionKind{testViewGVK})
			Expect(err).NotTo(HaveOccurred())

			// Start server in goroutine
			errChan := make(chan error, 1)
			go func() {
				defer close(errChan)
				errChan <- server.Start(ctx)
			}()

			// Give server a moment to start
			time.Sleep(20 * time.Millisecond)

			// Add another GVK
			err = server.RegisterAPIGroup(test2Group, []schema.GroupVersionKind{test2ViewGVK})
			Expect(err).NotTo(HaveOccurred())

			groupGVKs, ok := server.groupGVKs[testGroup]
			Expect(ok).To(BeTrue())
			Expect(groupGVKs).To(HaveKey(testViewGVK))
			Expect(groupGVKs[testViewGVK]).To(BeTrue())

			groupGVKs, ok = server.groupGVKs[test2Group]
			Expect(ok).To(BeTrue())
			Expect(groupGVKs).To(HaveKey(test2ViewGVK))
			Expect(groupGVKs[test2ViewGVK]).To(BeTrue())

			// Shutdown
			cancel()

			// Wait for start to return
			Eventually(errChan).Should(Receive(BeNil()))
		})

		It("should handle context cancellation", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			err := server.Start(ctx)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should handle restart when already running", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			err := server.RegisterAPIGroup(testGroup, []schema.GroupVersionKind{testViewGVK})
			Expect(err).NotTo(HaveOccurred())

			groupGVKs, ok := server.groupGVKs[testGroup]
			Expect(ok).To(BeTrue())
			Expect(groupGVKs).To(HaveKey(testViewGVK))
			Expect(groupGVKs[testViewGVK]).To(BeTrue())

			// Start first time
			errChan1 := make(chan error, 1)
			go func() {
				errChan1 <- server.Start(ctx)
			}()

			time.Sleep(50 * time.Millisecond)

			// Start again (should shutdown first)
			errChan2 := make(chan error, 1)
			go func() {
				errChan2 <- server.Start(ctx)
			}()

			time.Sleep(50 * time.Millisecond)

			groupGVKs, ok = server.groupGVKs[testGroup]
			Expect(ok).To(BeTrue())
			Expect(groupGVKs).To(HaveKey(testViewGVK))
			Expect(groupGVKs[testViewGVK]).To(BeTrue())

			// Cancel context to stop
			cancel()

			Eventually(errChan1).Should(Receive())
			Eventually(errChan2).Should(Receive())
		})
	})

	Describe("Server Configuration", func() {
		It("should handle empty GVK list", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()

			// Don't register any GVKs
			err := server.Start(ctx)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should handle various address formats", func() {
			testCases := []struct {
				addr     string
				expected net.IP
			}{
				{"127.0.0.1", net.ParseIP("127.0.0.1")},
				{"localhost", net.ParseIP("127.0.0.1")}, // Should default
				{"", net.ParseIP("127.0.0.1")},          // Should default
				{"0.0.0.0", net.ParseIP("0.0.0.0")},
			}

			for _, tc := range testCases {
				port = rand.IntN(15000) + (32768)
				config, err := NewDefaultConfig(tc.addr, port, true)
				Expect(err).NotTo(HaveOccurred())
				s, err := NewAPIServer(mgr, config)
				Expect(err).NotTo(HaveOccurred())
				Expect(s).NotTo(BeNil())
			}
		})
	})
})

var _ = Describe("APIServer Integration", func() {
	var (
		apiServer     *APIServer
		mgr           *manager.FakeManager
		cacheClient   client.Client
		serverCtx     context.Context
		serverCancel  context.CancelFunc
		serverAddr    string
		dynamicClient dynamic.Interface
		port          int
		obj           = &unstructured.Unstructured{
			Object: map[string]any{
				"apiVersion": "test.view.dcontroller.io/v1alpha1",
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

		// Create the pod resource
		content, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(podn)
		pod.SetUnstructuredContent(content)
		// this is needed: for some unknown reason the converter does not work on the GVK
		pod.GetObjectKind().SetGroupVersionKind(podn.GetObjectKind().GroupVersionKind())

		// Create mock manager
		var err error
		mgr, err = manager.NewFakeManager(runtimeManager.Options{Logger: logger})
		Expect(err).NotTo(HaveOccurred())

		// Create fake discovery client for testing with native resources
		fakeDiscovery := &fakediscovery.FakeDiscovery{Fake: &fakeclient.Fake{}}
		fakeDiscovery.Resources = []*metav1.APIResourceList{
			{
				GroupVersion: "v1",
				APIResources: []metav1.APIResource{
					{Name: "pods", Namespaced: true, Kind: "Pod"},
					{Name: "configmaps", Namespaced: true, Kind: "ConfigMap"},
				},
			},
		}
		fakeViewDiscovery := composite.NewCompositeDiscoveryClient(fakeDiscovery)

		// Add a view object and a native resource to the manaegr cache
		fakeCache, ok := mgr.GetCache().(*composite.CompositeCache)
		Expect(ok).To(BeTrue())
		Expect(fakeCache).NotTo(BeNil())
		err = fakeCache.GetViewCache().Add(obj)
		Expect(err).NotTo(HaveOccurred())
		podObj, err := object.NewViewObjectFromNativeObject("test", "view", pod)
		Expect(err).NotTo(HaveOccurred())
		err = fakeCache.GetViewCache().Add(podObj)
		Expect(err).NotTo(HaveOccurred())

		// Set API server logger
		klog.SetLogger(logger.V(10).WithName("generic-apiserver"))
		// klog.V(10).Enabled() // This forces klog to think level 10 is enabled

		// Set the controller-runtime log for maximum verbosity
		ctrl.SetLogger(logger)

		// Create API server at random port
		serverAddr = "localhost"
		port = rand.IntN(15000) + 32768
		config, err := NewDefaultConfig(serverAddr, port, true)
		config.DiscoveryClient = fakeViewDiscovery
		Expect(err).NotTo(HaveOccurred())
		apiServer, err = NewAPIServer(mgr, config)
		Expect(err).NotTo(HaveOccurred())

		err = apiServer.RegisterAPIGroup(viewv1a1.Group("test"), []schema.GroupVersionKind{
			viewv1a1.GroupVersionKind("test", "TestView2"), // use reverse order for testing the codec
			viewv1a1.GroupVersionKind("test", "TestView"),
		})
		Expect(err).NotTo(HaveOccurred())

		// Start the server
		go func() {
			defer GinkgoRecover()
			err := apiServer.Start(serverCtx)
			Expect(err).NotTo(HaveOccurred())
		}()

		// Create dynamic client
		dynamicClient, err = dynamic.NewForConfig(&rest.Config{
			Host: fmt.Sprintf("http://%s:%d", serverAddr, port),
		})
		Expect(err).NotTo(HaveOccurred())

		// Create fake runtime client
		cacheClient = mgr.GetClient()

		// Wait for the API server to start
		Eventually(func() bool { return apiServer.running }, timeout, interval).Should(BeTrue())
	})

	AfterEach(func() {
		serverCancel()
		time.Sleep(20 * time.Millisecond) // Give server time to shutdown
	})

	Describe("REST Operations", func() {
		var viewGVR = viewv1a1.GroupVersion("test").WithResource("testview")

		It("should handle GET operations", func() {
			// Get existing view object
			obj, err := dynamicClient.Resource(viewGVR).
				Namespace("default").
				Get(context.TODO(), "test-view", metav1.GetOptions{})

			Expect(err).NotTo(HaveOccurred())
			Expect(obj.GetName()).To(Equal("test-view"))
			Expect(obj.GetNamespace()).To(Equal("default"))
			Expect(obj.GetKind()).To(Equal("TestView"))

			// Check the data
			data, found, err := unstructured.NestedString(obj.Object, "a")
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(data).To(Equal("x"))
		})

		It("should handle LIST operations", func() {
			list, err := dynamicClient.Resource(viewGVR).
				Namespace("default").
				List(context.TODO(), metav1.ListOptions{})
			Expect(err).NotTo(HaveOccurred())

			Expect(list.GetKind()).To(Equal("TestViewList"))
			Expect(len(list.Items)).To(Equal(1))
			Expect(list.Items[0].GetKind()).To(Equal("TestView"))
			Expect(list.Items[0].GetName()).To(Equal("test-view"))

			data, found, err := unstructured.NestedString(list.Items[0].Object, "a")
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(data).To(Equal("x"))
		})

		It("should handle CREATE operations", func() {
			// Create
			newView := &unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": "test.view.dcontroller.io/v1alpha1",
					"kind":       "TestView",
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
			data, found, err := unstructured.NestedFieldNoCopy(created.Object, "data", "newkey")
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(data).To(Equal("newvalue"))

			// Verify it was created in the fake client
			createdView := object.NewViewObject("test", "TestView")
			Eventually(func() bool {
				err = cacheClient.Get(context.TODO(), client.ObjectKey{
					Name:      "new-view",
					Namespace: "default",
				}, createdView)

				if err != nil && apierrors.IsNotFound(err) {
					return false
				}

				return createdView.GetName() == "new-view"
			}, timeout, interval).Should(BeTrue())

			data, found, err = unstructured.NestedFieldNoCopy(createdView.Object, "data", "newkey")
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(data).To(Equal("newvalue"))

			// for gv, tp := range apiServer.GetScheme().AllKnownTypes() {
			// 	fmt.Println("Registered GVK", "gv", gv, "goType", tp.String())
			// }

			// List
			list, err := dynamicClient.Resource(viewGVR).
				Namespace("default").
				List(context.TODO(), metav1.ListOptions{})
			Expect(err).NotTo(HaveOccurred())
			Expect(list.GetKind()).To(Equal("TestViewList"))
			Expect(len(list.Items)).To(Equal(2))

			item := list.Items[0]
			if item.GetName() != "test-view" {
				item = list.Items[1]
			}
			Expect(item.GetKind()).To(Equal("TestView"))
			Expect(item.GetName()).To(Equal("test-view"))
			data, found, err = unstructured.NestedString(item.Object, "a")
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(data).To(Equal("x"))

			item = list.Items[1]
			if item.GetName() != "new-view" {
				item = list.Items[0]
			}
			Expect(item.GetKind()).To(Equal("TestView"))
			Expect(item.GetName()).To(Equal("new-view"))
			data, found, err = unstructured.NestedString(item.Object, "data", "newkey")
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(data).To(Equal("newvalue"))
		})

		It("should handle UPDATE operations", func() {
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

			// Wait until the update has been safely applied
			updatedView := object.NewViewObject("test", "TestView")
			Eventually(func() bool {
				err = cacheClient.Get(context.TODO(), client.ObjectKey{
					Name:      "test-view",
					Namespace: "default",
				}, updatedView)

				if err != nil && apierrors.IsNotFound(err) {
					return false
				}

				if updatedView.GetName() != "test-view" {
					return false
				}

				newData, found, err := unstructured.NestedFieldNoCopy(updatedView.Object, "data", "key")
				if err != nil || !found {
					return false
				}
				return newData.(string) == "updated-value"
			}, timeout, interval).Should(BeTrue())

			// Verify it was updated in the fake client
			Expect(err).NotTo(HaveOccurred())
			newData, found, err := unstructured.NestedFieldNoCopy(updatedView.Object, "data", "key")
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(newData).To(Equal("updated-value"))
			newData, found, err = unstructured.NestedFieldNoCopy(updatedView.Object, "data", "newkey")
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(newData).To(Equal("another-value"))

			// List
			list, err := dynamicClient.Resource(viewGVR).
				Namespace("default").
				List(context.TODO(), metav1.ListOptions{})
			Expect(err).NotTo(HaveOccurred())
			Expect(list.GetKind()).To(Equal("TestViewList"))
			Expect(len(list.Items)).To(Equal(1))

			item := list.Items[0]
			Expect(item.GetKind()).To(Equal("TestView"))
			Expect(item.GetName()).To(Equal("test-view"))
			updatedData, found, err := unstructured.NestedString(item.Object, "a")
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(updatedData).To(Equal("x"))
		})

		It("should handle DELETE operations", func() {
			err := dynamicClient.Resource(viewGVR).
				Namespace("default").
				Delete(context.TODO(), "test-view", metav1.DeleteOptions{})

			Expect(err).NotTo(HaveOccurred())

			// Verify it was deleted from the fake client
			deletedView := object.NewViewObject("test", "TestView")
			err = cacheClient.Get(context.TODO(), client.ObjectKey{
				Name:      "new-view",
				Namespace: "default",
			}, deletedView)
			Expect(err).To(HaveOccurred())
			Expect(client.IgnoreNotFound(err)).To(BeNil())

			// List
			list, err := dynamicClient.Resource(viewGVR).
				Namespace("default").
				List(context.TODO(), metav1.ListOptions{})
			Expect(err).NotTo(HaveOccurred())

			Expect(list.GetKind()).To(Equal("TestViewList"))
			Expect(len(list.Items)).To(Equal(0))
		})

		It("should handle 404 for non-existent view resources", func() {
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
			Expect(err.Error()).To(ContainSubstring("could not find"))
		})
	})

	Describe("List with Label Selectors", func() {
		It("should filter by labels", func() {
			viewGVR := viewv1a1.GroupVersion("test").WithResource("testview")

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
			viewGVR2 := viewv1a1.GroupVersion("test").WithResource("testview2")
			newView := &unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": "test.view.dcontroller.io/v1alpha1",
					"kind":       "TestView2",
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
			createdView := object.NewViewObject("test", "TestView")
			err = cacheClient.Get(context.TODO(), client.ObjectKey{
				Name:      "test-view",
				Namespace: "default",
			}, createdView)
			Expect(err).NotTo(HaveOccurred())
			data, found, err := unstructured.NestedFieldNoCopy(createdView.Object, "a")
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(data).To(Equal("x"))

			createdView = object.NewViewObject("test", "TestView2")
			err = cacheClient.Get(context.TODO(), client.ObjectKey{
				Name:      "new-view2",
				Namespace: "default",
			}, createdView)
			Expect(err).NotTo(HaveOccurred())
			data, found, err = unstructured.NestedFieldNoCopy(createdView.Object, "data", "newkey2")
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(data).To(Equal("newvalue2"))

			err = dynamicClient.Resource(viewGVR2).
				Namespace("default").
				Delete(context.TODO(), "new-view2", metav1.DeleteOptions{})
			Expect(err).NotTo(HaveOccurred())

			err = cacheClient.Get(context.TODO(), client.ObjectKey{
				Name:      "new-view2",
				Namespace: "default",
			}, createdView)
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("Custom Resource (View)", func() {
		It("should handle view resources", func() {
			viewGVR := viewv1a1.GroupVersion("test").WithResource("testview")

			// Create a test view
			testView := &unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": viewv1a1.GroupVersion("test").String(),
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
})

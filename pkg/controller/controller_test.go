package view

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap/zapcore"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	runtimeManager "sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/yaml"

	viewv1a1 "hsnlab/dcontroller-runtime/pkg/api/view/v1alpha1"
	"hsnlab/dcontroller-runtime/pkg/cache"
	"hsnlab/dcontroller-runtime/pkg/manager"
	"hsnlab/dcontroller-runtime/pkg/object"
	"hsnlab/dcontroller-runtime/pkg/pipeline"
)

const (
	timeout  = time.Second * 1
	interval = time.Millisecond * 50
)

var (
	loglevel = -10
	logger   = zap.New(zap.UseFlagOptions(&zap.Options{
		Development:     true,
		DestWriter:      GinkgoWriter,
		StacktraceLevel: zapcore.Level(10),
		TimeEncoder:     zapcore.RFC3339NanoTimeEncoder,
		Level:           zapcore.Level(loglevel),
	}))
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

// func init() {
// 	corev1.AddToScheme(scheme)
// }

func TestView(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "View")
}

var _ = Describe("Controller", func() {
	var (
		pod, view object.Object
		ctx       context.Context
		cancel    context.CancelFunc
	)

	BeforeEach(func() {
		ctx, cancel = context.WithCancel(logr.NewContext(context.Background(), logger))
		pod = &unstructured.Unstructured{}
		content, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(podn)
		pod.SetUnstructuredContent(content)
		pod.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "",
			Version: "v1",
			Kind:    "Pod",
		})

		view = object.NewViewObject("view")
		object.SetName(view, "default", "viewname")
		object.SetContent(view, map[string]any{"testannotation": "test-value"})
	})

	AfterEach(func() {
		cancel()
	})

	It("should generate watch requests on the target view", func() {
		jsonData := `
'@aggregate':
  - '@project':
      metadata:
        annotations:
          testannotation: $.testannotation`
		var p pipeline.Pipeline
		err := yaml.Unmarshal([]byte(jsonData), &p)
		Expect(err).NotTo(HaveOccurred())

		config := Config{
			Sources: []Source{{
				Resource: Resource{
					Kind: "view",
				},
			}},
			Pipeline: p,
			Target: Target{
				Resource: Resource{
					Kind: "view",
				},
				Type: "Patcher",
			},
		}

		mgr, err := manager.NewFakeManager(runtimeManager.Options{Logger: logger})
		Expect(err).NotTo(HaveOccurred())
		Expect(mgr).NotTo(BeNil())

		go func() { mgr.Start(ctx) }() // will stop with a context cancelled error

		// Create controller overriding the request processor
		request := Request{}
		c, err := New(mgr, config, Options{
			Processor: func(_ context.Context, _ *Controller, req Request) error {
				request = req
				return nil
			},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(c.GetName()).To(Equal(config.Target.Resource.String(mgr)))

		// push a view object via the view cache
		vcache := mgr.GetCompositeCache().GetViewCache()
		Expect(vcache).NotTo(BeNil())

		err = vcache.Add(view)
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() bool {
			return request != Request{}
		}, timeout, interval).Should(BeTrue())
		Expect(request).To(Equal(Request{
			GVK:       viewv1a1.NewGVK("view"),
			Namespace: "default",
			Name:      "viewname",
			EventType: cache.Added,
		}))
	})

	FIt("should implement a basic controller on view objects", func() {
		jsonData := `
'@aggregate':
  - '@project':
      metadata:
        name: $.metadata.name
        namespace: $.metadata.namespace
        annotations:
          testannotation: $.testannotation`
		var p pipeline.Pipeline
		err := yaml.Unmarshal([]byte(jsonData), &p)
		Expect(err).NotTo(HaveOccurred())

		config := Config{
			Sources: []Source{{
				Resource: Resource{
					Kind: "view",
				},
			}},
			Pipeline: p,
			Target: Target{
				Resource: Resource{
					Kind: "view",
				},
				Type: "Patcher",
			},
		}

		mgr, err := manager.NewFakeManager(runtimeManager.Options{Logger: logger})
		Expect(err).NotTo(HaveOccurred())
		Expect(mgr).NotTo(BeNil())

		go func() { mgr.Start(ctx) }() // will stop with a context cancelled error

		// Create controller
		c, err := New(mgr, config, Options{})
		Expect(err).NotTo(HaveOccurred())
		Expect(c.GetName()).To(Equal(config.Target.Resource.String(mgr)))

		// Obtain the viewcache
		vcache := mgr.GetCompositeCache().GetViewCache()
		Expect(vcache).NotTo(BeNil())

		// Push a view object via the view cache
		err = vcache.Add(view)
		Expect(err).NotTo(HaveOccurred())

		res := view.DeepCopy()
		anns := res.GetAnnotations()
		if anns == nil {
			anns = map[string]string{}
		}
		anns["testannotation"] = "test-value"
		res.SetAnnotations(anns)
		Eventually(func() bool {
			get := object.DeepCopy(view)
			err := vcache.Get(ctx, client.ObjectKeyFromObject(view), get)
			if err != nil {
				return false
			}
			return object.DeepEqual(get, res)
		}, timeout, interval).Should(BeTrue())

		// Push a view object via the view cache
		Expect(unstructured.SetNestedField(view.Object, "test-value-2", "testannotation")).NotTo(HaveOccurred())
		err = vcache.Add(view)
		Expect(err).NotTo(HaveOccurred())

		// Create a viewcache watcher
		watcher, err := vcache.Watch(ctx, object.NewViewObjectList("view"))
		Expect(err).NotTo(HaveOccurred())

		res = view.DeepCopy()
		anns = res.GetAnnotations()
		if anns == nil {
			anns = map[string]string{}
		}
		anns["testannotation"] = "test-value-2"
		res.SetAnnotations(anns)

		Eventually(func() bool {
			event, ok := tryWatchWatcher(watcher, interval)
			if !ok || event.Type != watch.Added {
				return false
			}
			fmt.Println("XXXXXXXXXXXXXXXXXXXX")
			fmt.Println(object.Dump(event.Object.(object.Object)))
			fmt.Println(object.Dump(res))
			return object.DeepEqual(event.Object.(object.Object), res)
		}, timeout, interval).Should(BeTrue())

		// Expect(err).NotTo(HaveOccurred())
		// name := fmt.Sprintf("%s:view", viewv1a1.GroupVersion.String())
		// Expect(c.GetName()).To(Equal(name))

		// watcher

		// tracker := mgr.GetObjectTracker()
		// err = tracker.Add(pod)
		// Expect(err).NotTo(HaveOccurred())

		// // this should induce an event on the pod itself
		// gvr := schema.GroupVersionResource{
		// 	Group:    "",
		// 	Version:  "v1",
		// 	Resource: "pods", // Resource does not equal Kind!
		// }
		// watcher, err := tracker.Watch(gvr, "testns")
		// Expect(err).NotTo(HaveOccurred())

		// event, ok := tryWatchWatcher(watcher, interval)
		// Expect(ok).To(BeTrue())
		// Expect(event.Type).To(Equal(watch.Added))
		// anns := event.Object.(object.Object).GetAnnotations()
		// rv, ok := anns["recourceVersion"]
		// Expect(ok).To(BeTrue())
		// Expect(rv).To(Equal("999"))
	})

	It("should implement a basic controller on native objects", func() {
		jsonData := `
'@aggregate':
  - '@project':
      annotations:
        - resourceVersion: $.metadata.resourceVersion`
		var p pipeline.Pipeline
		err := yaml.Unmarshal([]byte(jsonData), &p)
		Expect(err).NotTo(HaveOccurred())

		group, version := "", "v1"
		config := Config{
			Sources: []Source{{
				Resource: Resource{
					Group:   &group,
					Version: &version,
					Kind:    "Pod",
				},
			}},
			Pipeline: p,
			Target: Target{
				Resource: Resource{
					Group:   &group,
					Version: &version,
					Kind:    "Pod",
				},
				Type: "Patcher",
			},
		}

		mgr, err := manager.NewFakeManager(runtimeManager.Options{Logger: logger})
		Expect(err).NotTo(HaveOccurred())
		Expect(mgr).NotTo(BeNil())

		go func() { mgr.Start(ctx) }() // will stop with a context cancelled error

		c, err := New(mgr, config, Options{})
		Expect(err).NotTo(HaveOccurred())
		Expect(c.GetName()).To(Equal("core/v1:Pod"))

		// push a pod via the tracker
		tracker := mgr.GetObjectTracker()
		err = tracker.Add(pod)
		Expect(err).NotTo(HaveOccurred())

		// this should induce an event on the pod itself
		gvr := schema.GroupVersionResource{
			Group:    "",
			Version:  "v1",
			Resource: "pods", // Resource does not equal Kind!
		}
		watcher, err := tracker.Watch(gvr, "testns")
		Expect(err).NotTo(HaveOccurred())

		event, ok := tryWatchWatcher(watcher, interval)
		Expect(ok).To(BeTrue())
		Expect(event.Type).To(Equal(watch.Added))
		anns := event.Object.(object.Object).GetAnnotations()
		rv, ok := anns["recourceVersion"]
		Expect(ok).To(BeTrue())
		Expect(rv).To(Equal("999"))

	})
})

func tryWatchReq(watcher chan Request, d time.Duration) (Request, bool) {
	select {
	case req := <-watcher:
		return req, true
	case <-time.After(d):
		return Request{}, false
	}
}

func tryWatchWatcher(watcher watch.Interface, d time.Duration) (watch.Event, bool) {
	select {
	case event := <-watcher.ResultChan():
		return event, true
	case <-time.After(d):
		return watch.Event{}, false
	}
}

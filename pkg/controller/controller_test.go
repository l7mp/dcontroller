package controller

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap/zapcore"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	runtimeCache "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	runtimeManager "sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/yaml"

	opv1a1 "hsnlab/dcontroller/pkg/api/operator/v1alpha1"
	viewv1a1 "hsnlab/dcontroller/pkg/api/view/v1alpha1"
	"hsnlab/dcontroller/pkg/cache"
	"hsnlab/dcontroller/pkg/manager"
	"hsnlab/dcontroller/pkg/object"
	"hsnlab/dcontroller/pkg/reconciler"
)

const (
	timeout       = time.Second * 1
	interval      = time.Millisecond * 50
	retryInterval = time.Millisecond * 100
)

var (
	loglevel = -10
	// loglevel = -3
	logger = zap.New(zap.UseFlagOptions(&zap.Options{
		Development:     true,
		DestWriter:      GinkgoWriter,
		StacktraceLevel: zapcore.Level(10),
		TimeEncoder:     zapcore.RFC3339NanoTimeEncoder,
		Level:           zapcore.Level(loglevel),
	}))
	log  = logger.WithName("test")
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

func TestController(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Controller")
}

var _ = Describe("Controller", func() {
	var (
		pod, view                    object.Object
		dep1, dep2, pod1, pod2, pod3 object.Object
		ctx                          context.Context
		cancel                       context.CancelFunc
	)

	BeforeEach(func() {
		ctx, cancel = context.WithCancel(logr.NewContext(context.Background(), logger))
		pod = object.New()
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

		// for the join tests
		pod1 = object.NewViewObject("pod")
		object.SetContent(pod1, map[string]any{
			"spec": map[string]any{
				"image":  "image1",
				"parent": "dep1",
			},
		})
		object.SetName(pod1, "default", "pod1")
		pod1.SetLabels(map[string]string{"app": "app1"})

		pod2 = object.NewViewObject("pod")
		object.SetContent(pod2, map[string]any{
			"spec": map[string]any{
				"image":  "image2",
				"parent": "dep1",
			},
		})
		object.SetName(pod2, "other", "pod2")
		pod2.SetLabels(map[string]string{"app": "app2"})

		pod3 = object.NewViewObject("pod")
		object.SetContent(pod3, map[string]any{
			"spec": map[string]any{
				"image":  "image1",
				"parent": "dep2",
			},
		})
		object.SetName(pod3, "default", "pod3")
		pod3.SetLabels(map[string]string{"app": "app1"})

		dep1 = object.NewViewObject("dep")
		object.SetContent(dep1, map[string]any{
			"spec": map[string]any{
				"replicas": int64(3),
			},
		})
		object.SetName(dep1, "default", "dep1")
		dep1.SetLabels(map[string]string{"app": "app1"})

		dep2 = object.NewViewObject("dep")
		object.SetContent(dep2, map[string]any{
			"spec": map[string]any{
				"replicas": int64(1),
			},
		})
		object.SetName(dep2, "default", "dep2")
		dep2.SetLabels(map[string]string{"app": "app2"})
	})

	AfterEach(func() {
		cancel()
	})

	Describe("With Controllers using simple aggregation pipelines", func() {
		It("should generate watch requests on the target view", func() {
			jsonData := `
'@aggregate':
  - '@project':
      metadata:
        annotations:
          testannotation: $.testannotation`
			var p opv1a1.Pipeline
			err := yaml.Unmarshal([]byte(jsonData), &p)
			Expect(err).NotTo(HaveOccurred())

			config := opv1a1.Controller{
				Name: "test",
				Sources: []opv1a1.Source{{
					Resource: opv1a1.Resource{
						Kind: "view",
					},
				}},
				Pipeline: p,
				Target: opv1a1.Target{
					Resource: opv1a1.Resource{
						Kind: "view",
					},
					Type: "Patcher",
				},
			}

			mgr, err := manager.NewFakeManager(runtimeManager.Options{Logger: logger})
			Expect(err).NotTo(HaveOccurred())
			Expect(mgr).NotTo(BeNil())

			go func() { mgr.Start(ctx) }()

			// Create controller overriding the request processor
			request := reconciler.Request{}
			c, err := New(mgr, config, Options{
				Processor: func(_ context.Context, _ *Controller, req reconciler.Request) error {
					request = req
					return nil
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(c.GetName()).To(Equal("test"))

			// push a view object via the view cache
			vcache := mgr.GetCompositeCache().GetViewCache()
			Expect(vcache).NotTo(BeNil())

			err = vcache.Add(view)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() bool {
				return request != reconciler.Request{}
			}, timeout, retryInterval).Should(BeTrue())
			Expect(request).To(Equal(reconciler.Request{
				GVK:       viewv1a1.NewGVK("view"),
				Namespace: "default",
				Name:      "viewname",
				EventType: cache.Added,
			}))
		})

		It("should implement a basic controller on view objects", func() {
			jsonData := `
'@aggregate':
  - '@project':
      metadata:
        name: $.metadata.name
        namespace: $.metadata.namespace
        annotations:
          testannotation: $.testannotation`
			var p opv1a1.Pipeline
			err := yaml.Unmarshal([]byte(jsonData), &p)
			Expect(err).NotTo(HaveOccurred())

			config := opv1a1.Controller{
				Name: "test",
				Sources: []opv1a1.Source{{
					Resource: opv1a1.Resource{
						Kind: "view",
					},
				}},
				Pipeline: p,
				Target: opv1a1.Target{
					Resource: opv1a1.Resource{
						Kind: "view",
					},
					Type: "Patcher",
				},
			}

			mgr, err := manager.NewFakeManager(runtimeManager.Options{Logger: logger})
			Expect(err).NotTo(HaveOccurred())
			Expect(mgr).NotTo(BeNil())

			// Create controller
			c, err := New(mgr, config, Options{})
			Expect(err).NotTo(HaveOccurred())
			Expect(c.GetName()).To(Equal("test"))

			// Obtain the viewcache
			vcache := mgr.GetCompositeCache().GetViewCache()
			Expect(vcache).NotTo(BeNil())

			go func() { mgr.Start(ctx) }()

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
			get := object.New()
			Eventually(func() bool {
				get = object.DeepCopy(view)
				err := vcache.Get(ctx, client.ObjectKeyFromObject(view), get)
				if err != nil {
					return false
				}
				return object.DeepEqual(get, res)
			}, timeout, retryInterval).Should(BeTrue())

			// Push a view object via the view cache
			Expect(unstructured.SetNestedField(view.Object, "test-value-2", "testannotation")).NotTo(HaveOccurred())
			err = vcache.Update(get, view)
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
				if !ok || event.Type != watch.Modified {
					return false
				}

				return object.DeepEqual(event.Object.(object.Object), res)
			}, timeout, retryInterval).Should(BeTrue())
		})

		It("should implement a basic controller on native objects", func() {
			mgr, err := manager.NewFakeManager(runtimeManager.Options{Logger: logger})
			Expect(err).NotTo(HaveOccurred())
			Expect(mgr).NotTo(BeNil())

			go func() { mgr.Start(ctx) }()

			yamlData := `
name: test
sources:
  - apiGroup: ""
    kind: Pod
pipeline:
  '@aggregate':
    - '@project':
        metadata:
          name: "$.metadata.name"
          namespace: "$.metadata.namespace"
          annotations:
            containerNames:
              '@concat': [ '@map': ["$$.name", $.spec.containers] ]
target:
  apiGroup: ""
  kind: Pod
  type: Patcher`

			var config opv1a1.Controller
			err = yaml.Unmarshal([]byte(yamlData), &config)
			Expect(err).NotTo(HaveOccurred())

			c, err := New(mgr, config, Options{})
			Expect(err).NotTo(HaveOccurred())
			Expect(c.GetName()).To(Equal("test"))

			// Add the object to the fake runtime client object tracker, that's how the
			// controller's target will be able to modify it
			tracker := mgr.GetObjectTracker()
			gvr := schema.GroupVersionResource{
				Group:    "",
				Version:  "v1",
				Resource: "pods", // Resource does not equal Kind!
			}
			err = tracker.Add(pod)
			Expect(err).NotTo(HaveOccurred())

			// sanity check
			Eventually(func() bool {
				_, err := tracker.Get(gvr, "testns", "testpod")
				logger.Error(err, "tracker error")
				return err == nil
			}, timeout, retryInterval).Should(BeTrue())

			// Then push the object via the fakeRuntimeCache.Add function (the tracker
			// would not work since the split client takes all watches from the cache,
			// not the tracker)
			rcache := mgr.GetRuntimeCache()
			err = rcache.Add(pod)
			Expect(err).NotTo(HaveOccurred())

			// sanity check
			gvk := schema.GroupVersionKind{
				Group:   "",
				Version: "v1",
				Kind:    "Pod", // Resource does not equal Kind!
			}
			// sanity check
			Eventually(func() bool {
				g := object.New()
				g.SetGroupVersionKind(gvk)
				object.SetName(g, "testns", "testpod")
				err := rcache.Get(ctx, client.ObjectKeyFromObject(g), g)
				return err == nil
			}, timeout, retryInterval).Should(BeTrue())

			// Check whether the controller updated the object
			var get *corev1.Pod
			Eventually(func() bool {
				g, err := tracker.Get(gvr, "testns", "testpod")
				if err != nil {
					return false
				}
				// for some strange reason we get a structured object back
				ok := false
				get, ok = g.(*corev1.Pod)
				if !ok {
					return false
				}

				anns := get.GetAnnotations()
				if anns == nil {
					return false
				}
				a, ok := anns["containerNames"]
				if !ok {
					return false
				}
				return a == "nginx"
			}, timeout, retryInterval).Should(BeTrue())

			// add a new container

			pod1 := object.New()
			content, err := runtime.DefaultUnstructuredConverter.ToUnstructured(get)
			Expect(err).NotTo(HaveOccurred())
			pod1.SetUnstructuredContent(content)
			pod1.SetGroupVersionKind(schema.GroupVersionKind{
				Group:   "",
				Version: "v1",
				Kind:    "Pod",
			})

			pod2 := object.DeepCopy(pod1)
			cs, ok, err := unstructured.NestedSlice(pod2.Object, "spec", "containers")
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeTrue())
			cs = append(cs, map[string]any{"name": "envoy", "image": "envoy"})
			err = unstructured.SetNestedSlice(pod2.Object, cs, "spec", "containers")
			Expect(err).NotTo(HaveOccurred())

			err = tracker.Update(gvr, pod2, "testns")
			Expect(err).NotTo(HaveOccurred())

			err = rcache.Update(pod1, pod2)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() bool {
				g, err := tracker.Get(gvr, "testns", "testpod")
				if err != nil {
					return false
				}
				// for some strange reason we get a structured object back
				ok := false
				get, ok = g.(*corev1.Pod)
				if !ok {
					return false
				}

				anns := get.GetAnnotations()
				if anns == nil {
					return false
				}
				a, ok := anns["containerNames"]
				if !ok {
					return false
				}
				return a == "nginxenvoy"
			}, timeout, retryInterval).Should(BeTrue())
		})

		It("should reject a controller with no sources", func() {
			mgr, err := manager.NewFakeManager(runtimeManager.Options{Logger: logger})
			Expect(err).NotTo(HaveOccurred())
			Expect(mgr).NotTo(BeNil())

			go func() { mgr.Start(ctx) }()

			yamlData := `
name: test
pipeline:
  '@aggregate':
    - '@project':
        "$.metadata": "$.metadata"
target:
  apiGroup: ""
  kind: Pod
  type: Patcher`

			var config opv1a1.Controller
			err = yaml.Unmarshal([]byte(yamlData), &config)
			Expect(err).NotTo(HaveOccurred())

			_, err = New(mgr, config, Options{})
			Expect(err).To(HaveOccurred())
		})

		It("should reject a controller with no target", func() {
			mgr, err := manager.NewFakeManager(runtimeManager.Options{Logger: logger})
			Expect(err).NotTo(HaveOccurred())
			Expect(mgr).NotTo(BeNil())

			go func() { mgr.Start(ctx) }()

			yamlData := `
name: test
sources:
  - apiGroup: ""
    kind: Pod
pipeline:
  '@aggregate':
    - '@project':
        "$.metadata": "$.metadata"`

			var config opv1a1.Controller
			err = yaml.Unmarshal([]byte(yamlData), &config)
			Expect(err).NotTo(HaveOccurred())

			_, err = New(mgr, config, Options{})
			Expect(err).To(HaveOccurred())
		})

		It("should reject a controller with an invalid pipeline", func() {
			mgr, err := manager.NewFakeManager(runtimeManager.Options{Logger: logger})
			Expect(err).NotTo(HaveOccurred())
			Expect(mgr).NotTo(BeNil())

			go func() { mgr.Start(ctx) }()

			yamlData := `
sources:
  - apiGroup: ""
    kind: Pod
pipeline:
  '@aggregate': "AAAAAAAAAAAAAAAAAA"
target:
  apiGroup: ""
  kind: Pod
  type: Patcher`

			var config opv1a1.Controller
			err = yaml.Unmarshal([]byte(yamlData), &config)
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("With complex Controllers", func() {
		It("should implement a controller with a join pipeline", func() {
			mgr, err := manager.NewFakeManager(runtimeManager.Options{Logger: logger})
			Expect(err).NotTo(HaveOccurred())
			Expect(mgr).NotTo(BeNil())

			yamlData := `
# This will create a simple replicaset (rs image), with the joining pods and deployments on
# "pod.spec.parent == dep.name && pod.metadata.namespace == dep.metadata.namespace" condition, with
# the image taken from the pod.spec and the replica count from dep.spec.replicas. Name and
# namespace are the same as those of the deployment.
#
name: test
sources:
  - kind: pod
  - kind: dep
pipeline:
  '@join':
    '@and':
      - '@eq':
        - $.dep.metadata.name
        - $.pod.spec.parent
      - '@eq':
          - $.dep.metadata.namespace
          - $.pod.metadata.namespace
  '@aggregate':
    - '@project':
        "$.metadata": "$.dep.metadata"
        spec:
          image: $.pod.spec.image
          replicas: $.dep.spec.replicas
target:
  kind: rs
  type: Updater`
			var config opv1a1.Controller
			err = yaml.Unmarshal([]byte(yamlData), &config)
			Expect(err).NotTo(HaveOccurred())

			log.V(1).Info("Create controller")
			c, err := New(mgr, config, Options{})
			Expect(err).NotTo(HaveOccurred())
			Expect(c.GetName()).To(Equal("test"))

			log.V(1).Info("Starting the manager")
			go func() { mgr.Start(ctx) }()

			log.V(1).Info("Obtain the viewcache")
			vcache := mgr.GetCompositeCache().GetViewCache()
			Expect(vcache).NotTo(BeNil())

			log.V(1).Info("Push objects view the cache")
			for _, o := range []object.Object{pod1, pod2, pod3, dep1} {
				err = vcache.Add(o)
				Expect(err).NotTo(HaveOccurred())
			}

			log.V(1).Info("Create a viewcache watcher")
			watcher, err := vcache.Watch(ctx, object.NewViewObjectList("rs"))
			Expect(err).NotTo(HaveOccurred())

			// Should obtain one object in the rs view: pod1-dep1
			var rs1 object.Object
			Eventually(func() bool {
				event, ok := tryWatchWatcher(watcher, interval)
				if !ok {
					return false
				}

				rs1, err = getRuntimeObjFromCache(ctx, vcache, "rs", event.Object)
				return err == nil
			}, timeout, retryInterval).Should(BeTrue())

			// should be a single object only
			Eventually(func() bool {
				list := object.NewViewObjectList("rs")
				err := vcache.List(ctx, list)
				Expect(err).NotTo(HaveOccurred())

				return len(list.Items) == 1
			}, timeout, retryInterval).Should(BeTrue())

			Expect(rs1).To(Equal(&unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": "view.dcontroller.io/v1alpha1",
					"kind":       "rs",
					"metadata": map[string]any{
						"name":      "dep1",
						"namespace": "default",
						"labels": map[string]any{
							"app": "app1",
						},
					},
					"spec": map[string]any{
						"replicas": int64(3),
						"image":    "image1",
					},
				},
			}))

			log.V(1).Info("Add dep2 to the cache")
			err = vcache.Add(dep2)

			// Should obtain one object in the rs view: pod3-dep2
			var rs2 object.Object
			Eventually(func() bool {
				event, ok := tryWatchWatcher(watcher, interval)
				if !ok {
					return false
				}

				// we may re-get the first object
				if event.Type != watch.Added {
					return false
				}

				rs2, err = getRuntimeObjFromCache(ctx, vcache, "rs", event.Object)
				return err == nil
			}, timeout, retryInterval).Should(BeTrue())

			// Should be two objects
			Eventually(func() bool {
				list := object.NewViewObjectList("rs")
				err := vcache.List(ctx, list)
				Expect(err).NotTo(HaveOccurred())

				return len(list.Items) == 2
			}, timeout, retryInterval).Should(BeTrue())

			Expect(rs2).To(Equal(&unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": "view.dcontroller.io/v1alpha1",
					"kind":       "rs",
					"metadata": map[string]any{
						"name":      "dep2",
						"namespace": "default",
						"labels": map[string]any{
							"app": "app2",
						},
					},
					"spec": map[string]any{
						"replicas": int64(1),
						"image":    "image1",
					},
				},
			}))

			log.V(1).Info("move dep1 into the \"other\" namespace")
			// this must not be a cache.Update as object's namespacedname changes
			log.V(1).Info("Delete the old dep1 object")
			err = vcache.Delete(dep1)

			// Should should first remove rs pod1-dep1
			Eventually(func() bool {
				event, ok := tryWatchWatcher(watcher, interval)
				if !ok {
					return false
				}

				if event.Type != watch.Deleted {
					return false
				}

				m, err := meta.Accessor(event.Object)
				if err != nil {
					return false
				}
				return m.GetNamespace() == "default" && m.GetName() == "dep1"
			}, timeout, retryInterval).Should(BeTrue())

			// should be 1 object
			Eventually(func() bool {
				list := object.NewViewObjectList("rs")
				err := vcache.List(ctx, list)
				Expect(err).NotTo(HaveOccurred())

				return len(list.Items) == 1
			}, timeout, retryInterval).Should(BeTrue())

			log.V(1).Info("Re-add dep1 with the new namespace")
			dep1.SetNamespace("other")
			err = vcache.Add(dep1)

			// Should obtain one object in the rs view: pod1-dep1
			var rs3 object.Object
			Eventually(func() bool {
				event, ok := tryWatchWatcher(watcher, interval)
				if !ok {
					return false
				}

				if event.Type != watch.Added {
					return false
				}

				rs3, err = getRuntimeObjFromCache(ctx, vcache, "rs", event.Object)
				return err == nil
			}, timeout, retryInterval).Should(BeTrue())

			Expect(rs3).To(Equal(&unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": "view.dcontroller.io/v1alpha1",
					"kind":       "rs",
					"metadata": map[string]any{
						"name":      "dep1",
						"namespace": "other",
						"labels": map[string]any{
							"app": "app1",
						},
					},
					"spec": map[string]any{
						"replicas": int64(3),
						"image":    "image2",
					},
				},
			}))
		})

		It("should implement 2 controller chain with complex pipelines", func() {
			mgr, err := manager.NewFakeManager(runtimeManager.Options{Logger: logger})
			Expect(err).NotTo(HaveOccurred())
			Expect(mgr).NotTo(BeNil())

			// Start manager late
			go func() { mgr.Start(ctx) }()

			// 1. create replicasets by joining on the "app" label
			// 2. write the rs name back into the pod as the annotation "rs-name"

			yamlData1 := `
name: rs
sources:
  - kind: pod
  - kind: dep
pipeline:
  '@join':
    '@eq': ["$.pod.metadata.labels.app", "$.dep.metadata.labels.app"]
  '@aggregate':
    - '@project':
        metadata:
          name:
           "@concat":
             - "$.dep.metadata.namespace"
             - ":"
             - "$.dep.metadata.name"
             - "-"
             - "$.pod.metadata.namespace"
             - ":"
             - "$.pod.metadata.name"
          namespace: "$.dep.metadata.namespace"
        spec:
          podName: "$.pod.metadata.name"
target:
  kind: rs
  type: Updater`
			var config1 opv1a1.Controller
			err = yaml.Unmarshal([]byte(yamlData1), &config1)
			Expect(err).NotTo(HaveOccurred())

			// Create controller1
			_, err = New(mgr, config1, Options{})
			Expect(err).NotTo(HaveOccurred())

			yamlData2 := `
name: pod-patcher
sources:
  - kind: rs
  - kind: pod
pipeline:
  '@join':
    '@eq': ["$.rs.spec.podName", "$.pod.metadata.name"]
  '@aggregate':
    - '@project':
        metadata:
          name: $.pod.metadata.name
          namespace: $.pod.metadata.namespace
          annotations:
            rs-name: "$.rs.metadata.name"
target:
  kind: pod
  type: Patcher`
			var config2 opv1a1.Controller
			err = yaml.Unmarshal([]byte(yamlData2), &config2)
			Expect(err).NotTo(HaveOccurred())

			// Create controller2
			_, err = New(mgr, config2, Options{})
			Expect(err).NotTo(HaveOccurred())

			// Obtain the viewcache
			vcache := mgr.GetCompositeCache().GetViewCache()
			Expect(vcache).NotTo(BeNil())

			// Push objects view the cache
			for _, o := range []object.Object{pod1, dep1, dep2} {
				err = vcache.Add(o)
				Expect(err).NotTo(HaveOccurred())
			}

			// Create a viewcache watcher for the kind rs
			watcher, err := vcache.Watch(ctx, object.NewViewObjectList("pod"))
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() bool {
				event, ok := tryWatchWatcher(watcher, interval)
				if !ok {
					return false
				}

				pod, err = getRuntimeObjFromCache(ctx, vcache, "pod", event.Object)
				if err != nil {
					return false

				}

				// we get all the pod watch events: eventually one should have the
				// new annotation
				if pod.GetNamespace() != "default" || pod.GetName() != "pod1" {
					return false
				}
				anns := pod.GetAnnotations()
				if len(anns) != 1 {
					return false
				}

				rsName, ok := anns["rs-name"]
				return ok && rsName == "default:dep1-default:pod1"

			}, timeout, retryInterval).Should(BeTrue())

			// should be a single object only
			Eventually(func() bool {
				list := object.NewViewObjectList("rs")
				err := vcache.List(ctx, list)
				Expect(err).NotTo(HaveOccurred())

				return len(list.Items) == 1
			}, timeout, retryInterval).Should(BeTrue())
		})
	})
})

func tryWatchWatcher(watcher watch.Interface, d time.Duration) (watch.Event, bool) {
	select {
	case event := <-watcher.ResultChan():
		return event, true
	case <-time.After(d):
		return watch.Event{}, false
	}
}

func getRuntimeObjFromCache(ctx context.Context, c runtimeCache.Cache, kind string, obj runtime.Object) (object.Object, error) {
	m, err := meta.Accessor(obj)
	if err != nil {
		return nil, err
	}

	key := types.NamespacedName{
		Namespace: m.GetNamespace(),
		Name:      m.GetName(),
	}
	g := object.NewViewObject(kind)
	err = c.Get(ctx, key, g)
	if err != nil {
		return nil, err
	}

	return g.DeepCopy(), nil
}

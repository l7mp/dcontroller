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
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	runtimeManager "sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/yaml"

	opv1a1 "github.com/l7mp/dcontroller/pkg/api/operator/v1alpha1"
	viewv1a1 "github.com/l7mp/dcontroller/pkg/api/view/v1alpha1"
	"github.com/l7mp/dcontroller/pkg/composite"
	"github.com/l7mp/dcontroller/pkg/manager"
	"github.com/l7mp/dcontroller/pkg/object"
)

const (
	timeout       = time.Second * 5
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

		view = object.NewViewObject("test", "view")
		object.SetName(view, "default", "viewname")
		object.SetContent(view, map[string]any{"testannotation": "test-value"})

		// for the join tests
		pod1 = object.NewViewObject("test", "pod")
		object.SetContent(pod1, map[string]any{
			"spec": map[string]any{
				"image":  "image1",
				"parent": "dep1",
			},
		})
		object.SetName(pod1, "default", "pod1")
		pod1.SetLabels(map[string]string{"app": "app1"})

		pod2 = object.NewViewObject("test", "pod")
		object.SetContent(pod2, map[string]any{
			"spec": map[string]any{
				"image":  "image2",
				"parent": "dep1",
			},
		})
		object.SetName(pod2, "other", "pod2")
		pod2.SetLabels(map[string]string{"app": "app2"})

		pod3 = object.NewViewObject("test", "pod")
		object.SetContent(pod3, map[string]any{
			"spec": map[string]any{
				"image":  "image1",
				"parent": "dep2",
			},
		})
		object.SetName(pod3, "default", "pod3")
		pod3.SetLabels(map[string]string{"app": "app1"})

		dep1 = object.NewViewObject("test", "dep")
		object.SetContent(dep1, map[string]any{
			"spec": map[string]any{
				"replicas": int64(3),
			},
		})
		object.SetName(dep1, "default", "dep1")
		dep1.SetLabels(map[string]string{"app": "app1"})

		dep2 = object.NewViewObject("test", "dep")
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

	Describe("With Controllers using simple pipelines", func() {
		It("should implement a basic controller on view objects", func() {
			jsonData := `
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
			c, err := NewDeclarative(mgr, "test", config, Options{})
			Expect(err).NotTo(HaveOccurred())
			Expect(c.GetName()).To(Equal("test"))

			// Obtain the viewcache
			vcache := mgr.GetCompositeCache().GetViewCache()
			Expect(vcache).NotTo(BeNil())

			go func() { mgr.Start(ctx) }()

			// Push a view object via the view cache
			err = vcache.Add(view)
			Expect(err).NotTo(HaveOccurred())
			object.WithUID(view)

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
			watcher, err := vcache.Watch(ctx, composite.NewViewObjectList("test", "view"))
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

			c, err := NewDeclarative(mgr, "test", config, Options{})
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
  - '@project':
      "$.metadata": "$.metadata"
target:
  apiGroup: ""
  kind: Pod
  type: Patcher`

			var config opv1a1.Controller
			err = yaml.Unmarshal([]byte(yamlData), &config)
			Expect(err).NotTo(HaveOccurred())

			_, err = NewDeclarative(mgr, "test", config, Options{})
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
  - '@project':
      "$.metadata": "$.metadata"`

			var config opv1a1.Controller
			err = yaml.Unmarshal([]byte(yamlData), &config)
			Expect(err).NotTo(HaveOccurred())

			_, err = NewDeclarative(mgr, "test", config, Options{})
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
  '@dummmy': "AAAAAAAAAAAAAAAAAA"
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
  - '@join':
      '@and':
        - '@eq':
          - $.dep.metadata.name
          - $.pod.spec.parent
        - '@eq':
            - $.dep.metadata.namespace
            - $.pod.metadata.namespace
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
			c, err := NewDeclarative(mgr, "test", config, Options{})
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
			watcher, err := vcache.Watch(ctx, composite.NewViewObjectList("test", "rs"))
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
				list := composite.NewViewObjectList("test", "rs")
				err := vcache.List(ctx, list)
				if err != nil {
					return false
				}
				return len(list.Items) == 1
			}, timeout, retryInterval).Should(BeTrue())

			// Should obtain one object in the rs view: pod1-dep1
			object.RemoveUID(rs1)
			Expect(rs1).To(Equal(&unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": "test.view.dcontroller.io/v1alpha1",
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

			// this must not be a cache.Update as object's namespacedname changes
			log.V(1).Info("Delete the old dep1 object")
			err = vcache.Delete(dep1)
			Expect(err).NotTo(HaveOccurred())

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

			// should be zero objects
			Eventually(func() bool {
				list := composite.NewViewObjectList("test", "rs")
				err := vcache.List(ctx, list)
				if err != nil {
					return false
				}

				return len(list.Items) == 0
			}, timeout, retryInterval).Should(BeTrue())

			log.V(1).Info("Re-add dep1 with the new namespace")
			dep1.SetNamespace("other")
			err = vcache.Add(dep1)

			// Should obtain one object in the rs view: pod2-dep1
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

			object.RemoveUID(rs3)
			Expect(rs3).To(Equal(&unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": "test.view.dcontroller.io/v1alpha1",
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

		It("should implement 2 compex parallel controller pipelines", func() {
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
  - '@join':
      '@eq': ["$.pod.metadata.labels.app", "$.dep.metadata.labels.app"]
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
			_, err = NewDeclarative(mgr, "test", config1, Options{})
			Expect(err).NotTo(HaveOccurred())

			yamlData2 := `
name: pod-patcher
sources:
  - kind: rs
  - kind: pod
pipeline:
  - '@join':
      '@eq': ["$.rs.spec.podName", "$.pod.metadata.name"]
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
			_, err = NewDeclarative(mgr, "test", config2, Options{})
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
			watcher, err := vcache.Watch(ctx, composite.NewViewObjectList("test", "pod"))
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
				list := composite.NewViewObjectList("test", "rs")
				err := vcache.List(ctx, list)
				Expect(err).NotTo(HaveOccurred())

				return len(list.Items) == 1
			}, timeout, retryInterval).Should(BeTrue())
		})
	})

	Describe("With Controllers triggered by a virtual source", func() {
		It("should generate watch requests on the target view using a OneShot source", func() {
			jsonData := `
name: one-shot-controller
sources:
  - type: OneShot
    kind: InitialTrigger
pipeline:
 - '@project':
     metadata:
       name: test-name
       namespace: ns
target:
  kind: test-target`
			var config opv1a1.Controller
			err := yaml.Unmarshal([]byte(jsonData), &config)
			Expect(err).NotTo(HaveOccurred())

			mgr, err := manager.NewFakeManager(runtimeManager.Options{Logger: logger})
			Expect(err).NotTo(HaveOccurred())
			Expect(mgr).NotTo(BeNil())

			go func() { mgr.Start(ctx) }()

			errorChan := make(chan error, 1)
			defer close(errorChan)
			ctrl, err := NewDeclarative(mgr, "test", config, Options{ErrorChan: errorChan})
			Expect(err).NotTo(HaveOccurred())
			Expect(ctrl.GetName()).To(Equal("one-shot-controller"))

			vcache := mgr.GetCompositeCache().GetViewCache()
			Expect(vcache).NotTo(BeNil())
			watcher, err := vcache.Watch(ctx, composite.NewViewObjectList("test", "test-target"))
			Expect(err).NotTo(HaveOccurred())

			event := watch.Event{}
			Eventually(func() bool {
				var ok bool
				event, ok = tryWatchWatcher(watcher, interval)
				return ok && event.Type == watch.Added
			}, timeout, retryInterval).Should(BeTrue())

			obj, ok := event.Object.(object.Object)
			Expect(ok).To(BeTrue())
			Expect(obj.GroupVersionKind()).To(Equal(viewv1a1.GroupVersionKind("test", "test-target")))
			Expect(obj.GetName()).To(Equal("test-name"))
			Expect(obj.GetNamespace()).To(Equal("ns"))
		})

		It("should generate watch requests on the target view using a Periodoc source", func() {
			jsonData := `
name: test-controller
sources:
  - type: OneShot
    kind: InitialTrigger
  - type: Periodic
    kind: PeriodicTrigger
    parameters:
      period: "20ms"
pipeline:
 - '@project':
     metadata:
       name: test-name
       namespace: ns
target:
  kind: test-target`
			var config opv1a1.Controller
			err := yaml.Unmarshal([]byte(jsonData), &config)
			Expect(err).NotTo(HaveOccurred())

			mgr, err := manager.NewFakeManager(runtimeManager.Options{Logger: logger})
			Expect(err).NotTo(HaveOccurred())
			Expect(mgr).NotTo(BeNil())

			go func() { mgr.Start(ctx) }()

			errorChan := make(chan error, 1)
			ctrl, err := NewDeclarative(mgr, "test", config, Options{ErrorChan: errorChan})
			Expect(err).NotTo(HaveOccurred())
			Expect(ctrl.GetName()).To(Equal("test-controller"))
			c := ctrl.(*DeclarativeController)

			// Create a viewcache watcher
			vcache := mgr.GetCompositeCache().GetViewCache()
			Expect(vcache).NotTo(BeNil())
			watcher, err := vcache.Watch(ctx, composite.NewViewObjectList("test", "test-target"))
			Expect(err).NotTo(HaveOccurred())

			// wait for multiple events - initial from oneshot, then periodic SoW reconciliations
			counter := 0
			event := watch.Event{}
			for i := 0; i < 3; i++ {
				var ok bool
				event, ok = tryWatchWatcher(watcher, interval)
				if ok && (event.Type == watch.Modified || event.Type == watch.Added) {
					counter++
					// remove the generated object so that the periodic trigger
					// can redo it
					obj := object.NewViewObject("test", "test-target")
					object.SetName(obj, "ns", "test-name")
					err = vcache.Delete(obj)
					Expect(err).NotTo(HaveOccurred())

					// IMPORTANT: Also delete from the pipeline's target cache to keep it synchronized.
					targetCache := c.pipeline.GetTargetCache()
					Expect(targetCache).NotTo(BeNil())
					err = targetCache.Delete(obj)
					Expect(err).NotTo(HaveOccurred())
				}
			}

			Expect(counter).To(BeNumerically(">=", 2), "should have at least initial + 1 periodic reconciliation")
			obj, ok := event.Object.(object.Object)
			Expect(ok).To(BeTrue())
			Expect(obj.GroupVersionKind()).To(Equal(viewv1a1.GroupVersionKind("test", "test-target")))
			Expect(obj.GetName()).To(Equal("test-name"))
			Expect(obj.GetNamespace()).To(Equal("ns"))
		})

		It("should remove stale objects during periodic reconciliation", func() {
			jsonData := `
name: test-controller
sources:
  - type: OneShot
    kind: InitialTrigger
  - type: Periodic
    kind: PeriodicTrigger
    parameters:
      period: "20ms"
pipeline:
 - '@project':
     metadata:
       name: from-source
       namespace: ns
target:
  kind: test-target`
			var config opv1a1.Controller
			err := yaml.Unmarshal([]byte(jsonData), &config)
			Expect(err).NotTo(HaveOccurred())

			mgr, err := manager.NewFakeManager(runtimeManager.Options{Logger: logger})
			Expect(err).NotTo(HaveOccurred())
			Expect(mgr).NotTo(BeNil())

			go func() { mgr.Start(ctx) }()

			errorChan := make(chan error, 1)
			ctrl, err := NewDeclarative(mgr, "test", config, Options{ErrorChan: errorChan})
			Expect(err).NotTo(HaveOccurred())
			c := ctrl.(*DeclarativeController)

			// Create a viewcache watcher
			vcache := mgr.GetCompositeCache().GetViewCache()
			Expect(vcache).NotTo(BeNil())
			watcher, err := vcache.Watch(ctx, composite.NewViewObjectList("test", "test-target"))
			Expect(err).NotTo(HaveOccurred())

			// Wait for the initial object to be created
			event, ok := tryWatchWatcher(watcher, timeout)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Added))

			// Manually inject a stale object into both caches
			staleObj := object.NewViewObject("test", "test-target")
			object.SetName(staleObj, "ns", "stale-object")
			err = vcache.Add(staleObj)
			Expect(err).NotTo(HaveOccurred())
			targetCache := c.pipeline.GetTargetCache()
			err = targetCache.Add(staleObj)
			Expect(err).NotTo(HaveOccurred())

			// Wait for periodic reconciliation to remove the stale object
			Eventually(func() bool {
				event, ok := tryWatchWatcher(watcher, interval)
				if ok && event.Type == watch.Deleted {
					obj, ok := event.Object.(object.Object)
					if ok && obj.GetName() == "stale-object" {
						return true
					}
				}
				return false
			}, timeout*5, interval).Should(BeTrue(), "periodic reconciliation should remove stale object")
		})

		It("should handle timeout-like scenario with periodic reconciliation", func() {
			// This simulates a scenario where source objects have TTL-like behavior.
			// The pipeline includes a timestamp, and periodic reconciliation ensures
			// objects are recreated even if they expire/get deleted.
			jsonData := `
name: test-controller
sources:
  - type: OneShot
    kind: InitialTrigger
  - type: Periodic
    kind: PeriodicTrigger
    parameters:
      period: "20ms"
pipeline:
 - '@project':
     metadata:
       name: refreshed-object
       namespace: ns
       labels:
         refresh-count: "@string($.metadata.labels[\"dcontroller.io/last-triggered\"])"
target:
  kind: test-target`
			var config opv1a1.Controller
			err := yaml.Unmarshal([]byte(jsonData), &config)
			Expect(err).NotTo(HaveOccurred())

			mgr, err := manager.NewFakeManager(runtimeManager.Options{Logger: logger})
			Expect(err).NotTo(HaveOccurred())
			Expect(mgr).NotTo(BeNil())

			go func() { mgr.Start(ctx) }()

			errorChan := make(chan error, 1)
			ctrl, err := NewDeclarative(mgr, "test", config, Options{ErrorChan: errorChan})
			Expect(err).NotTo(HaveOccurred())
			c := ctrl.(*DeclarativeController)

			// Create a viewcache watcher
			vcache := mgr.GetCompositeCache().GetViewCache()
			Expect(vcache).NotTo(BeNil())
			watcher, err := vcache.Watch(ctx, composite.NewViewObjectList("test", "test-target"))
			Expect(err).NotTo(HaveOccurred())

			// Wait for initial object
			event, ok := tryWatchWatcher(watcher, timeout)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Added))

			// Simulate timeout by deleting the object (mimics expiration)
			obj := object.NewViewObject("test", "test-target")
			object.SetName(obj, "ns", "refreshed-object")
			err = vcache.Delete(obj)
			Expect(err).NotTo(HaveOccurred())
			targetCache := c.pipeline.GetTargetCache()
			err = targetCache.Delete(obj)
			Expect(err).NotTo(HaveOccurred())

			// Wait for deletion event
			event, ok = tryWatchWatcher(watcher, timeout)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Deleted))

			// Periodic reconciliation should recreate the object
			Eventually(func() bool {
				event, ok := tryWatchWatcher(watcher, interval)
				if ok && event.Type == watch.Added {
					obj, ok := event.Object.(object.Object)
					return ok && obj.GetName() == "refreshed-object"
				}
				return false
			}, timeout*5, interval).Should(BeTrue(), "periodic reconciliation should recreate deleted object")
		})

		FIt("should filter source events by label selector", func() {
			// Controller with label selector: only watch pods with app=app1
			jsonData := `
- '@project':
    metadata:
      name: $.metadata.name
      namespace: $.metadata.namespace
    spec:
      image: $.spec.image`
			var p opv1a1.Pipeline
			err := yaml.Unmarshal([]byte(jsonData), &p)
			Expect(err).NotTo(HaveOccurred())

			config := opv1a1.Controller{
				Name: "test-label-selector",
				Sources: []opv1a1.Source{{
					Resource: opv1a1.Resource{
						Kind: "pod",
					},
					LabelSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"app": "app1"},
					},
				}},
				Pipeline: p,
				Target: opv1a1.Target{
					Resource: opv1a1.Resource{
						Kind: "result",
					},
				},
			}

			mgr, err := manager.NewFakeManager(runtimeManager.Options{Logger: logger})
			Expect(err).NotTo(HaveOccurred())

			_, err = NewDeclarative(mgr, "test", config, Options{})
			Expect(err).NotTo(HaveOccurred())

			vcache := mgr.GetCompositeCache().GetViewCache()
			go func() { mgr.Start(ctx) }()

			// Watch target to see which objects get created
			watcher, err := vcache.Watch(ctx, composite.NewViewObjectList("test", "result"))
			Expect(err).NotTo(HaveOccurred())
			defer watcher.Stop()

			// Add pod1 (app=app1) - should be processed
			err = vcache.Add(pod1)
			Expect(err).NotTo(HaveOccurred())

			event, ok := tryWatchWatcher(watcher, timeout)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Added))
			Expect(event.Object.(object.Object).GetName()).To(Equal("pod1"))

			// Add pod2 (app=app2) - should NOT be processed
			err = vcache.Add(pod2)
			Expect(err).NotTo(HaveOccurred())

			// Should not get any event
			_, ok = tryWatchWatcher(watcher, 50*time.Millisecond)
			Expect(ok).To(BeFalse(), "pod2 with app=app2 should be filtered out by label selector")

			// Add pod3 (app=app1) - should be processed
			err = vcache.Add(pod3)
			Expect(err).NotTo(HaveOccurred())

			event, ok = tryWatchWatcher(watcher, timeout)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Added))
			Expect(event.Object.(object.Object).GetName()).To(Equal("pod3"))
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

func getRuntimeObjFromCache(ctx context.Context, c cache.Cache, kind string, obj runtime.Object) (object.Object, error) {
	m, err := meta.Accessor(obj)
	if err != nil {
		return nil, err
	}

	key := types.NamespacedName{
		Namespace: m.GetNamespace(),
		Name:      m.GetName(),
	}
	g := object.NewViewObject("test", kind)
	err = c.Get(ctx, key, g)
	if err != nil {
		return nil, err
	}

	return g.DeepCopy(), nil
}

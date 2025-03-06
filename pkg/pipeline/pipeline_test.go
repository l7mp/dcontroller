package pipeline

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap/zapcore"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/json"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/yaml"

	"github.com/hsnlab/dcontroller/internal/testutils"
	opv1a1 "github.com/hsnlab/dcontroller/pkg/api/operator/v1alpha1"
	viewv1a1 "github.com/hsnlab/dcontroller/pkg/api/view/v1alpha1"
	"github.com/hsnlab/dcontroller/pkg/cache"
	"github.com/hsnlab/dcontroller/pkg/object"
)

var (
	emptyView = []gvk{}
	loglevel  = -10
	logger    = zap.New(zap.UseFlagOptions(&zap.Options{
		Development:     true,
		DestWriter:      GinkgoWriter,
		StacktraceLevel: zapcore.Level(3),
		TimeEncoder:     zapcore.RFC3339NanoTimeEncoder,
		Level:           zapcore.Level(loglevel),
	}))
)

func TestPipeline(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Pipeline")
}

var _ = Describe("Pipelines", func() {
	Context("When running a pipeline with a 3-way join", Ordered, func() {
		var dep1, dep2, pod1, pod2, pod3, rs1, rs2 object.Object
		var eng Engine

		BeforeEach(func() {
			pod1 = object.NewViewObject("pod")
			object.SetContent(pod1, unstruct{
				"spec": unstruct{
					"image":  "image1",
					"parent": "dep1",
				},
			})
			object.SetName(pod1, "default", "pod1")
			pod1.SetLabels(map[string]string{"app": "app1"})

			pod2 = object.NewViewObject("pod")
			object.SetContent(pod2, unstruct{
				"spec": unstruct{
					"image":  "image2",
					"parent": "dep1",
				},
			})
			object.SetName(pod2, "other", "pod2")
			pod2.SetLabels(map[string]string{"app": "app2"})

			pod3 = object.NewViewObject("pod")
			object.SetContent(pod3, unstruct{
				"spec": unstruct{
					"image":  "image1",
					"parent": "dep2",
				},
			})
			object.SetName(pod3, "default", "pod3")
			pod3.SetLabels(map[string]string{"app": "app1"})

			dep1 = object.NewViewObject("dep")
			object.SetContent(dep1, unstruct{
				"spec": unstruct{
					"replicas": int64(3),
				},
			})
			object.SetName(dep1, "default", "dep1")
			dep1.SetLabels(map[string]string{"app": "app1"})

			dep2 = object.NewViewObject("dep")
			object.SetContent(dep2, unstruct{
				"spec": unstruct{
					"replicas": int64(1),
				},
			})
			object.SetName(dep2, "default", "dep2")
			dep2.SetLabels(map[string]string{"app": "app2"})

			rs1 = object.NewViewObject("rs")
			object.SetContent(rs1, unstruct{
				"spec": unstruct{
					"dep": "dep1",
				},
				"status": unstruct{
					"ready": int64(2),
				},
			})
			object.SetName(rs1, "default", "rs1")
			rs1.SetLabels(map[string]string{"app": "app1"})

			rs2 = object.NewViewObject("rs")
			object.SetContent(rs2, unstruct{
				"spec": unstruct{
					"dep": "dep2",
				},
				"status": unstruct{
					"ready": int64(1),
				},
			})
			object.SetName(rs2, "default", "rs2")
			rs2.SetLabels(map[string]string{"app": "app2"})

			eng = NewDefaultEngine("view", []gvk{viewv1a1.GroupVersion.WithKind("pod"),
				viewv1a1.GroupVersion.WithKind("dep"),
				viewv1a1.GroupVersion.WithKind("rs")}, logger)
		})

		Describe("Evaluating pipeline expressions for Added events", func() {
			It("should evaluate a simple pipeline", func() {
				jsonData := `
'@join':
  '@eq':
    - $.dep.metadata.name
    - $.pod.spec.parent
'@aggregate':
  - '@project':
      $.metadata.name:
        '@concat':
          - $.dep.metadata.name
          - "--"
          - $.pod.metadata.name
      $.metadata.namespace: $.pod.metadata.namespace`

				p := newPipeline(eng, []byte(jsonData))

				eng.WithObjects(dep1, dep2)
				Expect(eng.(*defaultEngine).baseViewStore[viewv1a1.GroupVersion.WithKind("dep")].List()).To(HaveLen(2))
				Expect(eng.(*defaultEngine).baseViewStore).NotTo(HaveKey("pod"))

				deltas, err := p.Evaluate(cache.Delta{Type: cache.Upserted, Object: pod1})
				Expect(err).NotTo(HaveOccurred())
				Expect(eng.(*defaultEngine).baseViewStore).To(HaveKey(viewv1a1.GroupVersion.WithKind("pod")))

				Expect(deltas).To(HaveLen(1))
				delta := deltas[0]
				Expect(delta.IsUnchanged()).To(BeFalse())
				Expect(delta.Object.GetName()).To(Equal("dep1--pod1"))
				Expect(delta.Object.GetNamespace()).To(Equal("default"))
				Expect(delta.Object.UnstructuredContent()).To(Equal(unstruct{
					"apiVersion": "view.dcontroller.io/v1alpha1",
					"kind":       "view",
					"metadata": unstruct{
						"name":      "dep1--pod1",
						"namespace": "default",
					},
				}))
			})

			It("should evaluate a pipeline without a @join", func() {
				jsonData := `
'@aggregate':
  - '@project':
      $.metadata: $.metadata`
				p := newPipeline(eng, []byte(jsonData))

				deltas, err := p.Evaluate(cache.Delta{Type: cache.Added, Object: pod1})
				Expect(err).NotTo(HaveOccurred())

				Expect(deltas).To(HaveLen(1))
				delta := deltas[0]
				Expect(delta.IsUnchanged()).To(BeFalse())
				Expect(delta.Object.GetName()).To(Equal("pod1"))
				Expect(delta.Object.GetNamespace()).To(Equal("default"))
				Expect(delta.Object.UnstructuredContent()).To(Equal(unstruct{
					"apiVersion": "view.dcontroller.io/v1alpha1",
					"kind":       "view",
					"metadata": unstruct{
						"name":      "pod1",
						"namespace": "default",
						"labels":    map[string]any{"app": "app1"},
					},
				}))
			})

			It("should handle a @select returning an empty object", func() {
				jsonData := `
'@aggregate':
  - '@select':
      '@eq': [$.metadata.namespace, "dummy"]`
				p := newPipeline(eng, []byte(jsonData))

				deltas, err := p.Evaluate(cache.Delta{Type: cache.Added, Object: pod1})
				Expect(err).NotTo(HaveOccurred())
				Expect(deltas).To(BeEmpty())
			})
		})

		Describe("Evaluating pipeline expressions for Update events", func() {
			It("should evaluate a simple pipeline - 1", func() {
				jsonData := `
'@join':
  '@and':
    - '@eq':
        - $.dep.metadata.name
        - $.pod.spec.parent
    - '@eq':
        - $.dep.metadata.name
        - $.rs.spec.dep
'@aggregate':
  - '@project':
      $.metadata: $.pod.metadata
      $.replicas: $.dep.spec.replicas
      $.ready: $.rs.status.ready`
				p := newPipeline(eng, []byte(jsonData))

				eng.WithObjects(pod1, pod2, pod3, dep2, rs1, rs2)
				deltas, err := p.Evaluate(cache.Delta{Type: cache.Upserted, Object: dep1})
				Expect(err).NotTo(HaveOccurred())

				Expect(deltas).To(HaveLen(2))
				delta := deltas[0]
				if delta.Object.GetName() != "pod1" {
					delta = deltas[1]
				}
				Expect(delta.IsUnchanged()).To(BeFalse())
				Expect(delta.Object.GetName()).To(Equal("pod1"))
				Expect(delta.Object.GetNamespace()).To(Equal("default"))
				Expect(delta.Object.UnstructuredContent()["replicas"]).To(Equal(int64(3)))
				Expect(delta.Object.UnstructuredContent()["ready"]).To(Equal(int64(2)))

				delta = deltas[1]
				if delta.Object.GetName() != "pod2" {
					delta = deltas[0]
				}
				Expect(delta.IsUnchanged()).To(BeFalse())
				Expect(delta.Object.GetName()).To(Equal("pod2"))
				Expect(delta.Object.GetNamespace()).To(Equal("other"))
				Expect(delta.Object.UnstructuredContent()["replicas"]).To(Equal(int64(3)))
				Expect(delta.Object.UnstructuredContent()["ready"]).To(Equal(int64(2)))

				// rewrite pod1 parent
				// oldpod1 := pod1.DeepCopy()
				pod1.UnstructuredContent()["spec"].(unstruct)["parent"] = "dep2"
				deltas, err = p.Evaluate(cache.Delta{Type: cache.Updated, Object: pod1})
				Expect(err).NotTo(HaveOccurred())

				Expect(deltas).To(HaveLen(1))
				delta = deltas[0]
				Expect(delta.IsUnchanged()).To(BeFalse())
				Expect(delta.Type).To(Equal(cache.Updated))
				Expect(delta.Object.GetName()).To(Equal("pod1"))
				Expect(delta.Object.GetNamespace()).To(Equal("default"))
				Expect(delta.Object.UnstructuredContent()["replicas"]).To(Equal(int64(1)))
				Expect(delta.Object.UnstructuredContent()["ready"]).To(Equal(int64(1)))
			})

			// almost the same as the previous but now the two updates (delete+add) must not collapse
			It("should evaluate a simple pipeline - 2", func() {
				jsonData := `
'@join':
  '@and':
    - '@eq':
        - $.dep.metadata.name
        - $.pod.spec.parent
    - '@eq':
        - $.dep.metadata.name
        - $.rs.spec.dep
'@aggregate':
  - '@project':
      metadata:
        namespace: $.dep.metadata.namespace
        name:
          "@concat":
            - $.dep.metadata.name
            - "--"
            - $.pod.metadata.name
      $.metadata.namespace: $.dep.metadata.namespace
      $.replicas: $.dep.spec.replicas
      $.ready: $.rs.status.ready`
				p := newPipeline(eng, []byte(jsonData))

				eng.WithObjects(pod1, pod2, pod3, dep2, rs1, rs2)
				deltas, err := p.Evaluate(cache.Delta{Type: cache.Upserted, Object: dep1})
				Expect(err).NotTo(HaveOccurred())

				Expect(deltas).To(HaveLen(2))
				delta := deltas[0]
				if delta.Object.GetName() != "dep1--pod1" {
					delta = deltas[1]
				}
				Expect(delta.IsUnchanged()).To(BeFalse())
				Expect(delta.Object.GetName()).To(Equal("dep1--pod1"))
				Expect(delta.Object.GetNamespace()).To(Equal("default"))
				Expect(delta.Object.UnstructuredContent()["replicas"]).To(Equal(int64(3)))
				Expect(delta.Object.UnstructuredContent()["ready"]).To(Equal(int64(2)))

				delta = deltas[1]
				if delta.Object.GetName() != "dep1--pod2" {
					delta = deltas[0]
				}
				Expect(delta.IsUnchanged()).To(BeFalse())
				Expect(delta.Object.GetName()).To(Equal("dep1--pod2"))
				Expect(delta.Object.GetNamespace()).To(Equal("default"))
				Expect(delta.Object.UnstructuredContent()["replicas"]).To(Equal(int64(3)))
				Expect(delta.Object.UnstructuredContent()["ready"]).To(Equal(int64(2)))

				// rewrite pod1 parent
				// oldpod1 := pod1.DeepCopy()
				pod1.UnstructuredContent()["spec"].(unstruct)["parent"] = "dep2"
				deltas, err = p.Evaluate(cache.Delta{Type: cache.Updated, Object: pod1})
				Expect(err).NotTo(HaveOccurred())

				Expect(deltas).To(HaveLen(2))

				delta = deltas[0]
				Expect(delta.IsUnchanged()).To(BeFalse())
				Expect(delta.Type).To(Equal(cache.Deleted))
				Expect(delta.Object.GetName()).To(Equal("dep1--pod1"))
				Expect(delta.Object.GetNamespace()).To(Equal("default"))
				Expect(delta.Object.UnstructuredContent()["replicas"]).To(Equal(int64(3)))
				Expect(delta.Object.UnstructuredContent()["ready"]).To(Equal(int64(2)))

				delta = deltas[1]
				Expect(delta.IsUnchanged()).To(BeFalse())
				Expect(delta.Type).To(Equal(cache.Upserted))
				Expect(delta.Object.GetName()).To(Equal("dep2--pod1"))
				Expect(delta.Object.GetNamespace()).To(Equal("default"))
				Expect(delta.Object.UnstructuredContent()["replicas"]).To(Equal(int64(1)))
				Expect(delta.Object.UnstructuredContent()["ready"]).To(Equal(int64(1)))
			})

			It("should ignore a duplicate event", func() {
				eng.WithObjects(dep1)
				p := &Pipeline{engine: eng}

				deltas, err := p.Evaluate(cache.Delta{Type: cache.Added, Object: dep1})
				Expect(err).NotTo(HaveOccurred())
				Expect(deltas).To(BeEmpty())

				deltas, err = p.Evaluate(cache.Delta{Type: cache.Updated, Object: dep1})
				Expect(err).NotTo(HaveOccurred())
				Expect(deltas).To(BeEmpty())

				// do not ignore a delete event for the same object
				deltas, err = p.Evaluate(cache.Delta{Type: cache.Deleted, Object: dep1})
				Expect(err).NotTo(HaveOccurred())
				Expect(deltas).To(HaveLen(1))
				Expect(deltas[0].IsUnchanged()).To(BeFalse())
				Expect(deltas[0].Type).To(Equal(cache.Deleted))
				Expect(deltas[0].Object.GetName()).To(Equal("dep1"))
				Expect(deltas[0].Object.GetNamespace()).To(Equal("default"))
			})
		})
	})

	var testUDPGateway = map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]any{
				"kubectl.kubernetes.io/last-applied-configuration": "{\"apiVersion\":\"gateway.networking.k8s.io/v1\",\"kind\":\"Gateway\",\"metadata\":{\"annotations\":{},\"name\":\"udp-gateway\",\"namespace\":\"stunner\"},\"spec\":{\"gatewayClassName\":\"stunner-gatewayclass\",\"listeners\":[{\"name\":\"udp-listener\",\"port\":3478,\"protocol\":\"TURN-UDP\"}]}}",
			},
			"creationTimestamp": "2024-06-27T13:45:32Z",
			"generation":        int64(1),
			"name":              "gateway",
			"namespace":         "default",
			"resourceVersion":   "38412796",
			"uid":               "3312bf0c-ccac-4998-bbd7-743522839dd2",
		},
		"spec": map[string]any{
			"gatewayClassName": "stunner-gatewayclass",
			"listeners": []any{
				map[string]any{
					"allowedRoutes": map[string]any{
						"namespaces": map[string]any{
							"from": "Same",
						},
					},
					"name":     "udp-listener",
					"port":     int64(3478),
					"protocol": "TURN-UDP",
				},
			},
		},
		"status": map[string]any{
			"addresses": []any{
				map[string]any{
					"type":  "IPAddress",
					"value": "35.232.39.111",
				},
			},
			"conditions": []any{
				map[string]any{
					"lastTransitionTime": "2024-07-01T12:46:03Z",
					"message":            "gateway accepted by controller stunner.l7mp.io/gateway-operator",
					"observedGeneration": int64(1),
					"reason":             "Accepted",
					"status":             "True",
					"type":               "Accepted",
				},
				map[string]any{
					"lastTransitionTime": "2024-07-08T05:36:35Z",
					"message":            "dataplane configuration successfully rendered",
					"observedGeneration": int64(1),
					"reason":             "Programmed",
					"status":             "True",
					"type":               "Programmed",
				},
			},
			"listeners": []any{
				map[string]any{
					"attachedRoutes": int64(1),
					"conditions": []any{
						map[string]any{
							"lastTransitionTime": "2024-07-08T05:36:35Z",
							"message":            "listener accepted",
							"observedGeneration": int64(1),
							"reason":             "Accepted",
							"status":             "True",
							"type":               "Accepted",
						},
						map[string]any{
							"lastTransitionTime": "2024-07-08T05:36:35Z",
							"message":            "listener protocol-port available",
							"observedGeneration": int64(1),
							"reason":             "NoConflicts",
							"status":             "False",
							"type":               "Conflicted",
						},
						map[string]any{
							"lastTransitionTime": "2024-07-08T05:36:35Z",
							"message":            "listener object references successfully resolved",
							"observedGeneration": int64(1),
							"reason":             "ResolvedRefs",
							"status":             "True",
							"type":               "ResolvedRefs",
						},
					},
					"name": "udp-listener",
					"supportedKinds": []any{
						map[string]any{
							"group": "gateway.networking.k8s.io",
							"kind":  "UDPRoute",
						},
						map[string]any{
							"group": "stunner.l7mp.io",
							"kind":  "UDPRoute",
						},
					},
				},
			},
		},
	}

	var testUDPRoute = map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]any{
				"kubectl.kubernetes.io/last-applied-configuration": "{\"apiVersion\":\"stunner.l7mp.io/v1\",\"kind\":\"UDPRoute\",\"metadata\":{\"annotations\":{},\"name\":\"iperf-server\",\"namespace\":\"stunner\"},\"spec\":{\"parentRefs\":[{\"name\":\"udp-gateway\"},{\"name\":\"tcp-gateway\"}],\"rules\":[{\"backendRefs\":[{\"name\":\"iperf-server\",\"namespace\":\"default\"}]}]}}\n",
			},
			"creationTimestamp": "2024-07-16T09:36:59Z",
			"generation":        int64(1),
			"name":              "route",
			"namespace":         "default",
			"resourceVersion":   "67544699",
			"uid":               "41c31a35-c7bc-4465-a1af-185ab4b00f90",
		},
		"spec": map[string]any{
			"parentRefs": []any{
				map[string]any{
					"group": "gateway.networking.k8s.io",
					"kind":  "Gateway",
					"name":  "udp-gateway",
				}, map[string]any{
					"group": "gateway.networking.k8s.io",
					"kind":  "Gateway",
					"name":  "tcp-gateway",
				}},
			"rules": []any{
				map[string]any{
					"backendRefs": []any{
						map[string]any{
							"group":     "",
							"kind":      "Service",
							"name":      "iperf-server",
							"namespace": "default",
						},
					},
				},
			},
			"status": map[string]any{
				"parents": []any{
					map[string]any{
						"conditions": []any{
							map[string]any{
								"lastTransitionTime": "2024-08-09T07:10:06Z",
								"message":            "parent accepts the route",
								"observedGeneration": int64(1),
								"reason":             "Accepted",
								"status":             "True",
								"type":               "Accepted",
							},
							map[string]any{
								"lastTransitionTime": "2024-08-09T07:10:06Z",
								"message":            "all backend references successfully resolved",
								"observedGeneration": int64(1),
								"reason":             "ResolvedRefs",
								"status":             "True",
								"type":               "ResolvedRefs",
							},
						},
						"controllerName": "stunner.l7mp.io/gateway-operator",
						"parentRef": map[string]any{
							"group": "gateway.networking.k8s.io",
							"kind":  "Gateway",
							"name":  "udp-gateway",
						},
					},
					map[string]any{
						"conditions": []any{
							map[string]any{
								"lastTransitionTime": "2024-08-09T07:10:06Z",
								"message":            "parent accepts the route",
								"observedGeneration": int64(1),
								"reason":             "Accepted",
								"status":             "True",
								"type":               "Accepted",
							},
							map[string]any{
								"lastTransitionTime": "2024-08-09T07:10:06Z",
								"message":            "all backend references successfully resolved",
								"observedGeneration": int64(1),
								"reason":             "ResolvedRefs",
								"status":             "True",
								"type":               "ResolvedRefs",
							},
						},
						"controllerName": "stunner.l7mp.io/gateway-operator",
						"parentRef": map[string]any{
							"group": "gateway.networking.k8s.io",
							"kind":  "Gateway",
							"name":  "tcp-gateway",
						},
					},
				},
			},
		},
	}

	var _ = Describe("Pipelines", func() {
		var gateway, route object.Object
		var eng Engine
		var routeArrachmentRule = `
'@join':
  '@any':
    - '@or':
        - "@eq": ["$$.allowedRoutes.namespaces.from", "All"]
        - "@and":
            - "@eq": ["$$.allowedRoutes.namespaces.from", "Same"]
            - "@eq": ["$.gateway.metadata.namespace", "$.route.metadata.namespace"]
        - "@and":
            - "@eq": ["$$.allowedRoutes.namespaces.from", "Selector"]
            - "@exists": "$$.allowedRoutes.namespaces.selector"
            - "@selector": ["$$.allowedRoutes.namespaces.selector", "$.route.metadata.labels"]
    - $.gateway.spec.listeners
'@aggregate':
  - '@project':
      metadata:
        namespace: $.route.metadata.namespace
        name:
          "@concat":
            - $.gateway.metadata.name
            - "--"
            - $.route.metadata.name`

		BeforeEach(func() {
			gateway = object.NewViewObject("gateway")
			object.SetName(gateway, "default", "gateway")
			object.SetContent(gateway, testUDPGateway)
			gateway = object.DeepCopy(gateway) // so we don't share stuff across tests
			route = object.NewViewObject("route")
			object.SetName(route, "default", "route")
			object.SetContent(route, testUDPRoute)
			route = object.DeepCopy(route)
			eng = NewDefaultEngine("view", []gvk{
				viewv1a1.GroupVersion.WithKind("gateway"),
				viewv1a1.GroupVersion.WithKind("route")}, logger)
		})

		It("should implement the route attachment API with the All policy", func() {
			p := newPipeline(eng, []byte(routeArrachmentRule))

			unstructured.SetNestedSlice(gateway.UnstructuredContent(),
				[]any{
					map[string]any{
						"allowedRoutes": map[string]any{
							"namespaces": map[string]any{
								"from": "All",
							},
						},
						"name":     "udp-listener",
						"port":     int64(3478),
						"protocol": "TURN-UDP",
					},
				}, "spec", "listeners")
			eng.WithObjects(gateway)

			deltas, err := p.Evaluate(cache.Delta{Type: cache.Upserted, Object: route})
			Expect(err).NotTo(HaveOccurred())

			Expect(deltas).To(HaveLen(1))
			delta := deltas[0]
			Expect(delta.IsUnchanged()).To(BeFalse())
			Expect(delta.Object.GetName()).To(Equal("gateway--route"))
			Expect(delta.Object.GetNamespace()).To(Equal("default"))

			deltas, err = p.Evaluate(cache.Delta{Type: cache.Deleted, Object: route})
			Expect(err).NotTo(HaveOccurred())

			Expect(deltas).To(HaveLen(1))
			delta = deltas[0]
			Expect(delta.IsUnchanged()).To(BeFalse())
			Expect(delta.Type).To(Equal(cache.Deleted))
			Expect(delta.Object.GetName()).To(Equal("gateway--route"))
			Expect(delta.Object.GetNamespace()).To(Equal("default"))

			route.SetNamespace("other")
			deltas, err = p.Evaluate(cache.Delta{Type: cache.Upserted, Object: route})
			Expect(err).NotTo(HaveOccurred())

			Expect(deltas).To(HaveLen(1))
			delta = deltas[0]
			Expect(delta.IsUnchanged()).To(BeFalse())
			Expect(delta.Type).To(Equal(cache.Upserted))
			Expect(delta.Object.GetName()).To(Equal("gateway--route"))
			Expect(delta.Object.GetNamespace()).To(Equal("other"))
		})

		It("should implement the route attachment API with the Same policy", func() {
			p := newPipeline(eng, []byte(routeArrachmentRule))

			eng.WithObjects(gateway)
			route.SetNamespace("other")
			deltas, err := p.Evaluate(cache.Delta{Type: cache.Added, Object: route})
			Expect(err).NotTo(HaveOccurred())
			Expect(deltas).To(BeEmpty())
		})

		It("should implement the route attachment API with the Selector policy", func() {
			p := newPipeline(eng, []byte(routeArrachmentRule))

			unstructured.SetNestedSlice(gateway.UnstructuredContent(),
				[]any{map[string]any{
					"allowedRoutes": map[string]any{
						"namespaces": map[string]any{
							"from": "Selector",
							"selector": map[string]any{
								"matchExpressions": []any{
									map[string]any{
										"key":      "app",
										"operator": "In",
										"values":   []any{"nginx", "apache"},
									},
								},
							},
						},
					},
					"name":     "udp-listener",
					"port":     int64(3478),
					"protocol": "TURN-UDP",
				},
				}, "spec", "listeners")
			eng.WithObjects(gateway)

			route.SetLabels(map[string]string{"app": "nginx"})
			deltas, err := p.Evaluate(cache.Delta{Type: cache.Added, Object: route})
			Expect(err).NotTo(HaveOccurred())
			Expect(deltas).To(HaveLen(1))
			delta := deltas[0]
			Expect(delta.IsUnchanged()).To(BeFalse())
			Expect(delta.Type).To(Equal(cache.Upserted))
			Expect(delta.Object.GetName()).To(Equal("gateway--route"))
			Expect(delta.Object.GetNamespace()).To(Equal("default"))

			route.SetLabels(map[string]string{"app": "httpd"})
			deltas, err = p.Evaluate(cache.Delta{Type: cache.Updated, Object: route})
			Expect(err).NotTo(HaveOccurred())
			Expect(deltas).To(HaveLen(1))
			delta = deltas[0]
			Expect(delta.IsUnchanged()).To(BeFalse())
			Expect(delta.Type).To(Equal(cache.Deleted))
			Expect(delta.Object.GetName()).To(Equal("gateway--route"))
			Expect(delta.Object.GetNamespace()).To(Equal("default"))
		})
	})

	Context("When running a pipeline with real Kubernetes objects", Ordered, func() {
		var svc1, es1 object.Object
		var eng Engine

		BeforeEach(func() {
			svc1 = testutils.TestSvc.DeepCopy()
			es1 = testutils.TestEndpointSlice.DeepCopy()

			eng = NewDefaultEngine("view", []gvk{svc1.GroupVersionKind(),
				es1.GroupVersionKind()}, logger)
		})

		It("evaluate the pipeline over a complex join expression", func() {
			yamlData := `
"@join":
  "@and":
    - '@eq':
        - $.Service.metadata.name
        - '$["EndpointSlice"]["metadata"]["labels"]["kubernetes.io/service-name"]'
    - '@eq':
        - $.Service.metadata.namespace
        - $.EndpointSlice.metadata.namespace
"@aggregate":
  - "@project":
      metadata:
        name: "$.EndpointSlice.metadata.name"
        namespace: "$.EndpointSlice.metadata.namespace"
        annotations:
          "dcontroller.io/service-type": "$.Service.spec.type"`

			p := newPipeline(eng, []byte(yamlData))

			eng.WithObjects(svc1)

			deltas, err := p.Evaluate(cache.Delta{Type: cache.Added, Object: es1})
			Expect(err).NotTo(HaveOccurred())

			// logger.Info(fmt.Sprintf("%v", deltas))
			// logger.Info(object.Dump(deltas[0].Object))

			Expect(deltas).To(HaveLen(1))
			delta := deltas[0]
			Expect(delta.IsUnchanged()).To(BeFalse())
			Expect(delta.Type).To(Equal(cache.Upserted))
			Expect(delta.Object.GetName()).To(Equal("test-endpointslice"))
			Expect(delta.Object.GetNamespace()).To(Equal("default"))

			anns := delta.Object.GetAnnotations()
			Expect(anns).To(HaveLen(1))

			t, ok := anns["dcontroller.io/service-type"]
			Expect(ok).To(BeTrue())
			Expect(t).To(Equal("ClusterIP"))
		})

		It("survive a full JSON marshal-unmarshal roundtrip", func() {
			jsonData := `{"@join":{"@and":[{"@eq":["$.Service.metadata.name","$[\"EndpointSlice\"][\"metadata\"][\"labels\"][\"kubernetes.io/service-name\"]"]},{"@eq":["$.Service.metadata.namespace","$.EndpointSlice.metadata.namespace"]}]},"@aggregate":[{"@project":{"metadata":{"name":"$.EndpointSlice.metadata.name","namespace":"$.EndpointSlice.metadata.namespace","annotations":{"dcontroller.io/service-type":"$.Service.spec.type"}}}}]}`
			p1 := newPipeline(nil, []byte(jsonData))
			Expect(p1.Join).NotTo(BeNil())
			Expect(p1.Join.Join).NotTo(BeNil())
			Expect(p1.Aggregation).NotTo(BeNil())
			Expect(p1.Aggregation.Aggregation).NotTo(BeNil())

			js, err := json.Marshal(p1)
			Expect(err).NotTo(HaveOccurred())

			// cannot test json equivalence: map key order is arbitrary instead, parse
			// back the json produced into a serialized Pipeline
			var p2 opv1a1.Pipeline
			err = json.Unmarshal(js, &p2)
			Expect(err).NotTo(HaveOccurred())
			Expect(p2.Join).NotTo(BeNil())
			Expect(p2.Aggregation).NotTo(BeNil())

			// compare the serializable representations
			Expect(p2.Join).To(Equal(p1.Join.Join))
			Expect(p2.Aggregation).To(Equal(p1.Aggregation.Aggregation))
		})
	})
})

func newPipeline(eng Engine, data []byte) *Pipeline {
	var p opv1a1.Pipeline
	err := yaml.Unmarshal(data, &p)
	Expect(err).NotTo(HaveOccurred())
	return &Pipeline{
		Join:        NewJoin(eng, p.Join),
		Aggregation: NewAggregation(eng, p.Aggregation),
		engine:      eng,
	}
}

package pipeline

import (
	"encoding/json"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"go.uber.org/zap/zapcore"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/yaml"

	"github.com/l7mp/dcontroller/internal/testutils"
	opv1a1 "github.com/l7mp/dcontroller/pkg/api/operator/v1alpha1"
	"github.com/l7mp/dcontroller/pkg/dbsp"
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

func TestPipeline(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Pipeline")
}

var _ = Describe("Pipelines", func() {
	Context("When running a pipeline with a 3-way join", Ordered, func() {
		var dep1, dep2, pod1, pod2, pod3, rs1, rs2 object.Object

		BeforeEach(func() {
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

			rs1 = object.NewViewObject("test", "rs")
			object.SetContent(rs1, map[string]any{
				"spec": map[string]any{
					"dep": "dep1",
				},
				"status": map[string]any{
					"ready": int64(2),
				},
			})
			object.SetName(rs1, "default", "rs1")
			rs1.SetLabels(map[string]string{"app": "app1"})

			rs2 = object.NewViewObject("test", "rs")
			object.SetContent(rs2, map[string]any{
				"spec": map[string]any{
					"dep": "dep2",
				},
				"status": map[string]any{
					"ready": int64(1),
				},
			})
			object.SetName(rs2, "default", "rs2")
			rs2.SetLabels(map[string]string{"app": "app2"})
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
				p, err := newPipeline(jsonData, []string{"pod", "dep"})
				Expect(err).NotTo(HaveOccurred())

				var deltas []object.Delta
				for _, o := range []object.Object{dep1, dep2} {
					deltas, err = p.Evaluate(object.Delta{Type: object.Upserted, Object: o})
					Expect(err).NotTo(HaveOccurred())
					Expect(deltas).To(BeEmpty())
				}

				deltas, err = p.Evaluate(object.Delta{Type: object.Upserted, Object: pod1})
				Expect(err).NotTo(HaveOccurred())
				Expect(deltas).To(HaveLen(1))
				delta := deltas[0]
				Expect(delta.IsUnchanged()).To(BeFalse())
				Expect(delta.Object.GetName()).To(Equal("dep1--pod1"))
				Expect(delta.Object.GetNamespace()).To(Equal("default"))
				Expect(delta.Object.UnstructuredContent()).To(Equal(map[string]any{
					"apiVersion": "test.view.dcontroller.io/v1alpha1",
					"kind":       "view",
					"metadata": map[string]any{
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
				p, err := newPipeline(jsonData, []string{"pod"})
				Expect(err).NotTo(HaveOccurred())

				deltas, err := p.Evaluate(object.Delta{Type: object.Added, Object: pod1})
				Expect(err).NotTo(HaveOccurred())

				Expect(deltas).To(HaveLen(1))
				delta := deltas[0]
				Expect(delta.IsUnchanged()).To(BeFalse())
				Expect(delta.Object.GetName()).To(Equal("pod1"))
				Expect(delta.Object.GetNamespace()).To(Equal("default"))
				Expect(delta.Object.UnstructuredContent()).To(Equal(map[string]any{
					"apiVersion": "test.view.dcontroller.io/v1alpha1",
					"kind":       "view",
					"metadata": map[string]any{
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
				p, err := newPipeline(jsonData, []string{"pod"})
				Expect(err).NotTo(HaveOccurred())

				deltas, err := p.Evaluate(object.Delta{Type: object.Added, Object: pod1})
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
				p, err := newPipeline(jsonData, []string{"pod", "dep", "rs"})
				Expect(err).NotTo(HaveOccurred())

				var deltas []object.Delta
				for _, o := range []object.Object{pod1, pod2, pod3, dep2, rs1, rs2} {
					_, err = p.Evaluate(object.Delta{Type: object.Upserted, Object: o})
					Expect(err).NotTo(HaveOccurred())
				}

				deltas, err = p.Evaluate(object.Delta{Type: object.Upserted, Object: dep1})
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
				pod1.UnstructuredContent()["spec"].(map[string]any)["parent"] = "dep2"
				deltas, err = p.Evaluate(object.Delta{Type: object.Updated, Object: pod1})
				Expect(err).NotTo(HaveOccurred())

				Expect(deltas).To(HaveLen(1))
				delta = deltas[0]
				Expect(delta.IsUnchanged()).To(BeFalse())
				Expect(delta.Type).To(Equal(object.Upserted))
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
				p, err := newPipeline(jsonData, []string{"pod", "dep", "rs"})
				Expect(err).NotTo(HaveOccurred())

				var deltas []object.Delta
				for _, o := range []object.Object{pod1, pod2, pod3, dep2, rs1, rs2} {
					_, err = p.Evaluate(object.Delta{Type: object.Upserted, Object: o})
					Expect(err).NotTo(HaveOccurred())
				}

				deltas, err = p.Evaluate(object.Delta{Type: object.Upserted, Object: dep1})
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
				pod1.UnstructuredContent()["spec"].(map[string]any)["parent"] = "dep2"
				deltas, err = p.Evaluate(object.Delta{Type: object.Updated, Object: pod1})
				Expect(err).NotTo(HaveOccurred())

				Expect(deltas).To(HaveLen(2))

				delta = deltas[0]
				Expect(delta.IsUnchanged()).To(BeFalse())
				Expect(delta.Type).To(Equal(object.Deleted))
				Expect(delta.Object.GetName()).To(Equal("dep1--pod1"))
				Expect(delta.Object.GetNamespace()).To(Equal("default"))
				Expect(delta.Object.UnstructuredContent()["replicas"]).To(Equal(int64(3)))
				Expect(delta.Object.UnstructuredContent()["ready"]).To(Equal(int64(2)))

				delta = deltas[1]
				Expect(delta.IsUnchanged()).To(BeFalse())
				Expect(delta.Type).To(Equal(object.Upserted))
				Expect(delta.Object.GetName()).To(Equal("dep2--pod1"))
				Expect(delta.Object.GetNamespace()).To(Equal("default"))
				Expect(delta.Object.UnstructuredContent()["replicas"]).To(Equal(int64(1)))
				Expect(delta.Object.UnstructuredContent()["ready"]).To(Equal(int64(1)))
			})

			It("should not ignore a duplicate event", func() {
				// DBSP theory results that only an explicit "distinct" element at
				// the end of the pipeline would remove duplicates, but we do not
				// have any since that's a performance hog. thus we preserve
				// multiple adds/deletes:
				//
				// Timestep 1: ΔInput = {doc: +1}
				//   I: snapshot becomes {doc: 1}
				//   Q (select): {doc: 1} (doc passes filter, multiplicity preserved)
				//   D: ΔOutput = {doc: +1}
				// Timestep 2: ΔInput = {doc: +1}
				//   I: snapshot becomes {doc: 2} (multiplicity accumulates)
				//   Q (select): {doc: 2} (doc still passes filter, multiplicity = 2)
				//   D: ΔOutput = {doc: +1} (change from 1 to 2)
				//
				// Add a 'distinct` op at the end to dedup:
				//  ΔInput → I → select → distinct → D → ΔOutput

				jsonData := `{'@aggregate': ['@select': true]}`
				p, err := newPipeline(jsonData, []string{"dep"})
				Expect(err).NotTo(HaveOccurred())

				deltas, err := p.Evaluate(object.Delta{Type: object.Added, Object: dep1})
				Expect(err).NotTo(HaveOccurred())
				Expect(deltas).To(HaveLen(1))
				Expect(deltas[0].IsUnchanged()).To(BeFalse())
				Expect(deltas[0].Type).To(Equal(object.Upserted))
				Expect(deltas[0].Object.GetName()).To(Equal("dep1"))
				Expect(deltas[0].Object.GetNamespace()).To(Equal("default"))

				// duplicate add -> singleton delta for the doc
				deltas, err = p.Evaluate(object.Delta{Type: object.Added, Object: dep1})
				Expect(err).NotTo(HaveOccurred())
				Expect(deltas).To(HaveLen(1))
				Expect(deltas[0].IsUnchanged()).To(BeFalse())
				Expect(deltas[0].Type).To(Equal(object.Upserted))
				Expect(deltas[0].Object.GetName()).To(Equal("dep1"))
				Expect(deltas[0].Object.GetNamespace()).To(Equal("default"))

				// duplicate upsert (maps to a delete+add for the same doc) -> no delta!
				deltas, err = p.Evaluate(object.Delta{Type: object.Upserted, Object: dep1})
				Expect(err).NotTo(HaveOccurred())
				Expect(deltas).To(BeEmpty())

				// duplicate update (maps to a delete+add for the same doc) -> no delta!
				deltas, err = p.Evaluate(object.Delta{Type: object.Updated, Object: dep1})
				Expect(err).NotTo(HaveOccurred())
				Expect(deltas).To(BeEmpty())

				// do not ignore a delete event for the same object
				deltas, err = p.Evaluate(object.Delta{Type: object.Deleted, Object: dep1})
				Expect(err).NotTo(HaveOccurred())
				Expect(deltas).To(HaveLen(1))
				Expect(deltas[0].IsUnchanged()).To(BeFalse())
				Expect(deltas[0].Type).To(Equal(object.Deleted))
				Expect(deltas[0].Object.GetName()).To(Equal("dep1"))
				Expect(deltas[0].Object.GetNamespace()).To(Equal("default"))

				// add a distinct: input → select → I → distinct → D → output
				q, ok := p.(*Pipeline)
				Expect(ok).To(BeTrue())
				q.graph.AddToChain(dbsp.NewIntegrator())
				q.graph.AddToChain(dbsp.NewDistinct())
				q.graph.AddToChain(dbsp.NewDifferentiator())
				err = q.rewriter.Optimize(q.graph)
				Expect(err).NotTo(HaveOccurred())
				executor, err := dbsp.NewExecutor(q.graph, q.log)
				Expect(err).NotTo(HaveOccurred())
				q.executor = executor

				deltas, err = q.Evaluate(object.Delta{Type: object.Added, Object: dep1})
				Expect(err).NotTo(HaveOccurred())
				Expect(deltas).To(HaveLen(1))
				Expect(deltas[0].IsUnchanged()).To(BeFalse())
				Expect(deltas[0].Type).To(Equal(object.Upserted))
				Expect(deltas[0].Object.GetName()).To(Equal("dep1"))
				Expect(deltas[0].Object.GetNamespace()).To(Equal("default"))

				// duplicate add -> singleton delta for the doc
				deltas, err = q.Evaluate(object.Delta{Type: object.Added, Object: dep1})
				Expect(err).NotTo(HaveOccurred())
				Expect(deltas).To(BeEmpty())

				// duplicate upsert (maps to a delete+add for the same doc) -> no delta!
				deltas, err = q.Evaluate(object.Delta{Type: object.Upserted, Object: dep1})
				Expect(err).NotTo(HaveOccurred())
				Expect(deltas).To(BeEmpty())

				// duplicate update (maps to a delete+add for the same doc) -> no delta!
				deltas, err = q.Evaluate(object.Delta{Type: object.Updated, Object: dep1})
				Expect(err).NotTo(HaveOccurred())
				Expect(deltas).To(BeEmpty())

				// ignore a delete event: "distinct" eliminates removals
				deltas, err = q.Evaluate(object.Delta{Type: object.Deleted, Object: dep1})
				Expect(err).NotTo(HaveOccurred())
				Expect(deltas).To(BeEmpty())
			})
		})
	})

	var testUDPGateway = map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]any{
				"kubectl.kubernetes.io/last-applied-configuration": "{\"apiVersion\":\"gateway.networking.k8s.io/v1\",\"kind\":\"Gateway\",\"metadata\":{\"annotations\":{},\"name\":\"udp-gateway\",\"namespace\":\"default\"},\"spec\":{\"gatewayClassName\":\"test-gatewayclass\",\"listeners\":[{\"name\":\"udp-listener\",\"port\":3478,\"protocol\":\"TURN-UDP\"}]}}",
			},
			"creationTimestamp": "2024-06-27T13:45:32Z",
			"generation":        int64(1),
			"name":              "gateway",
			"namespace":         "default",
			"resourceVersion":   "38412796",
			"uid":               "3312bf0c-ccac-4998-bbd7-743522839dd2",
		},
		"spec": map[string]any{
			"gatewayClassName": "test-gatewayclass",
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
					"message":            "gateway accepted by dcontroller.l7mp.io/gateway-operator",
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
							"group": "gateway.networking.k8s.io",
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
				"kubectl.kubernetes.io/last-applied-configuration": "{\"apiVersion\":\"gateway.networking.k8s.io/v1\",\"kind\":\"UDPRoute\",\"metadata\":{\"annotations\":{},\"name\":\"iperf-server\",\"namespace\":\"default\"},\"spec\":{\"parentRefs\":[{\"name\":\"udp-gateway\"},{\"name\":\"tcp-gateway\"}],\"rules\":[{\"backendRefs\":[{\"name\":\"iperf-server\",\"namespace\":\"default\"}]}]}}\n",
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
						"controllerName": "dcontroller.l7mp.io/gateway-operator",
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
						"controllerName": "dcontroller.l7mp.io/gateway-operator",
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
			gateway = object.NewViewObject("test", "gateway")
			object.SetName(gateway, "default", "gateway")
			object.SetContent(gateway, testUDPGateway)
			gateway = object.DeepCopy(gateway) // so we don't share stuff across tests
			route = object.NewViewObject("test", "route")
			object.SetName(route, "default", "route")
			object.SetContent(route, testUDPRoute)
			route = object.DeepCopy(route)
		})

		It("should implement the route attachment API with the All policy", func() {
			p, err := newPipeline(routeArrachmentRule, []string{"gateway", "route"})
			Expect(err).NotTo(HaveOccurred())

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

			var deltas []object.Delta
			for _, o := range []object.Object{gateway} {
				_, err = p.Evaluate(object.Delta{Type: object.Upserted, Object: o})
				Expect(err).NotTo(HaveOccurred())
			}

			deltas, err = p.Evaluate(object.Delta{Type: object.Upserted, Object: route})
			Expect(err).NotTo(HaveOccurred())

			Expect(deltas).To(HaveLen(1))
			delta := deltas[0]
			Expect(delta.IsUnchanged()).To(BeFalse())
			Expect(delta.Object.GetName()).To(Equal("gateway--route"))
			Expect(delta.Object.GetNamespace()).To(Equal("default"))

			deltas, err = p.Evaluate(object.Delta{Type: object.Deleted, Object: route})
			Expect(err).NotTo(HaveOccurred())

			Expect(deltas).To(HaveLen(1))
			delta = deltas[0]
			Expect(delta.IsUnchanged()).To(BeFalse())
			Expect(delta.Type).To(Equal(object.Deleted))
			Expect(delta.Object.GetName()).To(Equal("gateway--route"))
			Expect(delta.Object.GetNamespace()).To(Equal("default"))

			route.SetNamespace("other")
			deltas, err = p.Evaluate(object.Delta{Type: object.Upserted, Object: route})
			Expect(err).NotTo(HaveOccurred())

			Expect(deltas).To(HaveLen(1))
			delta = deltas[0]
			Expect(delta.IsUnchanged()).To(BeFalse())
			Expect(delta.Type).To(Equal(object.Upserted))
			Expect(delta.Object.GetName()).To(Equal("gateway--route"))
			Expect(delta.Object.GetNamespace()).To(Equal("other"))
		})

		It("should implement the route attachment API with the Same policy", func() {
			p, err := newPipeline(routeArrachmentRule, []string{"gateway", "route"})
			Expect(err).NotTo(HaveOccurred())

			var deltas []object.Delta
			for _, o := range []object.Object{gateway} {
				_, err = p.Evaluate(object.Delta{Type: object.Upserted, Object: o})
				Expect(err).NotTo(HaveOccurred())
			}

			route.SetNamespace("other")
			deltas, err = p.Evaluate(object.Delta{Type: object.Added, Object: route})
			Expect(err).NotTo(HaveOccurred())
			Expect(deltas).To(BeEmpty())
		})

		It("should implement the route attachment API with the Selector policy", func() {
			p, err := newPipeline(routeArrachmentRule, []string{"gateway", "route"})
			Expect(err).NotTo(HaveOccurred())

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

			var deltas []object.Delta
			for _, o := range []object.Object{gateway} {
				_, err = p.Evaluate(object.Delta{Type: object.Upserted, Object: o})
				Expect(err).NotTo(HaveOccurred())
			}

			route.SetLabels(map[string]string{"app": "nginx"})
			deltas, err = p.Evaluate(object.Delta{Type: object.Added, Object: route})
			Expect(err).NotTo(HaveOccurred())
			Expect(deltas).To(HaveLen(1))
			delta := deltas[0]
			Expect(delta.IsUnchanged()).To(BeFalse())
			Expect(delta.Type).To(Equal(object.Upserted))
			Expect(delta.Object.GetName()).To(Equal("gateway--route"))
			Expect(delta.Object.GetNamespace()).To(Equal("default"))

			route.SetLabels(map[string]string{"app": "httpd"})
			deltas, err = p.Evaluate(object.Delta{Type: object.Updated, Object: route})
			Expect(err).NotTo(HaveOccurred())
			Expect(deltas).To(HaveLen(1))
			delta = deltas[0]
			Expect(delta.IsUnchanged()).To(BeFalse())
			Expect(delta.Type).To(Equal(object.Deleted))
			Expect(delta.Object.GetName()).To(Equal("gateway--route"))
			Expect(delta.Object.GetNamespace()).To(Equal("default"))
		})
	})

	Context("When running a pipeline with real Kubernetes objects", Ordered, func() {
		var svc1, es1 object.Object

		BeforeEach(func() {
			svc1 = testutils.TestSvc.DeepCopy()
			es1 = testutils.TestEndpointSlice.DeepCopy()
		})

		It("evaluate the pipeline over a complex join expression", func() {
			jsonData := `
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

			p, err := newPipeline(jsonData, []string{"Service", "EndpointSlice"})
			Expect(err).NotTo(HaveOccurred())

			var deltas []object.Delta
			for _, o := range []object.Object{svc1} {
				_, err = p.Evaluate(object.Delta{Type: object.Upserted, Object: o})
				Expect(err).NotTo(HaveOccurred())
			}

			deltas, err = p.Evaluate(object.Delta{Type: object.Added, Object: es1})
			Expect(err).NotTo(HaveOccurred())

			// logger.Info(fmt.Sprintf("%v", deltas))
			// logger.Info(object.Dump(deltas[0].Object))

			Expect(deltas).To(HaveLen(1))
			delta := deltas[0]
			Expect(delta.IsUnchanged()).To(BeFalse())
			Expect(delta.Type).To(Equal(object.Upserted))
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
			e1, err := newPipeline(jsonData, []string{"Service", "EndpointSlice"})
			Expect(err).NotTo(HaveOccurred())

			p1, ok := e1.(*Pipeline)
			Expect(ok).To(BeTrue())
			Expect(p1.config.Join).NotTo(BeNil())
			Expect(p1.config.Aggregation).NotTo(BeNil())

			js, err := json.Marshal(p1.config)
			Expect(err).NotTo(HaveOccurred())

			// cannot test json equivalence: map key order is arbitrary instead, parse
			// back the json produced into a serialized Pipeline
			var p2 opv1a1.Pipeline
			err = json.Unmarshal(js, &p2)
			Expect(err).NotTo(HaveOccurred())
			Expect(p2.Join).NotTo(BeNil())
			Expect(p2.Aggregation).NotTo(BeNil())

			// compare the serializable representations
			Expect(p2.Join).To(Equal(p1.config.Join))
			Expect(p2.Aggregation).To(Equal(p1.config.Aggregation))
		})
	})
})

func newPipeline(data string, srcs []string) (Evaluator, error) {
	var conf opv1a1.Pipeline
	err := yaml.Unmarshal([]byte(data), &conf)
	if err != nil {
		return nil, err
	}
	gvks := []schema.GroupVersionKind{}
	for _, view := range srcs {
		gvks = append(gvks, opv1a1.GroupVersion.WithKind(view))
	}
	p, err := NewPipeline("test", gvk.Kind, gvks, conf, logger)
	if err != nil {
		return nil, err
	}
	return p, nil
}

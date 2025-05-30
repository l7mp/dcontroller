package pipeline

// . "github.com/onsi/ginkgo/v2"
// . "github.com/onsi/gomega"

// var _ = Describe("Joins", func() {
// 	var dep1, dep2, pod1, pod2, pod3, rs1, rs2 object.Object
// 	var eng Engine

// 	BeforeEach(func() {
// 		pod1 = object.NewViewObject("pod")
// 		object.SetContent(pod1, unstruct{
// 			"spec": unstruct{
// 				"image":  "image1",
// 				"parent": "dep1",
// 			},
// 		})
// 		object.SetName(pod1, "default", "pod1")
// 		pod1.SetLabels(map[string]string{"app": "app1"})

// 		pod2 = object.NewViewObject("pod")
// 		object.SetContent(pod2, unstruct{
// 			"spec": unstruct{
// 				"image":  "image2",
// 				"parent": "dep1",
// 			},
// 		})
// 		object.SetName(pod2, "other", "pod2")
// 		pod2.SetLabels(map[string]string{"app": "app2"})

// 		pod3 = object.NewViewObject("pod")
// 		object.SetContent(pod3, unstruct{
// 			"spec": unstruct{
// 				"image":  "image1",
// 				"parent": "dep2",
// 			},
// 		})
// 		object.SetName(pod3, "default", "pod3")
// 		pod3.SetLabels(map[string]string{"app": "app1"})

// 		dep1 = object.NewViewObject("dep")
// 		object.SetContent(dep1, unstruct{
// 			"spec": unstruct{
// 				"replicas": int64(3),
// 			},
// 		})
// 		object.SetName(dep1, "default", "dep1")
// 		dep1.SetLabels(map[string]string{"app": "app1"})

// 		dep2 = object.NewViewObject("dep")
// 		object.SetContent(dep2, unstruct{
// 			"spec": unstruct{
// 				"replicas": int64(1),
// 			},
// 		})
// 		object.SetName(dep2, "default", "dep2")
// 		dep2.SetLabels(map[string]string{"app": "app2"})

// 		rs1 = object.NewViewObject("rs")
// 		object.SetContent(rs1, unstruct{
// 			"spec": unstruct{
// 				"dep": "dep1",
// 			},
// 		})
// 		object.SetName(rs1, "default", "rs1")
// 		rs1.SetLabels(map[string]string{"app": "app1"})

// 		rs2 = object.NewViewObject("rs")
// 		object.SetContent(rs2, unstruct{
// 			"spec": unstruct{
// 				"dep": "dep2",
// 			},
// 		})
// 		object.SetName(rs2, "default", "rs2")
// 		rs2.SetLabels(map[string]string{"app": "app2"})

// 		eng = NewDefaultEngine("view", []schema.GroupVersionKind{viewv1a1.GroupVersion.WithKind("pod"),
// 			viewv1a1.GroupVersion.WithKind("dep"),
// 			viewv1a1.GroupVersion.WithKind("rs")}, logger)
// 	})

// 	Describe("Evaluating join expressions for Added events", func() {
// 		It("should evaluate a join on the pod parent", func() {
// 			jsonData := `{"@join":{"@eq":["$.dep.metadata.name","$.pod.spec.parent"]}}` //nolint:goconst
// 			j := newJoin(eng, []byte(jsonData))

// 			eng.WithObjects(dep1, dep2)
// 			Expect(eng.(*defaultEngine).baseViewStore[viewv1a1.GroupVersion.WithKind("dep")].List()).To(HaveLen(2))
// 			Expect(eng.(*defaultEngine).baseViewStore).NotTo(HaveKey(viewv1a1.GroupVersion.WithKind("pod")))

// 			deltas, err := j.Evaluate(cache.Delta{Type: cache.Upserted, Object: pod1})
// 			Expect(err).NotTo(HaveOccurred())
// 			Expect(eng.(*defaultEngine).baseViewStore[viewv1a1.GroupVersion.WithKind("dep")].List()).To(HaveLen(2))
// 			Expect(eng.(*defaultEngine).baseViewStore[viewv1a1.GroupVersion.WithKind("pod")].List()).To(HaveLen(1))

// 			Expect(deltas).To(HaveLen(1))
// 			delta := deltas[0]
// 			Expect(delta.IsUnchanged()).To(BeFalse())
// 			Expect(delta.Object.UnstructuredContent()["pod"]).To(Equal(pod1.UnstructuredContent()))
// 			Expect(delta.Object.UnstructuredContent()["dep"]).To(Equal(dep1.UnstructuredContent()))
// 		})

// 		It("should evaluate a join on pod namespace", func() {
// 			jsonData := `{"@join":{"@eq":["$.dep.metadata.namespace","$.pod.metadata.namespace"]}}`
// 			j := newJoin(eng, []byte(jsonData))

// 			eng.WithObjects(dep1, dep2)
// 			deltas, err := j.Evaluate(cache.Delta{Type: cache.Upserted, Object: pod1})
// 			Expect(err).NotTo(HaveOccurred())
// 			Expect(deltas).To(HaveLen(2))
// 			Expect(deltas).To(ContainElement(objFieldEq(dep1.UnstructuredContent(), "dep")))
// 			Expect(deltas).To(ContainElement(objFieldEq(pod1.UnstructuredContent(), "pod")))
// 			Expect(deltas).To(ContainElement(objFieldEq(dep2.UnstructuredContent(), "dep")))
// 			Expect(deltas).To(ContainElement(objFieldEq(pod1.UnstructuredContent(), "pod")))
// 		})

// 		It("should evaluate a join on labels", func() {
// 			jsonData := `{"@join":{"@eq":["$.dep.metadata.labels.app","$.pod.metadata.labels.app"]}}` //nolint:goconst
// 			j := newJoin(eng, []byte(jsonData))

// 			eng.WithObjects(pod1, pod2, pod3)
// 			Expect(eng.(*defaultEngine).baseViewStore[viewv1a1.GroupVersion.WithKind("pod")].List()).To(HaveLen(3))
// 			Expect(eng.(*defaultEngine).baseViewStore).NotTo(HaveKey(viewv1a1.GroupVersion.WithKind("dep")))

// 			deltas, err := j.Evaluate(cache.Delta{Type: cache.Upserted, Object: dep1})
// 			Expect(err).NotTo(HaveOccurred())

// 			Expect(eng.(*defaultEngine).baseViewStore[viewv1a1.GroupVersion.WithKind("pod")].List()).To(HaveLen(3))
// 			Expect(eng.(*defaultEngine).baseViewStore[viewv1a1.GroupVersion.WithKind("dep")].List()).To(HaveLen(1))

// 			Expect(deltas).To(HaveLen(2))
// 			Expect(deltas).To(ContainElement(objFieldEq(dep1.UnstructuredContent(), "dep")))
// 			Expect(deltas).To(ContainElement(objFieldEq(pod1.UnstructuredContent(), "pod")))
// 			Expect(deltas).To(ContainElement(objFieldEq(dep1.UnstructuredContent(), "dep")))
// 			Expect(deltas).To(ContainElement(objFieldEq(pod3.UnstructuredContent(), "pod")))
// 		})

// 		It("should yield an empty delta when joining on a non-existent object", func() {
// 			jsonData := `{"@join":{"@eq":["$.dep.metadata.labels.app","$.rs.metadata.labels.app"]}}` //nolint:goconst
// 			j := newJoin(eng, []byte(jsonData))

// 			eng.WithObjects(pod1, pod2, pod3)
// 			deltas, err := j.Evaluate(cache.Delta{Type: cache.Added, Object: dep1})
// 			Expect(err).NotTo(HaveOccurred())
// 			Expect(deltas).To(BeEmpty())
// 		})

// 		It("should evaluate a join on a 3 views", func() {
// 			jsonData := `{"@join":{"@and":[{"@eq":["$.dep.metadata.name","$.pod.spec.parent"]},{"@eq":["$.dep.metadata.name","$.rs.spec.dep"]}]}}`
// 			j := newJoin(eng, []byte(jsonData))

// 			eng.WithObjects(dep1, dep2, rs1, rs2)
// 			deltas, err := j.Evaluate(cache.Delta{Type: cache.Added, Object: pod1})
// 			Expect(err).NotTo(HaveOccurred())
// 			Expect(deltas).To(HaveLen(1))

// 			delta := deltas[0]
// 			Expect(delta.IsUnchanged()).To(BeFalse())
// 			Expect(delta.Type).To(Equal(cache.Added))
// 			Expect(delta.Object.UnstructuredContent()["pod"]).To(Equal(pod1.UnstructuredContent()))
// 			Expect(delta.Object.UnstructuredContent()["dep"]).To(Equal(dep1.UnstructuredContent()))
// 			Expect(delta.Object.UnstructuredContent()["rs"]).To(Equal(rs1.UnstructuredContent()))
// 		})
// 	})

// 	Describe("Evaluating join expressions for Deleted events", func() {
// 		It("should evaluate a join on the pod parent", func() {
// 			jsonData := `{"@join":{"@eq":["$.dep.metadata.name","$.pod.spec.parent"]}}`
// 			j := newJoin(eng, []byte(jsonData))

// 			eng.WithObjects(dep1, dep2, pod3)
// 			Expect(eng.(*defaultEngine).baseViewStore[viewv1a1.GroupVersion.WithKind("pod")].List()).To(HaveLen(1))
// 			Expect(eng.(*defaultEngine).baseViewStore[viewv1a1.GroupVersion.WithKind("dep")].List()).To(HaveLen(2))

// 			deltas, err := j.Evaluate(cache.Delta{Type: cache.Deleted, Object: pod3})
// 			Expect(err).NotTo(HaveOccurred())

// 			Expect(eng.(*defaultEngine).baseViewStore[viewv1a1.GroupVersion.WithKind("pod")].List()).To(BeEmpty())
// 			Expect(eng.(*defaultEngine).baseViewStore[viewv1a1.GroupVersion.WithKind("dep")].List()).To(HaveLen(2))

// 			Expect(deltas).To(HaveLen(1))
// 			delta := deltas[0]
// 			Expect(delta.IsUnchanged()).To(BeFalse())
// 			Expect(delta.Type).To(Equal(cache.Deleted))
// 			Expect(delta.Object.UnstructuredContent()["pod"]).To(Equal(pod3.UnstructuredContent()))
// 			Expect(delta.Object.UnstructuredContent()["dep"]).To(Equal(dep2.UnstructuredContent()))
// 		})

// 		It("should skip the join when deleting a non-existent object", func() {
// 			jsonData := `{"@join":{"@eq":["$.dep.metadata.name","$.pod.spec.parent"]}}`
// 			j := newJoin(eng, []byte(jsonData))

// 			eng.WithObjects(dep1, dep2)
// 			_, err := j.Evaluate(cache.Delta{Type: cache.Deleted, Object: pod3})
// 			Expect(err).NotTo(HaveOccurred())
// 		})
// 	})

// 	Describe("Evaluating join expressions for Updated events", func() {
// 		It("should evaluate a simple join on labels", func() {
// 			jsonData := `{"@join":{"@eq":["$.dep.metadata.name","$.pod.spec.parent"]}}`
// 			j := newJoin(eng, []byte(jsonData))

// 			eng.WithObjects(dep1, dep2, pod1, pod2)
// 			Expect(eng.(*defaultEngine).baseViewStore[viewv1a1.GroupVersion.WithKind("pod")].List()).To(HaveLen(2))
// 			Expect(eng.(*defaultEngine).baseViewStore[viewv1a1.GroupVersion.WithKind("dep")].List()).To(HaveLen(2))

// 			deltas, err := j.Evaluate(cache.Delta{Type: cache.Upserted, Object: pod3})
// 			Expect(err).NotTo(HaveOccurred())

// 			Expect(eng.(*defaultEngine).baseViewStore[viewv1a1.GroupVersion.WithKind("pod")].List()).To(HaveLen(3))
// 			Expect(eng.(*defaultEngine).baseViewStore[viewv1a1.GroupVersion.WithKind("dep")].List()).To(HaveLen(2))

// 			Expect(deltas).To(HaveLen(1))
// 			delta := deltas[0]
// 			Expect(delta.IsUnchanged()).To(BeFalse())
// 			Expect(delta.Type).To(Equal(cache.Added))
// 			Expect(delta.Object.UnstructuredContent()["pod"]).To(Equal(pod3.UnstructuredContent()))
// 			Expect(delta.Object.UnstructuredContent()["dep"]).To(Equal(dep2.UnstructuredContent()))

// 			// change the image in pod3
// 			pod3.UnstructuredContent()["spec"].(unstruct)["image"] = "newimage"
// 			deltas, err = j.Evaluate(cache.Delta{Type: cache.Upserted, Object: pod3})
// 			Expect(err).NotTo(HaveOccurred())

// 			Expect(eng.(*defaultEngine).baseViewStore[viewv1a1.GroupVersion.WithKind("pod")].List()).To(HaveLen(3))
// 			Expect(eng.(*defaultEngine).baseViewStore[viewv1a1.GroupVersion.WithKind("dep")].List()).To(HaveLen(2))

// 			// should receive update pod3-dep1
// 			Expect(deltas).To(HaveLen(1))
// 			delta = deltas[0]
// 			Expect(delta.IsUnchanged()).To(BeFalse())
// 			Expect(delta.Type).To(Equal(cache.Updated))
// 			Expect(delta.Object.UnstructuredContent()["pod"]).To(Equal(pod3.UnstructuredContent()))
// 			Expect(delta.Object.UnstructuredContent()["dep"]).To(Equal(dep2.UnstructuredContent()))
// 		})

// 		It("should evaluate a join on labels that induces a remove followed by an add", func() {
// 			jsonData := `{"@join":{"@eq":["$.dep.metadata.labels.app","$.pod.metadata.labels.app"]}}`
// 			j := newJoin(eng, []byte(jsonData))

// 			eng.WithObjects(dep1, dep2, pod1, pod2)
// 			Expect(eng.(*defaultEngine).baseViewStore[viewv1a1.GroupVersion.WithKind("pod")].List()).To(HaveLen(2))
// 			Expect(eng.(*defaultEngine).baseViewStore[viewv1a1.GroupVersion.WithKind("dep")].List()).To(HaveLen(2))

// 			deltas, err := j.Evaluate(cache.Delta{Type: cache.Added, Object: pod3})
// 			Expect(err).NotTo(HaveOccurred())

// 			Expect(eng.(*defaultEngine).baseViewStore[viewv1a1.GroupVersion.WithKind("pod")].List()).To(HaveLen(3))
// 			Expect(eng.(*defaultEngine).baseViewStore[viewv1a1.GroupVersion.WithKind("dep")].List()).To(HaveLen(2))

// 			Expect(deltas).To(HaveLen(1))
// 			delta := deltas[0]
// 			Expect(delta.IsUnchanged()).To(BeFalse())
// 			Expect(delta.Type).To(Equal(cache.Added))
// 			Expect(delta.Object.UnstructuredContent()["pod"]).To(Equal(pod3.UnstructuredContent()))
// 			Expect(delta.Object.UnstructuredContent()["dep"]).To(Equal(dep1.UnstructuredContent()))

// 			// re-label pod3
// 			oldpod3 := object.DeepCopy(pod3)
// 			pod3.SetLabels(map[string]string{"app": "app2"})
// 			deltas, err = j.Evaluate(cache.Delta{Type: cache.Updated, Object: pod3})
// 			Expect(err).NotTo(HaveOccurred())

// 			Expect(eng.(*defaultEngine).baseViewStore[viewv1a1.GroupVersion.WithKind("pod")].List()).To(HaveLen(3))
// 			Expect(eng.(*defaultEngine).baseViewStore[viewv1a1.GroupVersion.WithKind("dep")].List()).To(HaveLen(2))

// 			// should receive a delete for pod3-dep1 and an add for pod3-dep2
// 			Expect(deltas).To(HaveLen(2))
// 			delta = deltas[0]
// 			Expect(delta.IsUnchanged()).To(BeFalse())
// 			Expect(delta.Type).To(Equal(cache.Deleted))
// 			Expect(delta.Object.UnstructuredContent()["pod"]).To(Equal(oldpod3.UnstructuredContent()))
// 			Expect(delta.Object.UnstructuredContent()["dep"]).To(Equal(dep1.UnstructuredContent()))

// 			delta = deltas[1]
// 			Expect(delta.IsUnchanged()).To(BeFalse())
// 			Expect(delta.Type).To(Equal(cache.Added))
// 			Expect(delta.Object.UnstructuredContent()["pod"]).To(Equal(pod3.UnstructuredContent()))
// 			Expect(delta.Object.UnstructuredContent()["dep"]).To(Equal(dep2.UnstructuredContent()))

// 			deltas, err = j.Evaluate(cache.Delta{Type: cache.Deleted, Object: pod3})
// 			Expect(err).NotTo(HaveOccurred())
// 			Expect(deltas).To(HaveLen(1))
// 			Expect(deltas[0].IsUnchanged()).To(BeFalse())
// 			Expect(deltas[0].Type).To(Equal(cache.Deleted))
// 			Expect(deltas[0].Object.UnstructuredContent()["pod"]).To(Equal(pod3.UnstructuredContent()))
// 			Expect(deltas[0].Object.UnstructuredContent()["dep"]).To(Equal(dep2.UnstructuredContent()))
// 		})

// 		It("should evaluate a complex join", func() {
// 			jsonData := `{"@join":{"@eq":["$.dep.metadata.labels.app","$.pod.metadata.labels.app"]}}`
// 			j := newJoin(eng, []byte(jsonData))

// 			eng.WithObjects(dep1, dep2, pod1, pod2, pod3)
// 			Expect(eng.(*defaultEngine).baseViewStore[viewv1a1.GroupVersion.WithKind("pod")].List()).To(HaveLen(3))
// 			Expect(eng.(*defaultEngine).baseViewStore[viewv1a1.GroupVersion.WithKind("dep")].List()).To(HaveLen(2))

// 			// re-label pod
// 			olddep1 := object.DeepCopy(dep1)
// 			dep1.SetLabels(map[string]string{"app": "app2"})
// 			deltas, err := j.Evaluate(cache.Delta{Type: cache.Updated, Object: dep1})
// 			Expect(err).NotTo(HaveOccurred())

// 			Expect(eng.(*defaultEngine).baseViewStore[viewv1a1.GroupVersion.WithKind("pod")].List()).To(HaveLen(3))
// 			Expect(eng.(*defaultEngine).baseViewStore[viewv1a1.GroupVersion.WithKind("dep")].List()).To(HaveLen(2))

// 			// should remove dep1-pod1 and dep1-pod3 and add dep1-pod2
// 			Expect(deltas).To(HaveLen(3))

// 			delta := deltas[0]
// 			Expect(delta.IsUnchanged()).To(BeFalse())
// 			if delta.Object.UnstructuredContent()["pod"].(unstruct)["metadata"].(unstruct)["name"] != "pod1" {
// 				delta = deltas[1]
// 			}
// 			Expect(delta.Type).To(Equal(cache.Deleted))
// 			Expect(delta.Object.UnstructuredContent()["pod"]).To(Equal(pod1.UnstructuredContent()))
// 			Expect(delta.Object.UnstructuredContent()["dep"]).To(Equal(olddep1.UnstructuredContent()))

// 			delta = deltas[1]
// 			Expect(delta.IsUnchanged()).To(BeFalse())
// 			if delta.Object.UnstructuredContent()["pod"].(unstruct)["metadata"].(unstruct)["name"] != "pod3" {
// 				delta = deltas[0]
// 			}
// 			Expect(delta.Type).To(Equal(cache.Deleted))
// 			Expect(delta.Object.UnstructuredContent()["pod"]).To(Equal(pod3.UnstructuredContent()))
// 			Expect(delta.Object.UnstructuredContent()["dep"]).To(Equal(olddep1.UnstructuredContent()))

// 			delta = deltas[2]
// 			Expect(delta.IsUnchanged()).To(BeFalse())
// 			Expect(delta.Type).To(Equal(cache.Added))
// 			Expect(delta.Object.UnstructuredContent()["pod"]).To(Equal(pod2.UnstructuredContent()))
// 			Expect(delta.Object.UnstructuredContent()["dep"]).To(Equal(dep1.UnstructuredContent()))
// 		})

// 		// 		It("parse a complex join expression from yaml", func() {
// 		// 			jsonData := `
// 		// "@join":
// 		//   "@and":
// 		//     - '@eq':
// 		//         - $.Service.metadata.name
// 		//         - '$["EndpointSlice"]["metadata"]["labels"]["kubernetes.io/service-name"]'
// 		//     - '@eq':
// 		//         - $.Service.metadata.namespace
// 		//         - $.EndpointSlice.metadata.namespace`

// 		// 			var j Join
// 		// 			err := yaml.Unmarshal([]byte(jsonData), &j)
// 		// 			Expect(err).NotTo(HaveOccurred())

// 		// 			logger.Info(fmt.Sprintf("%v", j))
// 		// 			logger.Info(j.String())
// 		// 			Expect(false).To(BeTrue())
// 		// 		})
// 	})
// })

// func objFieldEq(elem any, fields ...string) types.GomegaMatcher {
// 	return WithTransform(func(delta cache.Delta) any {
// 		val, ok, err := unstructured.NestedFieldNoCopy(delta.Object.UnstructuredContent(), fields...)
// 		if err != nil || !ok {
// 			return nil
// 		}
// 		return val
// 	}, Equal(elem))
// }

// func newJoin(eng Engine, data []byte) *Join {
// 	var j opv1a1.Join
// 	err := yaml.Unmarshal(data, &j)
// 	Expect(err).NotTo(HaveOccurred())
// 	return NewJoin(eng, &j)
// }

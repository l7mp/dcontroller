package pipeline

import (
	"encoding/json"

	"github.com/bsm/gomega/types"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"hsnlab/dcontroller-runtime/pkg/cache"
	"hsnlab/dcontroller-runtime/pkg/object"
)

var _ = Describe("Joins", func() {
	var dep1, dep2, pod1, pod2, pod3, rs1, rs2 *object.Object
	var eng Engine

	BeforeEach(func() {
		pod1 = object.New("pod").WithName("default", "pod1").
			WithContent(Unstructured{
				"spec": Unstructured{
					"image":  "image1",
					"parent": "dep1",
				},
			})
		pod1.SetLabels(map[string]string{"app": "app1"})

		pod2 = object.New("pod").WithName("other", "pod2").
			WithContent(Unstructured{
				"spec": Unstructured{
					"image":  "image2",
					"parent": "dep1",
				},
			})
		pod2.SetLabels(map[string]string{"app": "app2"})

		pod3 = object.New("pod").WithName("default", "pod3").
			WithContent(Unstructured{
				"spec": Unstructured{
					"image":  "image1",
					"parent": "dep2",
				},
			})
		pod3.SetLabels(map[string]string{"app": "app1"})

		dep1 = object.New("dep").WithName("default", "dep1").
			WithContent(Unstructured{
				"spec": Unstructured{
					"replicas": int64(3),
				},
			})
		dep1.SetLabels(map[string]string{"app": "app1"})

		dep2 = object.New("dep").WithName("default", "dep2").
			WithContent(Unstructured{
				"spec": Unstructured{
					"replicas": int64(1),
				},
			})
		dep2.SetLabels(map[string]string{"app": "app2"})

		rs1 = object.New("rs").WithName("default", "rs1").
			WithContent(Unstructured{
				"spec": Unstructured{
					"dep": "dep1",
				},
			})
		rs1.SetLabels(map[string]string{"app": "app1"})

		rs2 = object.New("rs").WithName("default", "rs2").
			WithContent(Unstructured{
				"spec": Unstructured{
					"dep": "dep2",
				},
			})
		rs2.SetLabels(map[string]string{"app": "app2"})

		eng = NewDefaultEngine("view", []string{"pod", "dep", "rs"}, logger)
	})

	Describe("Evaluating join expressions for Added events", func() {
		It("should evaluate a join on the pod parent", func() {
			jsonData := `{"@join":{"@eq":["$.dep.metadata.name","$.pod.spec.parent"]}}`
			var j Join
			err := json.Unmarshal([]byte(jsonData), &j)
			Expect(err).NotTo(HaveOccurred())

			eng.WithObjects(dep1, dep2)
			Expect(eng.(*defaultEngine).views["dep"].List()).To(HaveLen(2))
			Expect(eng.(*defaultEngine).views).NotTo(HaveKey("pod"))

			deltas, err := j.Evaluate(eng, cache.Delta{Type: cache.Upserted, Object: pod1})
			Expect(err).NotTo(HaveOccurred())
			Expect(eng.(*defaultEngine).views["dep"].List()).To(HaveLen(2))
			Expect(eng.(*defaultEngine).views["pod"].List()).To(HaveLen(1))

			Expect(deltas).To(HaveLen(1))
			delta := deltas[0]
			Expect(delta.IsUnchanged()).To(BeFalse())
			Expect(delta.Object.UnstructuredContent()["pod"]).To(Equal(pod1.UnstructuredContent()))
			Expect(delta.Object.UnstructuredContent()["dep"]).To(Equal(dep1.UnstructuredContent()))
		})

		It("should evaluate a join on pod namespace", func() {
			jsonData := `{"@join":{"@eq":["$.dep.metadata.namespace","$.pod.metadata.namespace"]}}`
			var j Join
			err := json.Unmarshal([]byte(jsonData), &j)
			Expect(err).NotTo(HaveOccurred())

			eng.WithObjects(dep1, dep2)
			deltas, err := j.Evaluate(eng, cache.Delta{Type: cache.Upserted, Object: pod1})
			Expect(err).NotTo(HaveOccurred())
			Expect(deltas).To(HaveLen(2))
			Expect(deltas).To(ContainElement(objFieldEq(dep1.UnstructuredContent(), "dep")))
			Expect(deltas).To(ContainElement(objFieldEq(pod1.UnstructuredContent(), "pod")))
			Expect(deltas).To(ContainElement(objFieldEq(dep2.UnstructuredContent(), "dep")))
			Expect(deltas).To(ContainElement(objFieldEq(pod1.UnstructuredContent(), "pod")))
		})

		It("should evaluate a join on labels", func() {
			jsonData := `{"@join":{"@eq":["$.dep.metadata.labels.app","$.pod.metadata.labels.app"]}}`
			var j Join
			err := json.Unmarshal([]byte(jsonData), &j)
			Expect(err).NotTo(HaveOccurred())

			eng.WithObjects(pod1, pod2, pod3)
			Expect(eng.(*defaultEngine).views["pod"].List()).To(HaveLen(3))
			Expect(eng.(*defaultEngine).views).NotTo(HaveKey("dep"))

			deltas, err := j.Evaluate(eng, cache.Delta{Type: cache.Upserted, Object: dep1})
			Expect(err).NotTo(HaveOccurred())

			Expect(eng.(*defaultEngine).views["pod"].List()).To(HaveLen(3))
			Expect(eng.(*defaultEngine).views["dep"].List()).To(HaveLen(1))

			Expect(deltas).To(HaveLen(2))
			Expect(deltas).To(ContainElement(objFieldEq(dep1.UnstructuredContent(), "dep")))
			Expect(deltas).To(ContainElement(objFieldEq(pod1.UnstructuredContent(), "pod")))
			Expect(deltas).To(ContainElement(objFieldEq(dep1.UnstructuredContent(), "dep")))
			Expect(deltas).To(ContainElement(objFieldEq(pod3.UnstructuredContent(), "pod")))
		})

		It("should yield an empty delta when joining on a non-existent object", func() {
			jsonData := `{"@join":{"@eq":["$.dep.metadata.labels.app","$.rs.metadata.labels.app"]}}`
			var j Join
			err := json.Unmarshal([]byte(jsonData), &j)
			Expect(err).NotTo(HaveOccurred())

			eng.WithObjects(pod1, pod2, pod3)
			deltas, err := j.Evaluate(eng, cache.Delta{Type: cache.Added, Object: dep1})
			Expect(err).NotTo(HaveOccurred())
			Expect(deltas).To(HaveLen(0))
		})

		It("should evaluate a join on a 3 views", func() {
			jsonData := `{"@join":{"@and":[{"@eq":["$.dep.metadata.name","$.pod.spec.parent"]},{"@eq":["$.dep.metadata.name","$.rs.spec.dep"]}]}}`
			var j Join
			err := json.Unmarshal([]byte(jsonData), &j)
			Expect(err).NotTo(HaveOccurred())

			eng.WithObjects(dep1, dep2, rs1, rs2)
			deltas, err := j.Evaluate(eng, cache.Delta{Type: cache.Added, Object: pod1})
			Expect(err).NotTo(HaveOccurred())
			Expect(deltas).To(HaveLen(1))

			delta := deltas[0]
			Expect(delta.IsUnchanged()).To(BeFalse())
			Expect(delta.Type).To(Equal(cache.Added))
			Expect(delta.Object.UnstructuredContent()["pod"]).To(Equal(pod1.UnstructuredContent()))
			Expect(delta.Object.UnstructuredContent()["dep"]).To(Equal(dep1.UnstructuredContent()))
			Expect(delta.Object.UnstructuredContent()["rs"]).To(Equal(rs1.UnstructuredContent()))
		})
	})

	Describe("Evaluating join expressions for Deleted events", func() {
		It("should evaluate a join on the pod parent", func() {
			jsonData := `{"@join":{"@eq":["$.dep.metadata.name","$.pod.spec.parent"]}}`
			var j Join
			err := json.Unmarshal([]byte(jsonData), &j)
			Expect(err).NotTo(HaveOccurred())

			eng.WithObjects(dep1, dep2, pod3)
			Expect(eng.(*defaultEngine).views["pod"].List()).To(HaveLen(1))
			Expect(eng.(*defaultEngine).views["dep"].List()).To(HaveLen(2))

			deltas, err := j.Evaluate(eng, cache.Delta{Type: cache.Deleted, Object: pod3})
			Expect(err).NotTo(HaveOccurred())

			Expect(eng.(*defaultEngine).views["pod"].List()).To(HaveLen(0))
			Expect(eng.(*defaultEngine).views["dep"].List()).To(HaveLen(2))

			Expect(deltas).To(HaveLen(1))
			delta := deltas[0]
			Expect(delta.IsUnchanged()).To(BeFalse())
			Expect(delta.Type).To(Equal(cache.Deleted))
			Expect(delta.Object.UnstructuredContent()["pod"]).To(Equal(pod3.UnstructuredContent()))
			Expect(delta.Object.UnstructuredContent()["dep"]).To(Equal(dep2.UnstructuredContent()))
		})

		It("should skip the join when deleting a non-existent object", func() {
			jsonData := `{"@join":{"@eq":["$.dep.metadata.name","$.pod.spec.parent"]}}`
			var j Join
			err := json.Unmarshal([]byte(jsonData), &j)
			Expect(err).NotTo(HaveOccurred())

			eng.WithObjects(dep1, dep2)
			_, err = j.Evaluate(eng, cache.Delta{Type: cache.Deleted, Object: pod3})
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("Evaluating join expressions for Updated events", func() {
		It("should evaluate a simple join on labels", func() {
			jsonData := `{"@join":{"@eq":["$.dep.metadata.name","$.pod.spec.parent"]}}`
			var j Join
			err := json.Unmarshal([]byte(jsonData), &j)
			Expect(err).NotTo(HaveOccurred())

			eng.WithObjects(dep1, dep2, pod1, pod2)
			Expect(eng.(*defaultEngine).views["pod"].List()).To(HaveLen(2))
			Expect(eng.(*defaultEngine).views["dep"].List()).To(HaveLen(2))

			deltas, err := j.Evaluate(eng, cache.Delta{Type: cache.Upserted, Object: pod3})
			Expect(err).NotTo(HaveOccurred())

			Expect(eng.(*defaultEngine).views["pod"].List()).To(HaveLen(3))
			Expect(eng.(*defaultEngine).views["dep"].List()).To(HaveLen(2))

			Expect(deltas).To(HaveLen(1))
			delta := deltas[0]
			Expect(delta.IsUnchanged()).To(BeFalse())
			Expect(delta.Type).To(Equal(cache.Added))
			Expect(delta.Object.UnstructuredContent()["pod"]).To(Equal(pod3.UnstructuredContent()))
			Expect(delta.Object.UnstructuredContent()["dep"]).To(Equal(dep2.UnstructuredContent()))

			// change the image in pod3
			pod3.UnstructuredContent()["spec"].(Unstructured)["image"] = "newimage"
			deltas, err = j.Evaluate(eng, cache.Delta{Type: cache.Upserted, Object: pod3})
			Expect(err).NotTo(HaveOccurred())

			Expect(eng.(*defaultEngine).views["pod"].List()).To(HaveLen(3))
			Expect(eng.(*defaultEngine).views["dep"].List()).To(HaveLen(2))

			// should receive update pod3-dep1
			Expect(deltas).To(HaveLen(1))
			delta = deltas[0]
			Expect(delta.IsUnchanged()).To(BeFalse())
			Expect(delta.Type).To(Equal(cache.Updated))
			Expect(delta.Object.UnstructuredContent()["pod"]).To(Equal(pod3.UnstructuredContent()))
			Expect(delta.Object.UnstructuredContent()["dep"]).To(Equal(dep2.UnstructuredContent()))

			// should yield the same update on pod3-dep1
			deltas, err = j.Evaluate(eng, cache.Delta{Type: cache.Updated, Object: pod3})
			Expect(err).NotTo(HaveOccurred())
			Expect(deltas).To(HaveLen(1))
			Expect(deltas[0].IsUnchanged()).To(BeFalse())
			Expect(deltas[0].Type).To(Equal(cache.Updated))
			Expect(deltas[0].Object.UnstructuredContent()["pod"]).To(Equal(pod3.UnstructuredContent()))
			Expect(deltas[0].Object.UnstructuredContent()["dep"]).To(Equal(dep2.UnstructuredContent()))
		})

		It("should evaluate a join on labels that induces a remove followed by an add", func() {
			jsonData := `{"@join":{"@eq":["$.dep.metadata.labels.app","$.pod.metadata.labels.app"]}}`
			var j Join
			err := json.Unmarshal([]byte(jsonData), &j)
			Expect(err).NotTo(HaveOccurred())

			eng.WithObjects(dep1, dep2, pod1, pod2)
			Expect(eng.(*defaultEngine).views["pod"].List()).To(HaveLen(2))
			Expect(eng.(*defaultEngine).views["dep"].List()).To(HaveLen(2))

			deltas, err := j.Evaluate(eng, cache.Delta{Type: cache.Added, Object: pod3})
			Expect(err).NotTo(HaveOccurred())

			Expect(eng.(*defaultEngine).views["pod"].List()).To(HaveLen(3))
			Expect(eng.(*defaultEngine).views["dep"].List()).To(HaveLen(2))

			Expect(deltas).To(HaveLen(1))
			delta := deltas[0]
			Expect(delta.IsUnchanged()).To(BeFalse())
			Expect(delta.Type).To(Equal(cache.Added))
			Expect(delta.Object.UnstructuredContent()["pod"]).To(Equal(pod3.UnstructuredContent()))
			Expect(delta.Object.UnstructuredContent()["dep"]).To(Equal(dep1.UnstructuredContent()))

			// re-label pod3
			oldpod3 := pod3.DeepCopy()
			pod3.SetLabels(map[string]string{"app": "app2"})
			deltas, err = j.Evaluate(eng, cache.Delta{Type: cache.Updated, Object: pod3})
			Expect(err).NotTo(HaveOccurred())

			Expect(eng.(*defaultEngine).views["pod"].List()).To(HaveLen(3))
			Expect(eng.(*defaultEngine).views["dep"].List()).To(HaveLen(2))

			// should receive a delete for pod3-dep1 and an add for pod3-dep2
			Expect(deltas).To(HaveLen(2))
			delta = deltas[0]
			Expect(delta.IsUnchanged()).To(BeFalse())
			Expect(delta.Type).To(Equal(cache.Deleted))
			Expect(delta.Object.UnstructuredContent()["pod"]).To(Equal(oldpod3.UnstructuredContent()))
			Expect(delta.Object.UnstructuredContent()["dep"]).To(Equal(dep1.UnstructuredContent()))

			delta = deltas[1]
			Expect(delta.IsUnchanged()).To(BeFalse())
			Expect(delta.Type).To(Equal(cache.Added))
			Expect(delta.Object.UnstructuredContent()["pod"]).To(Equal(pod3.UnstructuredContent()))
			Expect(delta.Object.UnstructuredContent()["dep"]).To(Equal(dep2.UnstructuredContent()))

			// should now yield a single update on pod3-dep2
			deltas, err = j.Evaluate(eng, cache.Delta{Type: cache.Updated, Object: pod3})
			Expect(err).NotTo(HaveOccurred())
			Expect(deltas).To(HaveLen(1))
			Expect(deltas[0].IsUnchanged()).To(BeFalse())
			Expect(deltas[0].Type).To(Equal(cache.Updated))
			Expect(deltas[0].Object.UnstructuredContent()["pod"]).To(Equal(pod3.UnstructuredContent()))
			Expect(deltas[0].Object.UnstructuredContent()["dep"]).To(Equal(dep2.UnstructuredContent()))
		})

		It("should evaluate a complex join", func() {
			jsonData := `{"@join":{"@eq":["$.dep.metadata.labels.app","$.pod.metadata.labels.app"]}}`
			var j Join
			err := json.Unmarshal([]byte(jsonData), &j)
			Expect(err).NotTo(HaveOccurred())

			eng.WithObjects(dep1, dep2, pod1, pod2, pod3)
			Expect(eng.(*defaultEngine).views["pod"].List()).To(HaveLen(3))
			Expect(eng.(*defaultEngine).views["dep"].List()).To(HaveLen(2))

			// re-label pod
			olddep1 := dep1.DeepCopy()
			dep1.SetLabels(map[string]string{"app": "app2"})
			deltas, err := j.Evaluate(eng, cache.Delta{Type: cache.Updated, Object: dep1})
			Expect(err).NotTo(HaveOccurred())

			Expect(eng.(*defaultEngine).views["pod"].List()).To(HaveLen(3))
			Expect(eng.(*defaultEngine).views["dep"].List()).To(HaveLen(2))

			// should remove dep1-pod1 and dep1-pod3 and add dep1-pod2
			Expect(deltas).To(HaveLen(3))

			delta := deltas[0]
			Expect(delta.IsUnchanged()).To(BeFalse())
			if delta.Object.UnstructuredContent()["pod"].(Unstructured)["metadata"].(Unstructured)["name"] != "pod1" {
				delta = deltas[1]
			}
			Expect(delta.Type).To(Equal(cache.Deleted))
			Expect(delta.Object.UnstructuredContent()["pod"]).To(Equal(pod1.UnstructuredContent()))
			Expect(delta.Object.UnstructuredContent()["dep"]).To(Equal(olddep1.UnstructuredContent()))

			delta = deltas[1]
			Expect(delta.IsUnchanged()).To(BeFalse())
			if delta.Object.UnstructuredContent()["pod"].(Unstructured)["metadata"].(Unstructured)["name"] != "pod3" {
				delta = deltas[0]
			}
			Expect(delta.Type).To(Equal(cache.Deleted))
			Expect(delta.Object.UnstructuredContent()["pod"]).To(Equal(pod3.UnstructuredContent()))
			Expect(delta.Object.UnstructuredContent()["dep"]).To(Equal(olddep1.UnstructuredContent()))

			delta = deltas[2]
			Expect(delta.IsUnchanged()).To(BeFalse())
			Expect(delta.Type).To(Equal(cache.Added))
			Expect(delta.Object.UnstructuredContent()["pod"]).To(Equal(pod2.UnstructuredContent()))
			Expect(delta.Object.UnstructuredContent()["dep"]).To(Equal(dep1.UnstructuredContent()))

			// should now yield a single update on pod2-dep1
			deltas, err = j.Evaluate(eng, cache.Delta{Type: cache.Updated, Object: dep1})
			Expect(err).NotTo(HaveOccurred())
			Expect(deltas).To(HaveLen(1))
			Expect(deltas[0].IsUnchanged()).To(BeFalse())
			Expect(deltas[0].Type).To(Equal(cache.Updated))
			Expect(deltas[0].Object.UnstructuredContent()["pod"]).To(Equal(pod2.UnstructuredContent()))
			Expect(deltas[0].Object.UnstructuredContent()["dep"]).To(Equal(dep1.UnstructuredContent()))
		})
	})
})

func objFieldEq(elem any, fields ...string) types.GomegaMatcher {
	return WithTransform(func(delta cache.Delta) any {
		val, ok, err := unstructured.NestedFieldNoCopy(delta.Object.UnstructuredContent(), fields...)
		if err != nil || !ok {
			return nil
		}
		return val
	}, Equal(elem))
}

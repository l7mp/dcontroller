package view

import (
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

var _ = Describe("Predicate", func() {
	Context("with simple predicates", func() {
		It("should marshal and unmarshal GenerationChangedPredicate", func() {
			t := BasicPredicate("GenerationChanged")
			pred := Predicate{BasicPredicate: &t}

			data, err := json.Marshal(pred)
			Expect(err).NotTo(HaveOccurred())

			var unmarshaledPred Predicate
			err = json.Unmarshal(data, &unmarshaledPred)
			Expect(err).NotTo(HaveOccurred())

			reconstitutedPred, err := unmarshaledPred.ToPredicate()
			Expect(err).NotTo(HaveOccurred())
			Expect(reconstitutedPred).To(BeAssignableToTypeOf(predicate.GenerationChangedPredicate{}))
		})

		It("should marshal and unmarshal an Or predicate", func() {
			t1 := BasicPredicate("GenerationChanged")
			t2 := BasicPredicate("ResourceVersionChanged")
			compPred := BoolPredicate(map[string][]Predicate{
				"Or": []Predicate{{BasicPredicate: &t1}, {BasicPredicate: &t2}},
			})

			data, err := json.Marshal(compPred)
			Expect(err).NotTo(HaveOccurred())

			var unmarshaledCompPred BoolPredicate
			err = json.Unmarshal(data, &unmarshaledCompPred)
			Expect(err).NotTo(HaveOccurred())

			reconstitutedPred, err := unmarshaledCompPred.ToPredicate()
			Expect(err).NotTo(HaveOccurred())
			Expect(reconstitutedPred).To(BeAssignableToTypeOf(predicate.Or[client.Object]()))

			oldObj := &unstructured.Unstructured{}
			oldObj.SetGeneration(1)
			oldObj.SetResourceVersion("a")

			newObjGenChanged := &unstructured.Unstructured{}
			newObjGenChanged.SetGeneration(2)
			newObjGenChanged.SetResourceVersion("a")

			newObjResourceVerChanged := &unstructured.Unstructured{}
			newObjResourceVerChanged.SetGeneration(1)
			newObjResourceVerChanged.SetResourceVersion("b")

			newObjBothChanged := &unstructured.Unstructured{}
			newObjBothChanged.SetGeneration(2)
			newObjBothChanged.SetResourceVersion("b")

			Expect(reconstitutedPred.Update(event.UpdateEvent{
				ObjectOld: client.Object(oldObj), ObjectNew: client.Object(newObjGenChanged),
			})).To(BeTrue())
			Expect(reconstitutedPred.Update(event.UpdateEvent{
				ObjectOld: client.Object(oldObj), ObjectNew: client.Object(newObjResourceVerChanged),
			})).To(BeTrue())
			Expect(reconstitutedPred.Update(event.UpdateEvent{
				ObjectOld: client.Object(oldObj), ObjectNew: client.Object(newObjBothChanged),
			})).To(BeTrue())
		})

		It("should marshal and unmarshal and apply a Namespace predicate", func() {
			t := BasicPredicate("GenerationChanged")
			compPred := Predicate{BoolPredicate: &BoolPredicate{
				"Not": []Predicate{{BasicPredicate: &t}},
			}}

			data, err := json.Marshal(compPred)
			Expect(err).NotTo(HaveOccurred())

			var unmarshaledCompPred BoolPredicate
			err = json.Unmarshal(data, &unmarshaledCompPred)
			Expect(err).NotTo(HaveOccurred())

			reconstitutedPred, err := unmarshaledCompPred.ToPredicate()
			Expect(err).NotTo(HaveOccurred())
			Expect(reconstitutedPred).To(BeAssignableToTypeOf(
				predicate.Not(predicate.GenerationChangedPredicate{})))

			oldObj := &unstructured.Unstructured{}
			oldObj.SetGeneration(1)

			newObjGenChanged := &unstructured.Unstructured{}
			newObjGenChanged.SetGeneration(2)

			newObjGenUnchanged := &unstructured.Unstructured{}
			newObjGenUnchanged.SetGeneration(1)

			Expect(reconstitutedPred.Update(event.UpdateEvent{
				ObjectOld: client.Object(oldObj), ObjectNew: client.Object(newObjGenChanged),
			})).To(BeFalse())
			Expect(reconstitutedPred.Update(event.UpdateEvent{
				ObjectOld: client.Object(oldObj), ObjectNew: client.Object(newObjGenUnchanged),
			})).To(BeTrue())
		})
	})
})

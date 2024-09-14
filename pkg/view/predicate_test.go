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

var _ = Describe("Predicate Marshaling", func() {
	Context("with standard predicates", func() {
		It("should marshal and unmarshal GenerationChangedPredicate", func() {
			pred := predicate.GenerationChangedPredicate{}
			data, err := MarshalBasicPredicate(pred)
			Expect(err).NotTo(HaveOccurred())

			unmarshaledPred, err := UnmarshalPredicate(data)
			Expect(err).NotTo(HaveOccurred())
			Expect(unmarshaledPred).To(BeAssignableToTypeOf(predicate.GenerationChangedPredicate{}))
		})

		It("should marshal and unmarshal ResourceVersionChangedPredicate", func() {
			pred := predicate.ResourceVersionChangedPredicate{}
			data, err := MarshalBasicPredicate(pred)
			Expect(err).NotTo(HaveOccurred())

			unmarshaledPred, err := UnmarshalPredicate(data)
			Expect(err).NotTo(HaveOccurred())
			Expect(unmarshaledPred).To(BeAssignableToTypeOf(predicate.ResourceVersionChangedPredicate{}))
		})

		It("should marshal and unmarshal LabelChangedPredicate", func() {
			pred := predicate.LabelChangedPredicate{}
			data, err := MarshalBasicPredicate(pred)
			Expect(err).NotTo(HaveOccurred())

			unmarshaledPred, err := UnmarshalPredicate(data)
			Expect(err).NotTo(HaveOccurred())
			Expect(unmarshaledPred).To(BeAssignableToTypeOf(predicate.LabelChangedPredicate{}))
		})

		It("should marshal and unmarshal AnnotationChangedPredicate", func() {
			pred := predicate.AnnotationChangedPredicate{}
			data, err := MarshalBasicPredicate(pred)
			Expect(err).NotTo(HaveOccurred())

			unmarshaledPred, err := UnmarshalPredicate(data)
			Expect(err).NotTo(HaveOccurred())
			Expect(unmarshaledPred).To(BeAssignableToTypeOf(predicate.AnnotationChangedPredicate{}))
		})
	})

	Context("with composite predicates", func() {
		It("should marshal and unmarshal And predicate", func() {
			t1 := "GenerationChanged"
			t2 := "ResourceVersionChanged"
			compPred := BoolPredicate{
				Type: "And",
				Predicates: []BasicPredicate{
					{Type: &t1},
					{Type: &t2},
				},
			}

			data, err := json.Marshal(compPred)
			Expect(err).NotTo(HaveOccurred())

			var unmarshaledCompPred BoolPredicate
			err = json.Unmarshal(data, &unmarshaledCompPred)
			Expect(err).NotTo(HaveOccurred())

			reconstitutedPred, err := unmarshaledCompPred.ToPredicate()
			Expect(err).NotTo(HaveOccurred())
			Expect(reconstitutedPred).To(BeAssignableToTypeOf(predicate.And[client.Object]()))
		})

		It("should marshal and unmarshal Or predicate", func() {
			t1 := "LabelChanged"
			t2 := "AnnotationChanged"
			compPred := BoolPredicate{
				Type: "Or",
				Predicates: []BasicPredicate{
					{Type: &t1},
					{Type: &t2},
				},
			}

			data, err := json.Marshal(compPred)
			Expect(err).NotTo(HaveOccurred())

			var unmarshaledCompPred BoolPredicate
			err = json.Unmarshal(data, &unmarshaledCompPred)
			Expect(err).NotTo(HaveOccurred())

			reconstitutedPred, err := unmarshaledCompPred.ToPredicate()
			Expect(err).NotTo(HaveOccurred())
			Expect(reconstitutedPred).To(BeAssignableToTypeOf(predicate.Or[client.Object]()))
		})
	})

	Context("predicate behavior", func() {
		It("should correctly apply GenerationChangedPredicate", func() {
			t := "GenerationChanged"
			compPred := BasicPredicate{
				Type: &t,
			}

			reconstitutedPred, err := compPred.ToPredicate()
			Expect(err).NotTo(HaveOccurred())

			oldObj := &unstructured.Unstructured{}
			oldObj.SetGeneration(1)
			newObj := &unstructured.Unstructured{}
			newObj.SetGeneration(2)

			Expect(reconstitutedPred.Create(event.CreateEvent{
				Object: client.Object(oldObj),
			})).To(BeTrue())
			Expect(reconstitutedPred.Update(event.UpdateEvent{
				ObjectOld: client.Object(oldObj), ObjectNew: client.Object(newObj),
			})).To(BeTrue())
			Expect(reconstitutedPred.Update(event.UpdateEvent{
				ObjectOld: client.Object(oldObj), ObjectNew: client.Object(oldObj),
			})).To(BeFalse())
			Expect(reconstitutedPred.Delete(event.DeleteEvent{
				Object: client.Object(oldObj),
			})).To(BeTrue())
			Expect(reconstitutedPred.Generic(event.GenericEvent{
				Object: client.Object(oldObj)})).To(BeTrue())
		})

		It("should correctly apply composite And predicate", func() {
			t1 := "GenerationChanged"
			t2 := "LabelChanged"
			compPred := BoolPredicate{
				Type: "And",
				Predicates: []BasicPredicate{
					{Type: &t1},
					{Type: &t2},
				},
			}

			reconstitutedPred, err := compPred.ToPredicate()
			Expect(err).NotTo(HaveOccurred())

			oldObj := &unstructured.Unstructured{}
			oldObj.SetGeneration(1)
			oldObj.SetLabels(map[string]string{"foo": "bar"})

			newObjGenChanged := &unstructured.Unstructured{}
			newObjGenChanged.SetGeneration(2)
			newObjGenChanged.SetLabels(map[string]string{"foo": "bar"})

			newObjLabelChanged := &unstructured.Unstructured{}
			newObjLabelChanged.SetGeneration(1)
			newObjLabelChanged.SetLabels(map[string]string{"foo": "baz"})

			newObjBothChanged := &unstructured.Unstructured{}
			newObjBothChanged.SetGeneration(2)
			newObjBothChanged.SetLabels(map[string]string{"foo": "baz"})

			Expect(reconstitutedPred.Update(event.UpdateEvent{
				ObjectOld: client.Object(oldObj), ObjectNew: client.Object(newObjGenChanged),
			})).To(BeFalse())
			Expect(reconstitutedPred.Update(event.UpdateEvent{
				ObjectOld: client.Object(oldObj), ObjectNew: client.Object(newObjLabelChanged),
			})).To(BeFalse())
			Expect(reconstitutedPred.Update(event.UpdateEvent{
				ObjectOld: client.Object(oldObj), ObjectNew: client.Object(newObjBothChanged),
			})).To(BeTrue())
		})
	})

	Context("with actual predicates", func() {
		It("should marshal and unmarshal GenerationChangedPredicate", func() {
			t := "GenerationChanged"
			pred := Predicate{basic: &BasicPredicate{Type: &t}}

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
			t1 := "GenerationChanged"
			t2 := "ResourceVersionChanged"
			compPred := Predicate{
				boolp: &BoolPredicate{
					Type: "Or",
					Predicates: []BasicPredicate{
						{Type: &t1},
						{Type: &t2},
					},
				},
			}

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
			oldObj.SetResourceVersion("a")

			newObjResourceVerChanged := &unstructured.Unstructured{}
			newObjResourceVerChanged.SetGeneration(1)
			oldObj.SetResourceVersion("b")

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
			compPred := Predicate{
				boolp: &BoolPredicate{
					Type: "Not",
					Predicates: []BasicPredicate{
						{TypeArg: &map[string]string{"Namespace": "default"}},
					},
				},
			}

			data, err := json.Marshal(compPred)
			Expect(err).NotTo(HaveOccurred())

			var unmarshaledCompPred BoolPredicate
			err = json.Unmarshal(data, &unmarshaledCompPred)
			Expect(err).NotTo(HaveOccurred())

			reconstitutedPred, err := unmarshaledCompPred.ToPredicate()
			Expect(err).NotTo(HaveOccurred())
			Expect(reconstitutedPred).To(BeAssignableToTypeOf(
				predicate.Not[client.Object](predicate.GenerationChangedPredicate{})))

			oldObj := &unstructured.Unstructured{}
			oldObj.SetNamespace("default")

			newObjNsChanged := &unstructured.Unstructured{}
			newObjNsChanged.SetNamespace("prod")

			newObjNsUnchanged := &unstructured.Unstructured{}
			newObjNsUnchanged.SetNamespace("default")

			Expect(reconstitutedPred.Update(event.UpdateEvent{
				ObjectOld: client.Object(oldObj), ObjectNew: client.Object(newObjNsChanged),
			})).To(BeTrue())
			Expect(reconstitutedPred.Update(event.UpdateEvent{
				ObjectOld: client.Object(oldObj), ObjectNew: client.Object(newObjNsUnchanged),
			})).To(BeFalse())
		})
	})
})

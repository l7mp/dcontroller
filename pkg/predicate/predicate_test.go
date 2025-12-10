package predicate

import (
	"context"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"github.com/l7mp/dcontroller/internal/testutils"
	"github.com/l7mp/dcontroller/pkg/cache"
)

func TestPredicate(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Predicate")
}

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
				"Or": {{BasicPredicate: &t1}, {BasicPredicate: &t2}},
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

	Context("with view cache and GenerationChanged predicate", func() {
		var (
			viewCache *cache.ViewCache
			ctx       context.Context
			cancel    context.CancelFunc
			gvk       schema.GroupVersionKind
		)

		BeforeEach(func() {
			viewCache = cache.NewViewCache(cache.CacheOptions{})
			ctx, cancel = context.WithCancel(context.Background())
			gvk = schema.GroupVersionKind{
				Group:   "test.view.dcontroller.io",
				Version: "v1alpha1",
				Kind:    "TestView",
			}
			err := viewCache.RegisterCacheForKind(gvk)
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			cancel()
		})

		It("should trigger watch events when generation changes on Add", func() {
			// Create a GenerationChanged predicate
			genChangedPred := BasicPredicate("GenerationChanged")
			pred, err := genChangedPred.ToPredicate()
			Expect(err).NotTo(HaveOccurred())

			// Set up a watch on the view cache
			list := &unstructured.UnstructuredList{}
			list.SetGroupVersionKind(schema.GroupVersionKind{
				Group:   gvk.Group,
				Version: gvk.Version,
				Kind:    gvk.Kind + "List",
			})

			watcher, err := viewCache.Watch(ctx, list)
			Expect(err).NotTo(HaveOccurred())
			defer watcher.Stop()

			// Create an object with generation 1
			obj := &unstructured.Unstructured{}
			obj.SetGroupVersionKind(gvk)
			obj.SetNamespace("test-ns")
			obj.SetName("test-obj")
			obj.SetGeneration(1)

			// Add the object to the cache
			err = viewCache.Add(obj)
			Expect(err).NotTo(HaveOccurred())

			// Wait for the watch event
			watchEvent, ok := testutils.TryWatchEvent(watcher, 100*time.Millisecond)
			Expect(ok).To(BeTrue(), "should receive watch event for Add")
			Expect(watchEvent.Type).To(Equal(watch.Added))

			// Verify the predicate allows Create events
			Expect(pred.Create(event.CreateEvent{Object: watchEvent.Object.(client.Object)})).To(BeTrue())
		})

		It("should trigger watch events when generation changes on Update via cache", func() {
			// Create a GenerationChanged predicate
			genChangedPred := BasicPredicate("GenerationChanged")
			pred, err := genChangedPred.ToPredicate()
			Expect(err).NotTo(HaveOccurred())

			// Create an object with generation 1
			oldObj := &unstructured.Unstructured{}
			oldObj.SetGroupVersionKind(gvk)
			oldObj.SetNamespace("test-ns")
			oldObj.SetName("test-obj")
			oldObj.SetGeneration(1)

			// Add the object first
			err = viewCache.Add(oldObj)
			Expect(err).NotTo(HaveOccurred())

			// Set up a watch after adding the object
			list := &unstructured.UnstructuredList{}
			list.SetGroupVersionKind(schema.GroupVersionKind{
				Group:   gvk.Group,
				Version: gvk.Version,
				Kind:    gvk.Kind + "List",
			})

			watcher, err := viewCache.Watch(ctx, list)
			Expect(err).NotTo(HaveOccurred())
			defer watcher.Stop()

			// Consume the initial Add event
			_, ok := testutils.TryWatchEvent(watcher, 100*time.Millisecond)
			Expect(ok).To(BeTrue(), "should receive initial Add event")

			// Update with generation changed
			newObj := oldObj.DeepCopy()
			newObj.SetGeneration(2)

			err = viewCache.Update(oldObj, newObj)
			Expect(err).NotTo(HaveOccurred())

			// Wait for the update event
			watchEvent, ok := testutils.TryWatchEvent(watcher, 100*time.Millisecond)
			Expect(ok).To(BeTrue(), "should receive watch event for Update")
			Expect(watchEvent.Type).To(Equal(watch.Modified))

			// Verify the predicate allows this update (generation changed)
			updateEvent := event.UpdateEvent{
				ObjectOld: client.Object(oldObj),
				ObjectNew: watchEvent.Object.(client.Object),
			}
			Expect(pred.Update(updateEvent)).To(BeTrue(), "predicate should allow generation change")
		})

		It("should NOT trigger watch events when generation does NOT change on Update", func() {
			// Create a GenerationChanged predicate
			genChangedPred := BasicPredicate("GenerationChanged")
			pred, err := genChangedPred.ToPredicate()
			Expect(err).NotTo(HaveOccurred())

			// Create an object with generation 1
			oldObj := &unstructured.Unstructured{}
			oldObj.SetGroupVersionKind(gvk)
			oldObj.SetNamespace("test-ns")
			oldObj.SetName("test-obj")
			oldObj.SetGeneration(1)
			oldObj.SetResourceVersion("v1")

			// Add the object first
			err = viewCache.Add(oldObj)
			Expect(err).NotTo(HaveOccurred())

			// Set up a watch after adding the object
			list := &unstructured.UnstructuredList{}
			list.SetGroupVersionKind(schema.GroupVersionKind{
				Group:   gvk.Group,
				Version: gvk.Version,
				Kind:    gvk.Kind + "List",
			})

			watcher, err := viewCache.Watch(ctx, list)
			Expect(err).NotTo(HaveOccurred())
			defer watcher.Stop()

			// Consume the initial Add event
			_, ok := testutils.TryWatchEvent(watcher, 100*time.Millisecond)
			Expect(ok).To(BeTrue(), "should receive initial Add event")

			// Update with generation unchanged (only resourceVersion changed)
			newObj := oldObj.DeepCopy()
			newObj.SetResourceVersion("v2")

			err = viewCache.Update(oldObj, newObj)
			Expect(err).NotTo(HaveOccurred())

			// Wait for the update event
			watchEvent, ok := testutils.TryWatchEvent(watcher, 100*time.Millisecond)
			Expect(ok).To(BeTrue(), "should receive watch event for Update")
			Expect(watchEvent.Type).To(Equal(watch.Modified))

			// Verify the predicate BLOCKS this update (generation unchanged)
			updateEvent := event.UpdateEvent{
				ObjectOld: client.Object(oldObj),
				ObjectNew: watchEvent.Object.(client.Object),
			}
			Expect(pred.Update(updateEvent)).To(BeFalse(), "predicate should block updates without generation change")
		})

		It("should trigger watch events when generation changes on Update via client", func() {
			// Create a GenerationChanged predicate
			genChangedPred := BasicPredicate("GenerationChanged")
			pred, err := genChangedPred.ToPredicate()
			Expect(err).NotTo(HaveOccurred())

			// Get the view cache client
			viewClient := viewCache.GetClient()

			// Create an object with generation 1
			oldObj := &unstructured.Unstructured{}
			oldObj.SetGroupVersionKind(gvk)
			oldObj.SetNamespace("test-ns")
			oldObj.SetName("test-obj")
			oldObj.SetGeneration(1)

			// Create via client
			err = viewClient.Create(ctx, oldObj)
			Expect(err).NotTo(HaveOccurred())

			// Set up a watch after creating the object
			list := &unstructured.UnstructuredList{}
			list.SetGroupVersionKind(schema.GroupVersionKind{
				Group:   gvk.Group,
				Version: gvk.Version,
				Kind:    gvk.Kind + "List",
			})

			watcher, err := viewCache.Watch(ctx, list)
			Expect(err).NotTo(HaveOccurred())
			defer watcher.Stop()

			// Consume the initial Add event
			_, ok := testutils.TryWatchEvent(watcher, 100*time.Millisecond)
			Expect(ok).To(BeTrue(), "should receive initial Add event")

			// Update via client with generation changed
			updatedObj := oldObj.DeepCopy()
			updatedObj.SetGeneration(2)

			err = viewClient.Update(ctx, updatedObj)
			Expect(err).NotTo(HaveOccurred())

			// Wait for the update event
			watchEvent, ok := testutils.TryWatchEvent(watcher, 100*time.Millisecond)
			Expect(ok).To(BeTrue(), "should receive watch event for Update via client")
			Expect(watchEvent.Type).To(Equal(watch.Modified))

			// Verify the predicate allows this update (generation changed)
			updateEvent := event.UpdateEvent{
				ObjectOld: client.Object(oldObj),
				ObjectNew: watchEvent.Object.(client.Object),
			}
			Expect(pred.Update(updateEvent)).To(BeTrue(), "predicate should allow generation change")
		})

		It("should trigger watch events when generation changes on Patch via client", func() {
			// Create a GenerationChanged predicate
			genChangedPred := BasicPredicate("GenerationChanged")
			pred, err := genChangedPred.ToPredicate()
			Expect(err).NotTo(HaveOccurred())

			// Get the view cache client
			viewClient := viewCache.GetClient()

			// Create an object with generation 1
			oldObj := &unstructured.Unstructured{}
			oldObj.SetGroupVersionKind(gvk)
			oldObj.SetNamespace("test-ns")
			oldObj.SetName("test-obj")
			oldObj.SetGeneration(1)

			// Create via client
			err = viewClient.Create(ctx, oldObj)
			Expect(err).NotTo(HaveOccurred())

			// Set up a watch after creating the object
			list := &unstructured.UnstructuredList{}
			list.SetGroupVersionKind(schema.GroupVersionKind{
				Group:   gvk.Group,
				Version: gvk.Version,
				Kind:    gvk.Kind + "List",
			})

			watcher, err := viewCache.Watch(ctx, list)
			Expect(err).NotTo(HaveOccurred())
			defer watcher.Stop()

			// Consume the initial Add event
			_, ok := testutils.TryWatchEvent(watcher, 100*time.Millisecond)
			Expect(ok).To(BeTrue(), "should receive initial Add event")

			// Patch via client with generation changed
			patchObj := oldObj.DeepCopy()
			patch := client.MergeFrom(oldObj)
			patchObj.SetGeneration(2)

			err = viewClient.Patch(ctx, patchObj, patch)
			Expect(err).NotTo(HaveOccurred())

			// Wait for the update event (Patch generates an update event)
			watchEvent, ok := testutils.TryWatchEvent(watcher, 100*time.Millisecond)
			Expect(ok).To(BeTrue(), "should receive watch event for Patch")
			Expect(watchEvent.Type).To(Equal(watch.Modified))

			// Verify the predicate allows this update (generation changed)
			updateEvent := event.UpdateEvent{
				ObjectOld: client.Object(oldObj),
				ObjectNew: watchEvent.Object.(client.Object),
			}
			Expect(pred.Update(updateEvent)).To(BeTrue(), "predicate should allow generation change via Patch")
		})

		It("should demonstrate that generation is NOT automatically incremented by the framework", func() {
			// Create a GenerationChanged predicate
			genChangedPred := BasicPredicate("GenerationChanged")
			pred, err := genChangedPred.ToPredicate()
			Expect(err).NotTo(HaveOccurred())

			// Get the view cache client
			viewClient := viewCache.GetClient()

			// Create an object WITHOUT setting generation explicitly
			// In real Kubernetes, the API server would set generation=1 automatically
			oldObj := &unstructured.Unstructured{}
			oldObj.SetGroupVersionKind(gvk)
			oldObj.SetNamespace("test-ns")
			oldObj.SetName("test-obj")
			// NOTE: No SetGeneration() call here!

			// Create via client
			err = viewClient.Create(ctx, oldObj)
			Expect(err).NotTo(HaveOccurred())

			// Set up a watch after creating the object
			list := &unstructured.UnstructuredList{}
			list.SetGroupVersionKind(schema.GroupVersionKind{
				Group:   gvk.Group,
				Version: gvk.Version,
				Kind:    gvk.Kind + "List",
			})

			watcher, err := viewCache.Watch(ctx, list)
			Expect(err).NotTo(HaveOccurred())
			defer watcher.Stop()

			// Consume the initial Add event
			_, ok := testutils.TryWatchEvent(watcher, 100*time.Millisecond)
			Expect(ok).To(BeTrue(), "should receive initial Add event")

			// Update via client - modify spec without explicitly changing generation
			// In real Kubernetes, the API server would increment generation automatically
			updatedObj := oldObj.DeepCopy()
			updatedObj.SetLabels(map[string]string{"app": "updated"})
			// NOTE: No SetGeneration() call here either!

			err = viewClient.Update(ctx, updatedObj)
			Expect(err).NotTo(HaveOccurred())

			// Wait for the update event
			watchEvent, ok := testutils.TryWatchEvent(watcher, 100*time.Millisecond)
			Expect(ok).To(BeTrue(), "should receive watch event for Update")
			Expect(watchEvent.Type).To(Equal(watch.Modified))

			// Check what generation is on the updated object
			updatedObjFromEvent := watchEvent.Object.(*unstructured.Unstructured)
			actualGeneration := updatedObjFromEvent.GetGeneration()
			Expect(actualGeneration).To(Equal(int64(0)), "generation should still be 0 - NOT auto-incremented")

			// Verify the predicate BLOCKS this update (generation did not change - both are 0)
			updateEvent := event.UpdateEvent{
				ObjectOld: client.Object(oldObj),
				ObjectNew: watchEvent.Object.(client.Object),
			}
			Expect(pred.Update(updateEvent)).To(BeFalse(),
				"predicate should block update because generation was not manually incremented (0→0)")
		})

		It("should NOT trigger watch events when generation does NOT change on Patch", func() {
			// Create a GenerationChanged predicate
			genChangedPred := BasicPredicate("GenerationChanged")
			pred, err := genChangedPred.ToPredicate()
			Expect(err).NotTo(HaveOccurred())

			// Get the view cache client
			viewClient := viewCache.GetClient()

			// Create an object with generation 1
			oldObj := &unstructured.Unstructured{}
			oldObj.SetGroupVersionKind(gvk)
			oldObj.SetNamespace("test-ns")
			oldObj.SetName("test-obj")
			oldObj.SetGeneration(1)

			// Create via client
			err = viewClient.Create(ctx, oldObj)
			Expect(err).NotTo(HaveOccurred())

			// Set up a watch after creating the object
			list := &unstructured.UnstructuredList{}
			list.SetGroupVersionKind(schema.GroupVersionKind{
				Group:   gvk.Group,
				Version: gvk.Version,
				Kind:    gvk.Kind + "List",
			})

			watcher, err := viewCache.Watch(ctx, list)
			Expect(err).NotTo(HaveOccurred())
			defer watcher.Stop()

			// Consume the initial Add event
			_, ok := testutils.TryWatchEvent(watcher, 100*time.Millisecond)
			Expect(ok).To(BeTrue(), "should receive initial Add event")

			// Patch via client without changing generation (just add a label)
			patchObj := oldObj.DeepCopy()
			patch := client.MergeFrom(oldObj)
			patchObj.SetLabels(map[string]string{"new-label": "value"})
			// Generation remains 1

			err = viewClient.Patch(ctx, patchObj, patch)
			Expect(err).NotTo(HaveOccurred())

			// Wait for the update event
			watchEvent, ok := testutils.TryWatchEvent(watcher, 100*time.Millisecond)
			Expect(ok).To(BeTrue(), "should receive watch event for Patch")
			Expect(watchEvent.Type).To(Equal(watch.Modified))

			// Verify the predicate BLOCKS this update (generation unchanged)
			updateEvent := event.UpdateEvent{
				ObjectOld: client.Object(oldObj),
				ObjectNew: watchEvent.Object.(client.Object),
			}
			Expect(pred.Update(updateEvent)).To(BeFalse(), "predicate should block updates without generation change")
		})
	})
})

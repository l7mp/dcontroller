package cache

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"

	apiv1 "hsnlab/dcontroller-runtime/pkg/api/view/v1"
	"hsnlab/dcontroller-runtime/pkg/object"
)

const (
	timeout  = time.Second * 1
	interval = time.Millisecond * 50
)

var _ = Describe("ViewCache", func() {
	var (
		cache  *ViewCache
		ctx    context.Context
		cancel context.CancelFunc
	)

	BeforeEach(func() {
		cache = NewViewCache(Options{Logger: &logger})
		ctx, cancel = context.WithCancel(context.Background())
	})

	AfterEach(func() {
		cancel()
	})

	Describe("Registering views", func() {
		It("should allow a view to be registered", func() {
			err := cache.RegisterCacheForKind(apiv1.NewGVK("view"))
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("Get operation", func() {
		It("should retrieve an added object", func() {
			obj := object.NewViewObject("view").
				WithContent(map[string]any{"a": int64(1)}).
				WithName("ns", "test-1")

			err := cache.Add(obj)
			Expect(err).NotTo(HaveOccurred())

			retrieved := object.DeepCopy(obj)
			err = cache.Get(ctx, client.ObjectKeyFromObject(retrieved), retrieved)
			Expect(err).NotTo(HaveOccurred())
			Expect(object.DeepEqual(retrieved, obj)).To(BeTrue())
		})

		It("should return an error for non-existent object", func() {
			obj := object.NewViewObject("view").WithName("", "non-existent")

			err := cache.Get(ctx, client.ObjectKeyFromObject(obj), obj)
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("List operation", func() {
		It("should list all added objects", func() {
			objects := []object.Object{
				object.NewViewObject("view").WithContent(map[string]any{"a": int64(1)}).WithName("ns1", "test-1"),
				object.NewViewObject("view").WithContent(map[string]any{"b": int64(2)}).WithName("ns2", "test-2"),
				object.NewViewObject("view").WithContent(map[string]any{"c": int64(3)}).WithName("ns3", "test-3"),
			}

			for _, obj := range objects {
				err := cache.Add(obj)
				Expect(err).NotTo(HaveOccurred())
			}

			list := object.NewViewObjectList("view")
			err := cache.List(ctx, list)
			Expect(err).NotTo(HaveOccurred())
			Expect(list.Items).To(HaveLen(3))
			Expect(list.Items).To(ContainElement(objects[0]))
			Expect(list.Items).To(ContainElement(objects[1]))
			Expect(list.Items).To(ContainElement(objects[2]))
		})

		It("should return an empty list when cache is empty", func() {
			list := object.NewViewObjectList("view")
			err := cache.List(ctx, list)
			Expect(err).NotTo(HaveOccurred())
			Expect(list.Items).To(BeEmpty())
		})
	})

	Describe("Watch operation", func() {
		It("should notify of existing objects", func() {
			obj := object.NewViewObject("view").
				WithContent(map[string]any{"data": "watch-data"}).
				WithName("ns", "test-watch")
			cache.Add(obj)

			watcher, err := cache.Watch(ctx, object.NewViewObjectList("view"))
			Expect(err).NotTo(HaveOccurred())

			event, ok := tryWatch(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Added))
			Expect(object.DeepEqual(obj, event.Object.(object.Object))).To(BeTrue())
		})

		It("should notify of added objects", func() {
			watcher, err := cache.Watch(ctx, object.NewViewObjectList("view"))
			Expect(err).NotTo(HaveOccurred())

			obj := object.NewViewObject("view").
				WithContent(map[string]any{"data": "watch-data"}).
				WithName("ns", "test-watch")
			go func() {
				time.Sleep(25 * time.Millisecond)
				cache.Add(obj)
			}()

			event, ok := tryWatch(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Added))
			Expect(object.DeepEqual(obj, event.Object.(object.Object))).To(BeTrue())
		})

		It("should notify of updated objects", func() {
			watcher, err := cache.Watch(ctx, object.NewViewObjectList("view"))
			Expect(err).NotTo(HaveOccurred())

			obj := object.NewViewObject("view").
				WithContent(map[string]any{"data": "original data"}).
				WithName("ns", "test-update")
			cache.Add(obj)

			updatedObj := object.NewViewObject("view").
				WithContent(map[string]any{"data": "updated data"}).
				WithName("ns", "test-update")
			go func() {
				time.Sleep(25 * time.Millisecond)
				cache.Update(updatedObj)
			}()

			event, ok := tryWatch(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Added))
			Expect(object.DeepEqual(obj, event.Object.(object.Object))).To(BeTrue())

			event, ok = tryWatch(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Modified))
			Expect(object.DeepEqual(updatedObj, event.Object.(object.Object))).To(BeTrue())
		})

		It("should notify of deleted objects", func() {
			watcher, err := cache.Watch(ctx, object.NewViewObjectList("view"))
			Expect(err).NotTo(HaveOccurred())

			obj := object.NewViewObject("view").
				WithContent(map[string]any{"data": "original data"}).
				WithName("ns", "test-delete")
			cache.Add(obj)

			go func() {
				time.Sleep(25 * time.Millisecond)
				cache.Delete(obj)
			}()

			event, ok := tryWatch(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Added))
			Expect(object.DeepEqual(obj, event.Object.(object.Object))).To(BeTrue())

			event, ok = tryWatch(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Deleted))
			Expect(object.DeepEqual(obj, event.Object.(object.Object))).To(BeTrue())
		})
	})
})

func tryWatch(watcher watch.Interface, d time.Duration) (watch.Event, bool) {
	select {
	case event := <-watcher.ResultChan():
		return event, true
	case <-time.After(d):
		return watch.Event{}, false
	}
}

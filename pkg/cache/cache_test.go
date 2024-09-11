package cache

import (
	"context"
	"testing"
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

func TestCache(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Cache")
}

var _ = Describe("Cache", func() {
	var (
		cache *Cache
		ctx   context.Context
	)

	BeforeEach(func() {
		cache = New()
		ctx = context.Background()
	})

	Describe("Registering views", func() {
		It("should allow a view to be registered", func() {
			err := cache.RegisterGVK(apiv1.NewGVK("view"))
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("Get operation", func() {
		It("should allow a client to be created", func() {
			client := cache.NewClient()
			Expect(client).NotTo(BeNil())
		})

		It("should retrieve an added object", func() {
			obj := object.NewViewObject("view").
				WithContent(map[string]any{"a": int64(1)}).
				WithName("ns", "test-1")

			err := cache.RegisterGVK(apiv1.NewGVK("view"))
			Expect(err).NotTo(HaveOccurred())

			err = cache.Upsert(obj)
			Expect(err).NotTo(HaveOccurred())

			retrieved := object.DeepCopy(obj)
			err = cache.NewClient().Get(ctx, client.ObjectKeyFromObject(retrieved), retrieved)
			Expect(err).NotTo(HaveOccurred())
			Expect(object.DeepEqual(retrieved, obj)).To(BeTrue())
		})

		It("should return an error for non-existent object", func() {
			obj := object.NewViewObject("view").WithName("", "non-existent")

			err := cache.RegisterGVK(apiv1.NewGVK("view"))
			Expect(err).NotTo(HaveOccurred())

			err = cache.NewClient().Get(ctx, client.ObjectKeyFromObject(obj), obj)
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

			err := cache.RegisterGVK(apiv1.NewGVK("view"))
			Expect(err).NotTo(HaveOccurred())

			for _, obj := range objects {
				err := cache.Upsert(obj)
				Expect(err).NotTo(HaveOccurred())
			}

			list := object.NewViewObjectList("view")
			err = cache.NewClient().List(ctx, list)
			Expect(err).NotTo(HaveOccurred())
			Expect(list.Items).To(HaveLen(3))
			Expect(object.DeepEqual(&list.Items[0], objects[0])).NotTo(BeNil())
			Expect(object.DeepEqual(&list.Items[1], objects[1])).NotTo(BeNil())
			Expect(object.DeepEqual(&list.Items[2], objects[2])).NotTo(BeNil())
		})

		It("should return an empty list when cache is empty", func() {
			list := object.NewViewObjectList("view")

			err := cache.RegisterGVK(apiv1.NewGVK("view"))
			Expect(err).NotTo(HaveOccurred())

			err = cache.NewClient().List(ctx, list)
			Expect(err).NotTo(HaveOccurred())
			Expect(list.Items).To(BeEmpty())
		})
	})

	Describe("Watch operation", func() {
		It("should notify of existing objects", func() {
			err := cache.RegisterGVK(apiv1.NewGVK("view"))
			Expect(err).NotTo(HaveOccurred())

			obj := object.NewViewObject("view").
				WithContent(map[string]any{"data": "watch-data"}).
				WithName("ns", "test-watch")
			cache.Upsert(obj)

			watcher, err := cache.NewClient().Watch(ctx, object.NewViewObjectList("view"))
			Expect(err).NotTo(HaveOccurred())

			event, ok := tryWatch(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Added))
			Expect(object.DeepEqual(obj, event.Object.(object.Object))).To(BeTrue())
		})

		It("should notify of added objects", func() {
			err := cache.RegisterGVK(apiv1.NewGVK("view"))
			Expect(err).NotTo(HaveOccurred())

			watcher, err := cache.NewClient().Watch(ctx, object.NewViewObjectList("view"))
			Expect(err).NotTo(HaveOccurred())

			obj := object.NewViewObject("view").
				WithContent(map[string]any{"data": "watch-data"}).
				WithName("ns", "test-watch")
			go func() {
				time.Sleep(25 * time.Millisecond)
				cache.Upsert(obj)
			}()

			event, ok := tryWatch(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Added))
			Expect(object.DeepEqual(obj, event.Object.(object.Object))).To(BeTrue())
		})

		It("should notify of updated objects", func() {
			err := cache.RegisterGVK(apiv1.NewGVK("view"))
			Expect(err).NotTo(HaveOccurred())

			watcher, err := cache.NewClient().Watch(ctx, object.NewViewObjectList("view"))
			Expect(err).NotTo(HaveOccurred())

			obj := object.NewViewObject("view").
				WithContent(map[string]any{"data": "original data"}).
				WithName("ns", "test-update")
			cache.Upsert(obj)

			updatedObj := object.NewViewObject("view").
				WithContent(map[string]any{"data": "updated data"}).
				WithName("ns", "test-update")
			go func() {
				time.Sleep(25 * time.Millisecond)
				cache.Upsert(updatedObj)
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
			err := cache.RegisterGVK(apiv1.NewGVK("view"))
			Expect(err).NotTo(HaveOccurred())

			watcher, err := cache.NewClient().Watch(ctx, object.NewViewObjectList("view"))
			Expect(err).NotTo(HaveOccurred())

			obj := object.NewViewObject("view").
				WithContent(map[string]any{"data": "original data"}).
				WithName("ns", "test-delete")
			cache.Upsert(obj)

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

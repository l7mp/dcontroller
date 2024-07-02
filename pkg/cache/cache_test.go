package cache

import (
	"context"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"

	apiv1 "hsnlab/dcontroller-runtime/pkg/api/v1"
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
		It("should allow a view to be resgistered", func() {
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
			obj := object.New("view").WithName("ns", "test-1").WithContent(map[string]any{"a": int64(1)})

			err := cache.RegisterGVK(apiv1.NewGVK("view"))
			Expect(err).NotTo(HaveOccurred())

			err = cache.Upsert(obj)
			Expect(err).NotTo(HaveOccurred())

			retrieved := obj.DeepCopy()
			err = cache.NewClient().Get(ctx, client.ObjectKeyFromObject(retrieved), retrieved)
			Expect(err).NotTo(HaveOccurred())
			Expect(retrieved.DeepEqual(obj)).To(BeTrue())
		})

		It("should return an error for non-existent object", func() {
			obj := object.New("view").WithName("", "non-existent")

			err := cache.RegisterGVK(apiv1.NewGVK("view"))
			Expect(err).NotTo(HaveOccurred())

			err = cache.NewClient().Get(ctx, client.ObjectKeyFromObject(obj), obj)
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("List operation", func() {
		It("should list all added objects", func() {
			objects := []*object.Object{
				object.New("view").WithName("ns1", "test-1").WithContent(map[string]any{"a": int64(1)}),
				object.New("view").WithName("ns2", "test-2").WithContent(map[string]any{"b": int64(2)}),
				object.New("view").WithName("ns3", "test-3").WithContent(map[string]any{"c": int64(3)}),
			}

			err := cache.RegisterGVK(apiv1.NewGVK("view"))
			Expect(err).NotTo(HaveOccurred())

			for _, obj := range objects {
				err := cache.Upsert(obj)
				Expect(err).NotTo(HaveOccurred())
			}

			list := object.NewList("view")
			err = cache.NewClient().List(ctx, list)
			Expect(err).NotTo(HaveOccurred())
			Expect(list.Items).To(HaveLen(3))
			Expect(list.Items[0].DeepEqual(objects[0])).NotTo(BeNil())
			Expect(list.Items[1].DeepEqual(objects[1])).NotTo(BeNil())
			Expect(list.Items[2].DeepEqual(objects[2])).NotTo(BeNil())
		})

		It("should return an empty list when cache is empty", func() {
			list := object.NewList("view")

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

			obj := object.New("view").
				WithName("ns", "test-watch").
				WithContent(map[string]any{"data": "watch-data"})
			cache.Upsert(obj)

			watcher, err := cache.NewClient().Watch(ctx, object.NewList("view"))
			Expect(err).NotTo(HaveOccurred())

			event, ok := tryWatch(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Added))
			Expect(obj.DeepEqual(event.Object.(*object.Object))).To(BeTrue())
		})

		It("should notify of added objects", func() {
			err := cache.RegisterGVK(apiv1.NewGVK("view"))
			Expect(err).NotTo(HaveOccurred())

			watcher, err := cache.NewClient().Watch(ctx, object.NewList("view"))
			Expect(err).NotTo(HaveOccurred())

			obj := object.New("view").
				WithName("ns", "test-watch").
				WithContent(map[string]any{"data": "watch-data"})
			go func() {
				time.Sleep(25 * time.Millisecond)
				cache.Upsert(obj)
			}()

			event, ok := tryWatch(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Added))
			Expect(obj.DeepEqual(event.Object.(*object.Object))).To(BeTrue())
		})

		It("should notify of updated objects", func() {
			err := cache.RegisterGVK(apiv1.NewGVK("view"))
			Expect(err).NotTo(HaveOccurred())

			watcher, err := cache.NewClient().Watch(ctx, object.NewList("view"))
			Expect(err).NotTo(HaveOccurred())

			obj := object.New("view").WithName("ns", "test-update").
				WithContent(map[string]any{"data": "original data"})
			cache.Upsert(obj)

			updatedObj := object.New("view").WithName("ns", "test-update").
				WithContent(map[string]any{"data": "updated data"})
			go func() {
				time.Sleep(25 * time.Millisecond)
				cache.Upsert(updatedObj)
			}()

			event, ok := tryWatch(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Added))
			Expect(obj.DeepEqual(event.Object.(*object.Object))).To(BeTrue())

			event, ok = tryWatch(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Modified))
			Expect(updatedObj.DeepEqual(event.Object.(*object.Object))).To(BeTrue())
		})

		It("should notify of deleted objects", func() {
			err := cache.RegisterGVK(apiv1.NewGVK("view"))
			Expect(err).NotTo(HaveOccurred())

			watcher, err := cache.NewClient().Watch(ctx, object.NewList("view"))
			Expect(err).NotTo(HaveOccurred())

			obj := object.New("view").WithName("ns", "test-delete").
				WithContent(map[string]any{"data": "original data"})
			cache.Upsert(obj)

			go func() {
				time.Sleep(25 * time.Millisecond)
				cache.Delete(obj)
			}()

			event, ok := tryWatch(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Added))
			Expect(obj.DeepEqual(event.Object.(*object.Object))).To(BeTrue())

			event, ok = tryWatch(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Deleted))
			Expect(obj.DeepEqual(event.Object.(*object.Object))).To(BeTrue())
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

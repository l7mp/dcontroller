package cache

import (
	"context"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/watch"

	"hsnlab/dcontroller-runtime/pkg/object"
)

var _ = Describe("Cache", func() {
	var (
		cache *Cache
		ctx   context.Context
	)

	BeforeEach(func() {
		cache = NewCache()
		ctx = context.Background()
	})

	Describe("Get operation", func() {
		It("should retrieve an added object", func() {
			obj := object.New("view", "ns", "test-1", map[string]any{"a": 1})
			err := cache.Add(obj)
			Expect(err).NotTo(HaveOccurred())

			retrieved, err := cache.NewClient().Get("ns/test-1")
			Expect(err).NotTo(HaveOccurred())
			Expect(retrieved).To(Equal(obj))
		})

		It("should return an error for non-existent object", func() {
			_, err := cache.NewClient().Get("non-existent")
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("List operation", func() {
		It("should list all added objects", func() {
			objects := []*object.Object{
				object.New("view", "ns1", "test-1", map[string]any{"a": 1}),
				object.New("view", "ns2", "test-2", map[string]any{"b": 2}),
				object.New("view", "ns3", "test-3", map[string]any{"c": 3}),
			}

			for _, obj := range objects {
				err := cache.Add(obj)
				Expect(err).NotTo(HaveOccurred())
			}

			list, err := cache.NewClient().List()
			Expect(err).NotTo(HaveOccurred())
			Expect(list).To(HaveLen(3))
			Expect(list).To(ConsistOf(objects))
		})

		It("should return an empty list when cache is empty", func() {
			list, err := cache.NewClient().List()
			Expect(err).NotTo(HaveOccurred())
			Expect(list).To(BeEmpty())
		})
	})

	Describe("Watch operation", func() {
		It("should notify of added objects", func() {
			watcher, err := cache.NewClient().Watch(ctx)
			Expect(err).NotTo(HaveOccurred())

			go func() {
				time.Sleep(20 * time.Millisecond)
				cache.Add(object.New("view", "ns", "test-watch", map[string]any{"data": "watch-data"}))
			}()

			event := <-watcher.ResultChan()
			Expect(event.Type).To(Equal(watch.Added))
			Expect(event.Object).To(Equal(object.New("view", "ns", "test-watch", map[string]any{"data": "watch-data"})))
		})

		It("should notify of updated objects", func() {
			obj := object.New("view", "ns", "test-update", map[string]any{"data": "original data"})
			cache.Add(obj)

			watcher, err := cache.NewClient().Watch(ctx)
			Expect(err).NotTo(HaveOccurred())

			go func() {
				time.Sleep(20 * time.Millisecond)
				updatedObj := object.New("view", "ns", "test-update", map[string]any{"data": "updated data"})
				cache.Update(updatedObj)
			}()

			event := <-watcher.ResultChan()
			Expect(event.Type).To(Equal(watch.Modified))
			Expect(event.Object).To(Equal(object.New("view", "ns", "test-update", map[string]any{"data": "updated data"})))
		})
	})
})

func TestCache(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "CustomCache")
}

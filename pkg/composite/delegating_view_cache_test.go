package composite

import (
	"context"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/watch"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	viewv1a1 "github.com/l7mp/dcontroller/pkg/api/view/v1alpha1"
	"github.com/l7mp/dcontroller/pkg/object"
)

var _ = Describe("DelegatingViewCache", func() {
	var (
		sharedStorage *ViewCache
		ctx           context.Context
		cancel        context.CancelFunc
	)

	BeforeEach(func() {
		sharedStorage = NewViewCache(CacheOptions{Logger: logger})
		ctx, cancel = context.WithCancel(context.Background())
	})

	AfterEach(func() {
		cancel()
	})

	Describe("Basic operations", func() {
		It("should create a delegating cache with shared storage", func() {
			delegatingCache := NewDelegatingViewCache(sharedStorage, CacheOptions{Logger: logger})
			Expect(delegatingCache).NotTo(BeNil())
			Expect(delegatingCache.storage).To(Equal(sharedStorage))
		})

		It("should delegate Get operations to shared storage", func() {
			delegatingCache := NewDelegatingViewCache(sharedStorage, CacheOptions{Logger: logger})

			// Add object via shared storage directly
			obj := object.NewViewObject("test", "view")
			object.SetContent(obj, map[string]any{"a": int64(1)})
			object.SetName(obj, "ns", "test-1")
			err := sharedStorage.Add(obj)
			Expect(err).NotTo(HaveOccurred())

			// Retrieve via delegating cache
			object.WithUID(obj)
			retrieved := object.DeepCopy(obj)
			err = delegatingCache.Get(ctx, client.ObjectKeyFromObject(retrieved), retrieved)
			Expect(err).NotTo(HaveOccurred())
			Expect(object.DeepEqual(retrieved, obj)).To(BeTrue())
		})

		It("should delegate List operations to shared storage", func() {
			delegatingCache := NewDelegatingViewCache(sharedStorage, CacheOptions{Logger: logger})

			// Add objects via shared storage
			objects := []object.Object{
				object.NewViewObject("test", "view"),
				object.NewViewObject("test", "view"),
			}
			object.SetName(objects[0], "ns1", "test-1")
			object.SetName(objects[1], "ns2", "test-2")
			object.SetContent(objects[0], map[string]any{"a": int64(1)})
			object.SetContent(objects[1], map[string]any{"b": int64(2)})

			for _, obj := range objects {
				err := sharedStorage.Add(obj)
				Expect(err).NotTo(HaveOccurred())
			}

			// List via delegating cache
			list := NewViewObjectList("test", "view")
			err := delegatingCache.List(ctx, list)
			Expect(err).NotTo(HaveOccurred())
			Expect(list.Items).To(HaveLen(2))
		})

		It("should delegate Add operations to shared storage", func() {
			delegatingCache := NewDelegatingViewCache(sharedStorage, CacheOptions{Logger: logger})

			// Add via delegating cache
			obj := object.NewViewObject("test", "view")
			object.SetContent(obj, map[string]any{"a": int64(1)})
			object.SetName(obj, "ns", "test-add")
			err := delegatingCache.Add(obj)
			Expect(err).NotTo(HaveOccurred())

			// Verify in shared storage
			object.WithUID(obj)
			retrieved := object.DeepCopy(obj)
			err = sharedStorage.Get(ctx, client.ObjectKeyFromObject(retrieved), retrieved)
			Expect(err).NotTo(HaveOccurred())
			Expect(object.DeepEqual(retrieved, obj)).To(BeTrue())
		})
	})

	Describe("Cross-operator sharing", func() {
		It("should allow multiple delegating caches to share the same storage", func() {
			cache1 := NewDelegatingViewCache(sharedStorage, CacheOptions{Logger: logger})
			cache2 := NewDelegatingViewCache(sharedStorage, CacheOptions{Logger: logger})

			// Add via cache1
			obj := object.NewViewObject("test", "view")
			object.SetContent(obj, map[string]any{"data": "shared"})
			object.SetName(obj, "ns", "shared-obj")
			err := cache1.Add(obj)
			Expect(err).NotTo(HaveOccurred())

			// Read via cache2
			object.WithUID(obj)
			retrieved := object.DeepCopy(obj)
			err = cache2.Get(ctx, client.ObjectKeyFromObject(retrieved), retrieved)
			Expect(err).NotTo(HaveOccurred())
			Expect(object.DeepEqual(retrieved, obj)).To(BeTrue())
		})

		It("should allow multiple caches to list the same shared objects", func() {
			cache1 := NewDelegatingViewCache(sharedStorage, CacheOptions{Logger: logger})
			cache2 := NewDelegatingViewCache(sharedStorage, CacheOptions{Logger: logger})

			// Add objects via cache1
			obj1 := object.NewViewObject("test", "view")
			object.SetName(obj1, "ns", "obj1")
			object.SetContent(obj1, map[string]any{"id": int64(1)})
			err := cache1.Add(obj1)
			Expect(err).NotTo(HaveOccurred())

			// Add objects via cache2
			obj2 := object.NewViewObject("test", "view")
			object.SetName(obj2, "ns", "obj2")
			object.SetContent(obj2, map[string]any{"id": int64(2)})
			err = cache2.Add(obj2)
			Expect(err).NotTo(HaveOccurred())

			// Both caches should see all objects
			list1 := NewViewObjectList("test", "view")
			err = cache1.List(ctx, list1)
			Expect(err).NotTo(HaveOccurred())
			Expect(list1.Items).To(HaveLen(2))

			list2 := NewViewObjectList("test", "view")
			err = cache2.List(ctx, list2)
			Expect(err).NotTo(HaveOccurred())
			Expect(list2.Items).To(HaveLen(2))
		})
	})

	Describe("Informer management", func() {
		It("should create local informers that wrap shared indexers", func() {
			delegatingCache := NewDelegatingViewCache(sharedStorage, CacheOptions{Logger: logger})

			gvk := viewv1a1.GroupVersionKind("test", "view")
			informer, err := delegatingCache.GetInformerForKind(ctx, gvk)
			Expect(err).NotTo(HaveOccurred())
			Expect(informer).NotTo(BeNil())

			// Verify it's a ViewCacheInformer
			viewInformer, ok := informer.(*ViewCacheInformer)
			Expect(ok).To(BeTrue())
			Expect(viewInformer).NotTo(BeNil())
		})

		It("should receive initial object list when creating informer on pre-populated storage", func() {
			// This test checks that when a delegating cache creates an informer on a shared
			// storage that already contains objects, the informer's event handlers receive
			// the initial object list.

			gvk := viewv1a1.GroupVersionKind("test", "view")

			// Pre-populate shared storage with objects BEFORE creating delegating cache
			objects := []object.Object{
				object.NewViewObject("test", "view"),
				object.NewViewObject("test", "view"),
				object.NewViewObject("test", "view"),
			}
			object.SetName(objects[0], "ns1", "obj-1")
			object.SetName(objects[1], "ns1", "obj-2")
			object.SetName(objects[2], "ns2", "obj-3")
			object.SetContent(objects[0], map[string]any{"id": int64(1)})
			object.SetContent(objects[1], map[string]any{"id": int64(2)})
			object.SetContent(objects[2], map[string]any{"id": int64(3)})

			for _, obj := range objects {
				err := sharedStorage.Add(obj)
				Expect(err).NotTo(HaveOccurred())
			}

			// NOW create the delegating cache
			delegatingCache := NewDelegatingViewCache(sharedStorage, CacheOptions{Logger: logger})

			// Create an informer for the GVK
			informer, err := delegatingCache.GetInformerForKind(ctx, gvk)
			Expect(err).NotTo(HaveOccurred())

			// Track events received by the handler
			var receivedObjects []object.Object
			var mu sync.Mutex

			handler := toolscache.ResourceEventHandlerFuncs{
				AddFunc: func(obj any) {
					mu.Lock()
					defer mu.Unlock()
					viewObj, ok := obj.(object.Object)
					Expect(ok).To(BeTrue())
					receivedObjects = append(receivedObjects, viewObj)
				},
			}

			// Register the handler - this should trigger initial list
			_, err = informer.AddEventHandler(handler)
			Expect(err).NotTo(HaveOccurred())

			// The handler should have received all 3 initial objects
			mu.Lock()
			numReceived := len(receivedObjects)
			mu.Unlock()

			Expect(numReceived).To(Equal(3), "Handler should receive all pre-existing objects as initial list")

			// Verify we got the correct objects
			mu.Lock()
			receivedNames := make(map[string]bool)
			for _, obj := range receivedObjects {
				key := client.ObjectKeyFromObject(obj).String()
				receivedNames[key] = true
			}
			mu.Unlock()

			Expect(receivedNames).To(HaveKey("ns1/obj-1"))
			Expect(receivedNames).To(HaveKey("ns1/obj-2"))
			Expect(receivedNames).To(HaveKey("ns2/obj-3"))
		})

		It("should receive initial list across multiple delegating caches on shared storage", func() {
			// This test simulates the real-world scenario:
			// 1. Operator A starts and adds objects to shared storage
			// 2. Operator B starts later and creates a delegating cache
			// 3. Operator B should receive all objects from shared storage as initial list

			gvk := viewv1a1.GroupVersionKind("test", "view")

			// Simulate Operator A: create delegating cache and add objects
			operatorA := NewDelegatingViewCache(sharedStorage, CacheOptions{Logger: logger})

			objects := []object.Object{
				object.NewViewObject("test", "view"),
				object.NewViewObject("test", "view"),
				object.NewViewObject("test", "view"),
			}
			object.SetName(objects[0], "default", "a-1")
			object.SetName(objects[1], "default", "a-2")
			object.SetName(objects[2], "kube-system", "a-3")
			object.SetContent(objects[0], map[string]any{"operator": "A", "id": int64(1)})
			object.SetContent(objects[1], map[string]any{"operator": "A", "id": int64(2)})
			object.SetContent(objects[2], map[string]any{"operator": "A", "id": int64(3)})

			for _, obj := range objects {
				err := operatorA.Add(obj)
				Expect(err).NotTo(HaveOccurred())
			}

			// Verify objects are in shared storage
			list := NewViewObjectList("test", "view")
			err := sharedStorage.List(ctx, list)
			Expect(err).NotTo(HaveOccurred())
			Expect(list.Items).To(HaveLen(3))

			// Simulate Operator B starting LATER
			operatorB := NewDelegatingViewCache(sharedStorage, CacheOptions{Logger: logger})

			// Operator B creates an informer and registers a handler
			informerB, err := operatorB.GetInformerForKind(ctx, gvk)
			Expect(err).NotTo(HaveOccurred())

			// Track what Operator B receives
			var receivedByB []object.Object
			var muB sync.Mutex

			handlerB := toolscache.ResourceEventHandlerFuncs{
				AddFunc: func(obj any) {
					muB.Lock()
					defer muB.Unlock()
					viewObj, ok := obj.(object.Object)
					Expect(ok).To(BeTrue())
					receivedByB = append(receivedByB, viewObj)
				},
			}

			// Register handler - Operator B should receive ALL objects from shared storage
			_, err = informerB.AddEventHandler(handlerB)
			Expect(err).NotTo(HaveOccurred())

			// Operator B should have received all 3 objects that Operator A added
			muB.Lock()
			numReceivedByB := len(receivedByB)
			muB.Unlock()

			Expect(numReceivedByB).To(Equal(3),
				"Operator B should receive all pre-existing objects from shared storage as initial list")

			// Verify Operator B received the correct objects
			muB.Lock()
			receivedNamesB := make(map[string]bool)
			for _, obj := range receivedByB {
				key := client.ObjectKeyFromObject(obj).String()
				receivedNamesB[key] = true
			}
			muB.Unlock()

			Expect(receivedNamesB).To(HaveKey("default/a-1"))
			Expect(receivedNamesB).To(HaveKey("default/a-2"))
			Expect(receivedNamesB).To(HaveKey("kube-system/a-3"))
		})

		It("should create separate informers for each delegating cache", func() {
			cache1 := NewDelegatingViewCache(sharedStorage, CacheOptions{Logger: logger})
			cache2 := NewDelegatingViewCache(sharedStorage, CacheOptions{Logger: logger})

			gvk := viewv1a1.GroupVersionKind("test", "view")
			informer1, err := cache1.GetInformerForKind(ctx, gvk)
			Expect(err).NotTo(HaveOccurred())

			informer2, err := cache2.GetInformerForKind(ctx, gvk)
			Expect(err).NotTo(HaveOccurred())

			// They should be different informer instances
			Expect(informer1).NotTo(BeIdenticalTo(informer2))

			// But they should wrap the same indexer
			viewInformer1 := informer1.(*ViewCacheInformer)
			viewInformer2 := informer2.(*ViewCacheInformer)
			Expect(viewInformer1.GetIndexer()).To(BeIdenticalTo(viewInformer2.GetIndexer()))
		})

		It("should receive initial events when handlers are added", func() {
			cache1 := NewDelegatingViewCache(sharedStorage, CacheOptions{Logger: logger})
			cache2 := NewDelegatingViewCache(sharedStorage, CacheOptions{Logger: logger})

			gvk := viewv1a1.GroupVersionKind("test", "view")

			// Add object to shared storage FIRST
			obj := object.NewViewObject("test", "view")
			object.SetName(obj, "ns", "test-event")
			object.SetContent(obj, map[string]any{"data": "test"})
			err := sharedStorage.Add(obj)
			Expect(err).NotTo(HaveOccurred())

			// Setup informers AFTER object is added
			informer1, err := cache1.GetInformerForKind(ctx, gvk)
			Expect(err).NotTo(HaveOccurred())

			informer2, err := cache2.GetInformerForKind(ctx, gvk)
			Expect(err).NotTo(HaveOccurred())

			// Track events
			var events1, events2 []string
			var mu1, mu2 sync.Mutex

			handler1 := toolscache.ResourceEventHandlerFuncs{
				AddFunc: func(obj any) {
					mu1.Lock()
					defer mu1.Unlock()
					events1 = append(events1, "add")
				},
			}

			handler2 := toolscache.ResourceEventHandlerFuncs{
				AddFunc: func(obj any) {
					mu2.Lock()
					defer mu2.Unlock()
					events2 = append(events2, "add")
				},
			}

			// When handlers are added, they receive initial list from shared indexer
			_, err = informer1.AddEventHandler(handler1)
			Expect(err).NotTo(HaveOccurred())

			_, err = informer2.AddEventHandler(handler2)
			Expect(err).NotTo(HaveOccurred())

			// Both handlers should have received the initial object
			Eventually(func() int {
				mu1.Lock()
				defer mu1.Unlock()
				return len(events1)
			}, timeout, interval).Should(BeNumerically(">=", 1))

			Eventually(func() int {
				mu2.Lock()
				defer mu2.Unlock()
				return len(events2)
			}, timeout, interval).Should(BeNumerically(">=", 1))
		})
	})

	Describe("Independent lifecycle", func() {
		It("should start and stop local informers independently", func() {
			cache1 := NewDelegatingViewCache(sharedStorage, CacheOptions{Logger: logger})
			cache2 := NewDelegatingViewCache(sharedStorage, CacheOptions{Logger: logger})

			gvk := viewv1a1.GroupVersionKind("test", "view")

			// Create informers
			informer1, err := cache1.GetInformerForKind(ctx, gvk)
			Expect(err).NotTo(HaveOccurred())

			informer2, err := cache2.GetInformerForKind(ctx, gvk)
			Expect(err).NotTo(HaveOccurred())

			// Start cache1
			ctx1, cancel1 := context.WithCancel(ctx)
			go cache1.Start(ctx1)

			// Start cache2
			ctx2, cancel2 := context.WithCancel(ctx)
			go cache2.Start(ctx2)

			// Both informers should be running
			time.Sleep(50 * time.Millisecond)
			Expect(informer1.IsStopped()).To(BeFalse())
			Expect(informer2.IsStopped()).To(BeFalse())

			// Stop cache1
			cancel1()
			time.Sleep(50 * time.Millisecond)

			// Informer1 should be stopped, but informer2 should still be running
			Expect(informer1.IsStopped()).To(BeTrue())
			Expect(informer2.IsStopped()).To(BeFalse())

			// Shared storage should still work
			obj := object.NewViewObject("test", "view")
			object.SetName(obj, "ns", "test-after-stop")
			object.SetContent(obj, map[string]any{"data": "still works"})
			err = sharedStorage.Add(obj)
			Expect(err).NotTo(HaveOccurred())

			// Cache2 should still be able to read (shared storage still works)
			object.WithUID(obj)
			retrieved := object.DeepCopy(obj)
			err = cache2.Get(ctx, client.ObjectKeyFromObject(retrieved), retrieved)
			Expect(err).NotTo(HaveOccurred())
			Expect(object.DeepEqual(retrieved, obj)).To(BeTrue())

			cancel2()
		})

		It("should allow restarting a stopped delegating cache", func() {
			delegatingCache := NewDelegatingViewCache(sharedStorage, CacheOptions{Logger: logger})

			gvk := viewv1a1.GroupVersionKind("test", "view")
			informer, err := delegatingCache.GetInformerForKind(ctx, gvk)
			Expect(err).NotTo(HaveOccurred())

			// Start and stop
			ctx1, cancel1 := context.WithCancel(ctx)
			go delegatingCache.Start(ctx1)
			time.Sleep(50 * time.Millisecond)
			cancel1()
			time.Sleep(50 * time.Millisecond)

			Expect(informer.IsStopped()).To(BeTrue())

			// Create a new informer (old one is stopped)
			informer2, err := delegatingCache.GetInformerForKind(ctx, gvk)
			Expect(err).NotTo(HaveOccurred())

			// This should return the same stopped informer
			// (because we don't auto-recreate stopped informers)
			Expect(informer2).To(Equal(informer))
		})
	})

	Describe("WaitForCacheSync", func() {
		It("should return true immediately for delegating cache", func() {
			delegatingCache := NewDelegatingViewCache(sharedStorage, CacheOptions{Logger: logger})

			// Create an informer
			gvk := viewv1a1.GroupVersionKind("test", "view")
			_, err := delegatingCache.GetInformerForKind(ctx, gvk)
			Expect(err).NotTo(HaveOccurred())

			// WaitForCacheSync should return true immediately
			// because local informers wrap already-populated indexers
			synced := delegatingCache.WaitForCacheSync(ctx)
			Expect(synced).To(BeTrue())
		})
	})

	Describe("Update and Delete operations", func() {
		It("should delegate Update to shared storage", func() {
			delegatingCache := NewDelegatingViewCache(sharedStorage, CacheOptions{Logger: logger})

			// Add initial object
			obj := object.NewViewObject("test", "view")
			object.SetName(obj, "ns", "test-update")
			object.SetContent(obj, map[string]any{"version": int64(1)})
			err := delegatingCache.Add(obj)
			Expect(err).NotTo(HaveOccurred())

			// Update via delegating cache
			object.WithUID(obj)
			updatedObj := object.DeepCopy(obj)
			object.SetContent(updatedObj, map[string]any{"version": int64(2)})
			err = delegatingCache.Update(obj, updatedObj)
			Expect(err).NotTo(HaveOccurred())

			// Verify in shared storage - get fresh copy
			retrieved := object.NewViewObject("test", "view")
			object.SetName(retrieved, "ns", "test-update")
			err = sharedStorage.Get(ctx, client.ObjectKeyFromObject(retrieved), retrieved)
			Expect(err).NotTo(HaveOccurred())

			// Check the content was updated
			Expect(retrieved.Object["version"]).To(Equal(int64(2)))
		})

		It("should delegate Delete to shared storage", func() {
			delegatingCache := NewDelegatingViewCache(sharedStorage, CacheOptions{Logger: logger})

			// Add object
			obj := object.NewViewObject("test", "view")
			object.SetName(obj, "ns", "test-delete")
			object.SetContent(obj, map[string]any{"data": "to be deleted"})
			err := delegatingCache.Add(obj)
			Expect(err).NotTo(HaveOccurred())

			// Delete via delegating cache
			err = delegatingCache.Delete(obj)
			Expect(err).NotTo(HaveOccurred())

			// Verify deleted from shared storage
			retrieved := object.NewViewObject("test", "view")
			object.SetName(retrieved, "ns", "test-delete")
			err = sharedStorage.Get(ctx, client.ObjectKeyFromObject(retrieved), retrieved)
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("Cross-operator data visibility", func() {
		It("should allow all delegating caches to see shared data immediately", func() {
			cache1 := NewDelegatingViewCache(sharedStorage, CacheOptions{Logger: logger})
			cache2 := NewDelegatingViewCache(sharedStorage, CacheOptions{Logger: logger})

			// Write from cache1
			obj := object.NewViewObject("test", "view")
			object.SetName(obj, "ns", "cross-op-add")
			object.SetContent(obj, map[string]any{"data": "cross-operator"})
			err := cache1.Add(obj)
			Expect(err).NotTo(HaveOccurred())

			// Read from cache2 - should see it immediately (same storage)
			object.WithUID(obj)
			retrieved := object.DeepCopy(obj)
			err = cache2.Get(ctx, client.ObjectKeyFromObject(retrieved), retrieved)
			Expect(err).NotTo(HaveOccurred())
			Expect(object.DeepEqual(retrieved, obj)).To(BeTrue())
		})

		It("should allow all delegating caches to see updates immediately", func() {
			cache1 := NewDelegatingViewCache(sharedStorage, CacheOptions{Logger: logger})
			cache2 := NewDelegatingViewCache(sharedStorage, CacheOptions{Logger: logger})

			// Add initial object via cache1
			obj := object.NewViewObject("test", "view")
			object.SetName(obj, "ns", "cross-op-update")
			object.SetContent(obj, map[string]any{"version": int64(1)})
			err := cache1.Add(obj)
			Expect(err).NotTo(HaveOccurred())

			// Update from cache2
			object.WithUID(obj)
			updatedObj := object.DeepCopy(obj)
			object.SetContent(updatedObj, map[string]any{"version": int64(2)})
			err = cache2.Update(obj, updatedObj)
			Expect(err).NotTo(HaveOccurred())

			// Read from cache1 - should see the update
			retrieved := object.NewViewObject("test", "view")
			object.SetName(retrieved, "ns", "cross-op-update")
			err = cache1.Get(ctx, client.ObjectKeyFromObject(retrieved), retrieved)
			Expect(err).NotTo(HaveOccurred())

			// Verify the update is visible
			Expect(retrieved.Object["version"]).To(Equal(int64(2)))
		})
	})

	Context("CompositeCache integration", func() {
		It("should use DelegatingViewCache directly when wrapped in CompositeCache", func() {
			// Create delegating cache
			delegatingCache := NewDelegatingViewCache(sharedStorage, CacheOptions{Logger: logger})

			// Wrap it in a CompositeCache (simulating what CacheInjector does)
			compositeCache, err := NewCompositeCache(nil, CacheOptions{
				ViewCache: delegatingCache,
				Logger:    logger,
			})
			Expect(err).NotTo(HaveOccurred())

			// The CompositeCache should use the DelegatingViewCache directly,
			// NOT extract its storage or create a new ViewCache
			viewCache := compositeCache.GetViewCache()
			Expect(viewCache).NotTo(BeNil())
			Expect(viewCache).To(BeIdenticalTo(delegatingCache))
		})

		It("should enable API server and operator to share view storage", func() {
			// Simulate API server setup
			apiCache, err := NewCompositeCache(nil, CacheOptions{Logger: logger})
			Expect(err).NotTo(HaveOccurred())
			apiViewCache := apiCache.GetViewCache()

			// Simulate operator setup with CacheInjector pattern
			// The delegating cache wraps the API server's view cache (which is a *ViewCache)
			apiViewStorage, ok := apiViewCache.(*ViewCache)
			Expect(ok).To(BeTrue(), "API server view cache should be *ViewCache")

			delegatingCache := NewDelegatingViewCache(apiViewStorage, CacheOptions{Logger: logger})
			operatorCache, err := NewCompositeCache(nil, CacheOptions{
				ViewCache: delegatingCache,
				Logger:    logger,
			})
			Expect(err).NotTo(HaveOccurred())

			// Operator's CompositeCache should use the DelegatingViewCache
			operatorViewCache := operatorCache.GetViewCache()
			Expect(operatorViewCache).To(BeIdenticalTo(delegatingCache))

			// Write via operator, read via API server
			obj := object.NewViewObject("test", "view")
			object.SetName(obj, "ns", "api-server-test")
			object.SetContent(obj, map[string]any{"source": "operator"})

			// Operator writes (via delegating cache client)
			opClient := delegatingCache.GetClient()
			err = opClient.Create(ctx, obj)
			Expect(err).NotTo(HaveOccurred())

			// API server reads (directly from its cache)
			object.WithUID(obj)
			retrieved := object.DeepCopy(obj)
			err = apiCache.Get(ctx, client.ObjectKeyFromObject(retrieved), retrieved)
			Expect(err).NotTo(HaveOccurred())

			// Should see the object written by operator (because delegating cache
			// delegates storage operations to the shared storage)
			Expect(retrieved.Object["source"]).To(Equal("operator"))
		})
	})

	Describe("Watch operation", func() {
		It("should notify of existing objects", func() {
			delegatingCache := NewDelegatingViewCache(sharedStorage, CacheOptions{Logger: logger})
			Expect(delegatingCache).NotTo(BeNil())
			Expect(delegatingCache.storage).To(Equal(sharedStorage))

			obj := object.NewViewObject("test", "view")
			object.SetContent(obj, map[string]any{"data": "watch-data"})
			object.SetName(obj, "ns", "test-watch")
			delegatingCache.Add(obj)
			object.WithUID(obj)

			watcher, err := delegatingCache.Watch(ctx, NewViewObjectList("test", "view"))
			Expect(err).NotTo(HaveOccurred())

			event, ok := tryWatch(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Added))
			Expect(object.DeepEqual(obj, event.Object.(object.Object))).To(BeTrue())
		})

		It("should notify of added objects", func() {
			delegatingCache := NewDelegatingViewCache(sharedStorage, CacheOptions{Logger: logger})
			Expect(delegatingCache).NotTo(BeNil())
			Expect(delegatingCache.storage).To(Equal(sharedStorage))

			watcher, err := delegatingCache.Watch(ctx, NewViewObjectList("test", "view"))
			Expect(err).NotTo(HaveOccurred())

			obj := object.NewViewObject("test", "view")
			object.SetContent(obj, map[string]any{"data": "watch-data"})
			object.SetName(obj, "ns", "test-watch")
			go func() {
				time.Sleep(25 * time.Millisecond)
				delegatingCache.Add(obj)
				object.WithUID(obj)
			}()

			event, ok := tryWatch(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Added))
			// Expect(object.DeepEqual(obj, event.Object.(object.Object))).To(BeTrue())
			Expect(obj).To(Equal(event.Object.(object.Object)))
		})

		It("should notify of objects added to the underlying storageCache", func() {
			delegatingCache := NewDelegatingViewCache(sharedStorage, CacheOptions{Logger: logger})
			Expect(delegatingCache).NotTo(BeNil())
			Expect(delegatingCache.storage).To(Equal(sharedStorage))

			watcher, err := delegatingCache.Watch(ctx, NewViewObjectList("test", "view"))
			Expect(err).NotTo(HaveOccurred())

			obj := object.NewViewObject("test", "view")
			object.SetContent(obj, map[string]any{"data": "watch-data"})
			object.SetName(obj, "ns", "test-watch")
			go func() {
				time.Sleep(25 * time.Millisecond)
				sharedStorage.Add(obj)
				object.WithUID(obj)
			}()

			event, ok := tryWatch(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Added))
			// Expect(object.DeepEqual(obj, event.Object.(object.Object))).To(BeTrue())
			Expect(obj).To(Equal(event.Object.(object.Object)))
		})

		It("should notify of updated objects", func() {
			delegatingCache := NewDelegatingViewCache(sharedStorage, CacheOptions{Logger: logger})
			Expect(delegatingCache).NotTo(BeNil())
			Expect(delegatingCache.storage).To(Equal(sharedStorage))

			watcher, err := delegatingCache.Watch(ctx, NewViewObjectList("test", "view"))
			Expect(err).NotTo(HaveOccurred())

			obj := object.NewViewObject("test", "view")
			object.SetContent(obj, map[string]any{"data": "original data"})
			object.SetName(obj, "ns", "test-update")
			delegatingCache.Add(obj)
			object.WithUID(obj)

			updatedObj := object.NewViewObject("test", "view")
			object.SetContent(updatedObj, map[string]any{"data": "updated data"})
			object.SetName(updatedObj, "ns", "test-update")
			go func() {
				time.Sleep(25 * time.Millisecond)
				delegatingCache.Update(obj, updatedObj)
				object.WithUID(updatedObj)
			}()

			event, ok := tryWatch(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Added))
			Expect(obj).To(Equal(event.Object.(object.Object)))
			Expect(object.DeepEqual(obj, event.Object.(object.Object))).To(BeTrue())

			event, ok = tryWatch(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Modified))
			Expect(updatedObj).To(Equal(event.Object.(object.Object)))
			Expect(object.DeepEqual(updatedObj, event.Object.(object.Object))).To(BeTrue())
		})

		It("should notify of objects updated in the underlying storage", func() {
			delegatingCache := NewDelegatingViewCache(sharedStorage, CacheOptions{Logger: logger})
			Expect(delegatingCache).NotTo(BeNil())
			Expect(delegatingCache.storage).To(Equal(sharedStorage))

			watcher, err := delegatingCache.Watch(ctx, NewViewObjectList("test", "view"))
			Expect(err).NotTo(HaveOccurred())

			obj := object.NewViewObject("test", "view")
			object.SetContent(obj, map[string]any{"data": "original data"})
			object.SetName(obj, "ns", "test-update")
			delegatingCache.Add(obj)
			object.WithUID(obj)

			updatedObj := object.NewViewObject("test", "view")
			object.SetContent(updatedObj, map[string]any{"data": "updated data"})
			object.SetName(updatedObj, "ns", "test-update")
			go func() {
				time.Sleep(25 * time.Millisecond)
				sharedStorage.Update(obj, updatedObj)
				object.WithUID(updatedObj)
			}()

			event, ok := tryWatch(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Added))
			Expect(obj).To(Equal(event.Object.(object.Object)))
			Expect(object.DeepEqual(obj, event.Object.(object.Object))).To(BeTrue())

			event, ok = tryWatch(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Modified))
			Expect(updatedObj).To(Equal(event.Object.(object.Object)))
			Expect(object.DeepEqual(updatedObj, event.Object.(object.Object))).To(BeTrue())
		})

		It("should notify of deleted objects", func() {
			delegatingCache := NewDelegatingViewCache(sharedStorage, CacheOptions{Logger: logger})
			Expect(delegatingCache).NotTo(BeNil())
			Expect(delegatingCache.storage).To(Equal(sharedStorage))

			watcher, err := delegatingCache.Watch(ctx, NewViewObjectList("test", "view"))
			Expect(err).NotTo(HaveOccurred())

			obj := object.NewViewObject("test", "view")
			object.SetContent(obj, map[string]any{"data": "original data"})
			object.SetName(obj, "ns", "test-delete")
			delegatingCache.Add(obj)
			object.WithUID(obj)

			go func() {
				time.Sleep(25 * time.Millisecond)
				delegatingCache.Delete(obj)
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

		It("should notify of objects deleted in the underlying storage", func() {
			delegatingCache := NewDelegatingViewCache(sharedStorage, CacheOptions{Logger: logger})
			Expect(delegatingCache).NotTo(BeNil())
			Expect(delegatingCache.storage).To(Equal(sharedStorage))

			watcher, err := delegatingCache.Watch(ctx, NewViewObjectList("test", "view"))
			Expect(err).NotTo(HaveOccurred())

			obj := object.NewViewObject("test", "view")
			object.SetContent(obj, map[string]any{"data": "original data"})
			object.SetName(obj, "ns", "test-delete")
			delegatingCache.Add(obj)
			object.WithUID(obj)

			go func() {
				time.Sleep(25 * time.Millisecond)
				sharedStorage.Delete(obj)
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

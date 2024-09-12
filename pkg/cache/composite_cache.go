package cache

// composite cache is a cache that serves views from the view cache and the rest from th default
// Kubernetes cache

/////////////////
// CREATE source.Source

// import (
//     "sigs.k8s.io/controller-runtime/pkg/source"
// )

// // In your controller setup
// err := ctrl.NewControllerManagedBy(mgr).
//     For(&yourv1.YourCustomType{}).
//     WatchesRawSource(
//         source.Kind(compositeCache, &yourv1.YourCustomType{}),
//         &handler.EnqueueRequestForObject{},
//     ).
//     Complete(reconciler)

///////////////////
// Create controller

// import (
//     "sigs.k8s.io/controller-runtime/pkg/manager"
//     "sigs.k8s.io/controller-runtime/pkg/cache"
//     "sigs.k8s.io/controller-runtime/pkg/client"
// )

// func SetupManager(config *rest.Config) (manager.Manager, error) {
//     customCache := NewCustomCache(/* your custom client */, /* your scheme */)
//     compositeCache := &CompositeCache{
//         customCache: customCache,
//         customGroup: "your.custom.group",
//     }

//     mgr, err := manager.New(config, manager.Options{
//         NewCache: func(config *rest.Config, opts cache.Options) (cache.Cache, error) {
//             // Create the default cache
//             defaultCache, err := cache.New(config, opts)
//             if err != nil {
//                 return nil, err
//             }

//             // Set the default cache in the composite cache
//             compositeCache.defaultCache = defaultCache

//             return compositeCache, nil
//         },
//     })
//     if err != nil {
//         return nil, err
//     }

//     // Use the manager as usual
//     return mgr, nil
// }

//////////////////////////////////
// CompositeCache

// import (
//     "context"
//     "sigs.k8s.io/controller-runtime/pkg/cache"
//     "sigs.k8s.io/controller-runtime/pkg/client"
//     "k8s.io/apimachinery/pkg/runtime/schema"
// )

// type CompositeCache struct {
//     defaultCache cache.Cache
//     customCache  *CustomCache
//     customGroup  string
// }

// // Ensure CompositeCache implements cache.Cache
// var _ cache.Cache = &CompositeCache{}

// // Implement cache.Cache methods

// func (cc *CompositeCache) GetInformer(ctx context.Context, obj client.Object) (cache.SharedIndexInformer, error) {
//     gvk := obj.GetObjectKind().GroupVersionKind()
//     if gvk.Group == cc.customGroup {
//         return cc.customCache.GetCustomInformer(obj)
//     }
//     return cc.defaultCache.GetInformer(ctx, obj)
// }

// func (cc *CompositeCache) GetInformerForKind(ctx context.Context, gvk schema.GroupVersionKind) (cache.SharedIndexInformer, error) {
//     if gvk.Group == cc.customGroup {
//         return cc.customCache.GetCustomInformerForKind(gvk)
//     }
//     return cc.defaultCache.GetInformerForKind(ctx, gvk)
// }

// func (cc *CompositeCache) Start(ctx context.Context) error {
//     if err := cc.customCache.Start(ctx); err != nil {
//         return err
//     }
//     return cc.defaultCache.Start(ctx)
// }

// func (cc *CompositeCache) WaitForCacheSync(ctx context.Context) bool {
//     return cc.customCache.WaitForCacheSync(ctx) && cc.defaultCache.WaitForCacheSync(ctx)
// }

// func (cc *CompositeCache) IndexField(ctx context.Context, obj client.Object, field string, extractValue client.IndexerFunc) error {
//     gvk := obj.GetObjectKind().GroupVersionKind()
//     if gvk.Group == cc.customGroup {
//         return cc.customCache.IndexField(ctx, obj, field, extractValue)
//     }
//     return cc.defaultCache.IndexField(ctx, obj, field, extractValue)
// }

// // Implement client.Reader methods

// func (cc *CompositeCache) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
//     gvk := obj.GetObjectKind().GroupVersionKind()
//     if gvk.Group == cc.customGroup {
//         return cc.customCache.Get(ctx, key, obj, opts...)
//     }
//     return cc.defaultCache.Get(ctx, key, obj, opts...)
// }

// func (cc *CompositeCache) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
//     gvk := list.GetObjectKind().GroupVersionKind()
//     if gvk.Group == cc.customGroup {
//         return cc.customCache.List(ctx, list, opts...)
//     }
//     return cc.defaultCache.List(ctx, list, opts...)
// }

// // You may need to implement additional methods depending on the exact interface of cache.Cache in your version of controller-runtime

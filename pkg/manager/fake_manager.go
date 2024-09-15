package manager

import (
	"context"
	"fmt"
	"net/http"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	ccache "hsnlab/dcontroller-runtime/pkg/cache"
)

var _ manager.Manager = &FakeRuntimeManager{}
var _ manager.Manager = &FakeManager{}

// /////// FakeManager
type FakeManager struct {
	*Manager
	// runtime
	fakeRuntimeManager manager.Manager
	fakeRuntimeCache   *ccache.FakeRuntimeCache
	fakeRuntimeClient  client.WithWatch
	// composite
	compositeCache *ccache.CompositeCache
}

func NewFakeManager(ctx context.Context, logger logr.Logger, objs ...client.Object) (*FakeManager, error) {
	fakeRuntimeCache := ccache.NewFakeRuntimeCache(nil)
	compositeCache, err := ccache.NewCompositeCache(nil, ccache.Options{
		DefaultCache: fakeRuntimeCache,
		Logger:       &logger,
	})
	if err != nil {
		return nil, err
	}

	fakeRuntimeClient := fake.NewClientBuilder().WithObjects(objs...).Build()
	fakeRuntimeManager := NewFakeRuntimeManager(compositeCache, &compositeClient{
		Client:         fakeRuntimeClient,
		compositeCache: compositeCache,
	})

	// mgr, err := New(&rest.Config{Host: "https://fake.example.com"}, Options{
	mgr, err := New(nil, Options{
		Manager: fakeRuntimeManager,
	})
	if err != nil {
		return nil, err
	}

	fm := &FakeManager{
		Manager:            mgr,
		fakeRuntimeManager: fakeRuntimeManager,
		fakeRuntimeCache:   fakeRuntimeCache,
		fakeRuntimeClient:  fakeRuntimeClient,
		compositeCache:     compositeCache,
	}

	go fm.Start(ctx)
	return fm, nil
}

func (m *FakeManager) GetRuntimeManager() manager.Manager        { return m.fakeRuntimeManager }
func (m *FakeManager) GetRuntimeCache() *ccache.FakeRuntimeCache { return m.fakeRuntimeCache }
func (m *FakeManager) GetRuntimeClient() client.WithWatch        { return m.fakeRuntimeClient }
func (m *FakeManager) GetCompositeCache() *ccache.CompositeCache { return m.compositeCache }

///////// FakeRuntimeManager

type FakeRuntimeManager struct {
	Client client.Client
	Cache  cache.Cache
	Scheme *runtime.Scheme
}

func NewFakeRuntimeManager(cache cache.Cache, client client.Client) *FakeRuntimeManager {
	return &FakeRuntimeManager{
		Cache:  cache,
		Client: client,
		Scheme: runtime.NewScheme(),
	}
}

// manager.Manager
func (f *FakeRuntimeManager) Add(runnable manager.Runnable) error                      { return nil }
func (f *FakeRuntimeManager) Elected() <-chan struct{}                                 { return nil }
func (f *FakeRuntimeManager) AddHealthzCheck(name string, check healthz.Checker) error { return nil }
func (f *FakeRuntimeManager) AddReadyzCheck(name string, check healthz.Checker) error  { return nil }
func (f *FakeRuntimeManager) Start(ctx context.Context) error                          { return nil }
func (f *FakeRuntimeManager) GetWebhookServer() webhook.Server                         { return nil }
func (f *FakeRuntimeManager) GetLogger() logr.Logger                                   { return logr.New(nil) }
func (f *FakeRuntimeManager) GetControllerOptions() config.Controller                  { return config.Controller{} }
func (f *FakeRuntimeManager) AddMetricsServerExtraHandler(path string, handler http.Handler) error {
	return nil
}

// cluster.Cluster
func (f *FakeRuntimeManager) GetHTTPClient() *http.Client                          { return nil }
func (f *FakeRuntimeManager) GetConfig() *rest.Config                              { return nil }
func (f *FakeRuntimeManager) GetCache() cache.Cache                                { return f.Cache }
func (f *FakeRuntimeManager) GetScheme() *runtime.Scheme                           { return f.Scheme }
func (f *FakeRuntimeManager) GetClient() client.Client                             { return f.Client }
func (f *FakeRuntimeManager) GetFieldIndexer() client.FieldIndexer                 { return nil }
func (f *FakeRuntimeManager) GetEventRecorderFor(name string) record.EventRecorder { return nil }
func (f *FakeRuntimeManager) GetRESTMapper() meta.RESTMapper                       { return &fakeRESTMapper{} }
func (f *FakeRuntimeManager) GetAPIReader() client.Reader                          { return nil }

/////////////////////

var _ meta.RESTMapper = &fakeRESTMapper{}

var fakeRESTMap = map[schema.GroupKind]string{
	{Group: "", Kind: "Pod"}:        "v1",
	{Group: "", Kind: "Deployment"}: "v1",
	{Group: "", Kind: "Service"}:    "v1",
}

type fakeRESTMapper struct{}

func (d *fakeRESTMapper) KindFor(resource schema.GroupVersionResource) (schema.GroupVersionKind, error) {
	if v, ok := fakeRESTMap[schema.GroupKind{Group: resource.Group, Kind: resource.Resource}]; ok {
		return schema.GroupVersionKind{Group: resource.Group, Kind: resource.Resource, Version: v}, nil
	}
	return schema.GroupVersionKind{}, fmt.Errorf("no RESTmapping for GR %s", resource.String())
}

func (d *fakeRESTMapper) KindsFor(resource schema.GroupVersionResource) ([]schema.GroupVersionKind, error) {
	if v, ok := fakeRESTMap[schema.GroupKind{Group: resource.Group, Kind: resource.Resource}]; ok {
		return []schema.GroupVersionKind{{Group: resource.Group, Kind: resource.Resource, Version: v}}, nil
	}
	return nil, fmt.Errorf("no RESTmapping for GR %s", resource.String())
}

func (d *fakeRESTMapper) ResourceFor(input schema.GroupVersionResource) (schema.GroupVersionResource, error) {
	return schema.GroupVersionResource{}, nil
}

func (d *fakeRESTMapper) ResourcesFor(input schema.GroupVersionResource) ([]schema.GroupVersionResource, error) {
	return nil, nil
}

func (d *fakeRESTMapper) RESTMapping(gk schema.GroupKind, versions ...string) (*meta.RESTMapping, error) {
	return nil, nil
}

func (d *fakeRESTMapper) RESTMappings(gk schema.GroupKind, versions ...string) ([]*meta.RESTMapping, error) {
	return nil, nil
}

func (d *fakeRESTMapper) ResourceSingularizer(resource string) (singular string, err error) {
	return "", nil
}

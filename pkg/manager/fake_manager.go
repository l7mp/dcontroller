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
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
)

var _ manager.Manager = &FakeManager{}

type FakeManager struct {
	Client client.Client
	Cache  cache.Cache
	Scheme *runtime.Scheme
}

func NewFakeManager(cache cache.Cache, client client.Client) *FakeManager {
	return &FakeManager{
		Cache:  cache,
		Client: client,
		Scheme: runtime.NewScheme(),
	}
}

// manager.Manager
func (f *FakeManager) Add(runnable manager.Runnable) error                      { return nil }
func (f *FakeManager) Elected() <-chan struct{}                                 { return nil }
func (f *FakeManager) AddHealthzCheck(name string, check healthz.Checker) error { return nil }
func (f *FakeManager) AddReadyzCheck(name string, check healthz.Checker) error  { return nil }
func (f *FakeManager) Start(ctx context.Context) error                          { return nil }
func (f *FakeManager) GetWebhookServer() webhook.Server                         { return nil }
func (f *FakeManager) GetLogger() logr.Logger                                   { return logr.New(nil) }
func (f *FakeManager) GetControllerOptions() config.Controller                  { return config.Controller{} }
func (f *FakeManager) AddMetricsServerExtraHandler(path string, handler http.Handler) error {
	return nil
}

// cluster.Cluster
func (f *FakeManager) GetHTTPClient() *http.Client                          { return nil }
func (f *FakeManager) GetConfig() *rest.Config                              { return nil }
func (f *FakeManager) GetCache() cache.Cache                                { return f.Cache }
func (f *FakeManager) GetScheme() *runtime.Scheme                           { return f.Scheme }
func (f *FakeManager) GetClient() client.Client                             { return f.Client }
func (f *FakeManager) GetFieldIndexer() client.FieldIndexer                 { return nil }
func (f *FakeManager) GetEventRecorderFor(name string) record.EventRecorder { return nil }
func (f *FakeManager) GetRESTMapper() meta.RESTMapper                       { return &fakeRESTMapper{} }
func (f *FakeManager) GetAPIReader() client.Reader                          { return nil }

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

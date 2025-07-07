package manager

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/testing"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	"github.com/l7mp/dcontroller/pkg/composite"
	"github.com/l7mp/dcontroller/pkg/object"
)

var _ manager.Manager = &FakeRuntimeManager{}
var _ manager.Manager = &FakeManager{}

// FakeManager is a fake manager that is used for testing.
type FakeManager struct {
	*Manager
	// runtime
	fakeRuntimeManager manager.Manager
	fakeRuntimeCache   *composite.FakeRuntimeCache
	fakeRuntimeClient  client.WithWatch
	tracker            testing.ObjectTracker
	// composite
	compositeCache *composite.CompositeCache
	// allow to push a fake config
	Cfg *rest.Config
}

// NewFakeManager creates a new fake manager.
func NewFakeManager(opts manager.Options, objs ...client.Object) (*FakeManager, error) {
	logger := opts.Logger
	if logger.GetSink() == nil {
		logger = logr.Discard()
	}

	fakeRuntimeCache := composite.NewFakeRuntimeCache(nil)
	// add initial onjects here too (also to fake runtime client later)
	for _, o := range objs {
		if err := fakeRuntimeCache.Add(o.(object.Object)); err != nil {
			return nil, err
		}
	}

	compositeCache, err := composite.NewCompositeCache(nil, composite.CacheOptions{
		DefaultCache: fakeRuntimeCache,
		Logger:       logger,
	})
	if err != nil {
		return nil, err
	}

	scheme := object.GetBaseScheme()
	tracker := testing.NewObjectTracker(scheme, serializer.NewCodecFactory(scheme).UniversalDecoder())
	fakeRuntimeClient := fake.NewClientBuilder().
		WithObjectTracker(tracker).
		WithObjects(objs...).
		Build()
	fakeCompositeClient, err := composite.NewCompositeClient(nil, composite.ClientOptions{})
	if err != nil {
		return nil, err
	}
	fakeCompositeClient.SetClient(fakeRuntimeClient)
	fakeCompositeClient.SetCache(compositeCache)
	fakeRuntimeManager := NewFakeRuntimeManager(compositeCache, fakeCompositeClient, logger)

	mgr, err := New(nil, Options{Options: opts, Manager: fakeRuntimeManager})
	if err != nil {
		return nil, err
	}

	return &FakeManager{
		Manager:            mgr,
		fakeRuntimeManager: fakeRuntimeManager,
		fakeRuntimeCache:   fakeRuntimeCache,
		fakeRuntimeClient:  fakeRuntimeClient,
		compositeCache:     compositeCache,
		tracker:            tracker,
	}, nil
}

func (m *FakeManager) GetManager() manager.Manager                  { return m.Manager }
func (m *FakeManager) GetRuntimeManager() manager.Manager           { return m.fakeRuntimeManager }
func (m *FakeManager) GetRuntimeCache() *composite.FakeRuntimeCache { return m.fakeRuntimeCache }
func (m *FakeManager) GetRuntimeClient() client.WithWatch           { return m.fakeRuntimeClient }
func (m *FakeManager) GetCompositeCache() *composite.CompositeCache { return m.compositeCache }
func (m *FakeManager) GetObjectTracker() testing.ObjectTracker      { return m.tracker }

// Runnable is something that can be run.
type Runnable interface {
	manager.Runnable
	GetName() string
}

// FakeRuntimeManager is a runtime manager fake.
type FakeRuntimeManager struct {
	Client       client.Client
	Cache        cache.Cache
	Scheme       *runtime.Scheme
	runnables    []manager.Runnable
	started      bool
	startedMutex sync.Mutex
	ctx          context.Context
	logger, log  logr.Logger
}

func NewFakeRuntimeManager(cache cache.Cache, client client.Client, logger logr.Logger) *FakeRuntimeManager {
	scheme := object.GetBaseScheme()
	return &FakeRuntimeManager{
		Cache:  cache,
		Client: client,
		Scheme: scheme,
		logger: logger,
		log:    logger.WithName("fakeruntimemanager"),
	}
}

func (f *FakeRuntimeManager) Elected() <-chan struct{}                                 { return nil }
func (f *FakeRuntimeManager) SetFields(i interface{}) error                            { return nil }
func (f *FakeRuntimeManager) AddHealthzCheck(name string, check healthz.Checker) error { return nil }
func (f *FakeRuntimeManager) AddReadyzCheck(name string, check healthz.Checker) error  { return nil }
func (f *FakeRuntimeManager) GetWebhookServer() webhook.Server                         { return nil }
func (f *FakeRuntimeManager) GetLogger() logr.Logger                                   { return f.logger }
func (f *FakeRuntimeManager) GetControllerOptions() config.Controller                  { return config.Controller{} }
func (f *FakeRuntimeManager) AddMetricsServerExtraHandler(path string, handler http.Handler) error {
	return nil
}

func (f *FakeRuntimeManager) Add(runnable manager.Runnable) error {
	f.log.V(4).Info("adding runnable")

	f.startedMutex.Lock()
	defer f.startedMutex.Unlock()

	if f.started {
		name := "<unknown>"
		if r, ok := runnable.(Runnable); ok {
			name = r.GetName()
		}
		f.log.V(4).Info("starting runnable", "started", f.started, "name", name)
		go runnable.Start(f.ctx) //nolint:errcheck
	}

	f.runnables = append(f.runnables, runnable)

	return nil
}

func (f *FakeRuntimeManager) Start(ctx context.Context) error {
	f.startedMutex.Lock()
	if f.started {
		return nil
	}
	f.ctx = ctx
	f.started = true
	runnables := make([]manager.Runnable, len(f.runnables))
	copy(runnables, f.runnables)
	f.startedMutex.Unlock()

	f.log.V(4).Info("starting fake runtime manager")

	for _, runnable := range runnables {
		name := "<unknown>"
		if r, ok := runnable.(Runnable); ok {
			name = r.GetName()
		}
		f.log.V(4).Info("starting runnable", "started", f.started, "name", name)

		go runnable.Start(f.ctx) //nolint:errcheck
	}

	<-ctx.Done()

	return nil
}

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

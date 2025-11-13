package apiserver

import (
	"fmt"
	"maps"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/managedfields"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/endpoints/handlers"

	viewv1a1 "github.com/l7mp/dcontroller/pkg/api/view/v1alpha1"
)

// resourceHandler routes requests for dynamically registered resources to their storage implementations.
// It uses an atomic map for lock-free reads and supports dynamic registration/unregistration.
// This pattern is inspired by Kubernetes CRD handlers.
type resourceHandler struct {
	// Atomic storage map for lock-free reads: map[GVR]*servingInfo.
	storage atomic.Value

	// Protects storage updates (writes only).
	storageLock sync.Mutex

	// Configuration.
	delegatingClient client.Client
	scheme           *runtime.Scheme
	codecs           runtime.NegotiatedSerializer

	// Delegate for non-dynamic requests.
	delegate http.Handler

	log logr.Logger
}

// resourceHandlerConfig provides configuration for creating a resourceHandler.
type resourceHandlerConfig struct {
	delegatingClient client.Client
	scheme           *runtime.Scheme
	codecs           runtime.NegotiatedSerializer
	delegate         http.Handler
	log              logr.Logger
}

// storageMap maps GVR to serving information.
type storageMap map[schema.GroupVersionResource]*servingInfo

// servingInfo contains everything needed to serve a resource.
type servingInfo struct {
	storage      *ClientDelegatedStorage // Storage instance.
	requestScope *handlers.RequestScope  // Pre-built scope for handlers.
}

// newResourceHandler creates a new dynamic resource handler.
func newResourceHandler(config resourceHandlerConfig) *resourceHandler {
	handler := &resourceHandler{
		delegatingClient: config.delegatingClient,
		scheme:           config.scheme,
		codecs:           config.codecs,
		delegate:         config.delegate,
		log:              config.log,
	}

	// Initialize with empty map.
	handler.storage.Store(make(storageMap))

	return handler
}

// ServeHTTP implements http.Handler and routes resource requests.
func (h *resourceHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// Extract request info from context (set by earlier middleware).
	requestInfo, ok := genericapirequest.RequestInfoFrom(req.Context())
	if !ok {
		h.log.V(5).Info("no request info in context, delegating", "path", req.URL.Path)
		h.delegate.ServeHTTP(w, req)
		return
	}

	// Always delegate non-resource requests (discovery, etc.) to the chain.
	// This includes /apis, /apis/<group>, /apis/<group>/<version> which are handled
	// by dynamicVersionHandler -> dynamicGroupHandler -> go-restful.
	if !requestInfo.IsResourceRequest {
		h.log.V(5).Info("not a resource request, delegating", "path", req.URL.Path,
			"isResourceRequest", requestInfo.IsResourceRequest)
		h.delegate.ServeHTTP(w, req)
		return
	}

	// Construct GVR from request.
	gvr := schema.GroupVersionResource{
		Group:    requestInfo.APIGroup,
		Version:  requestInfo.APIVersion,
		Resource: requestInfo.Resource,
	}

	// Only handle dynamically registered resources (views for now).
	if !viewv1a1.IsViewGroup(gvr.Group) {
		h.log.V(5).Info("not a view group, delegating", "group", gvr.Group)
		h.delegate.ServeHTTP(w, req)
		return
	}

	// Lookup serving info from atomic map (lock-free read).
	currentStorage := h.storage.Load().(storageMap)
	info, ok := currentStorage[gvr]
	if !ok {
		// Resource not registered - delegate.
		h.log.V(5).Info("resource not found in handler, delegating",
			"GVR", gvr.String(), "path", req.URL.Path)
		h.delegate.ServeHTTP(w, req)
		return
	}

	h.log.V(2).Info("serving resource from dynamic handler",
		"GVR", gvr.String(), "verb", requestInfo.Verb, "path", req.URL.Path)

	// Route to appropriate handler.
	h.routeRequest(w, req, requestInfo, info)
}

// routeRequest routes a request to the appropriate Kubernetes handler based on verb.
func (h *resourceHandler) routeRequest(
	w http.ResponseWriter,
	req *http.Request,
	requestInfo *genericapirequest.RequestInfo,
	info *servingInfo,
) {
	scope := info.requestScope
	storage := info.storage

	// No subresources supported.
	if requestInfo.Subresource != "" {
		http.Error(w, "subresources not supported", http.StatusNotFound)
		return
	}

	// Route based on verb.
	switch requestInfo.Verb {
	case "get":
		handlers.GetResource(storage, scope)(w, req)

	case "list":
		forceWatch := false
		minRequestTimeout := 1 * time.Minute
		handlers.ListResource(storage, storage, scope, forceWatch, minRequestTimeout)(w, req)

	case "watch":
		forceWatch := true
		minRequestTimeout := 1 * time.Minute
		handlers.ListResource(storage, storage, scope, forceWatch, minRequestTimeout)(w, req)

	case "create":
		// No admission control for unstructured objects.
		admit := admission.NewChainHandler()
		handlers.CreateResource(storage, scope, admit)(w, req)

	case "update":
		// No admission control for unstructured objects.
		admit := admission.NewChainHandler()
		handlers.UpdateResource(storage, scope, admit)(w, req)

	case "patch":
		// No admission control for unstructured objects.
		admit := admission.NewChainHandler()
		supportedTypes := []string{
			string(types.JSONPatchType),
			string(types.MergePatchType),
			string(types.StrategicMergePatchType),
			string(types.ApplyPatchType),
		}
		handlers.PatchResource(storage, scope, admit, supportedTypes)(w, req)

	case "delete":
		gracefulDeleter := false
		admit := admission.NewChainHandler()
		handlers.DeleteResource(storage, gracefulDeleter, scope, admit)(w, req)

	case "deletecollection":
		checkBody := false
		admit := admission.NewChainHandler()
		handlers.DeleteCollection(storage, checkBody, scope, admit)(w, req)

	default:
		http.Error(w, fmt.Sprintf("unsupported verb: %s", requestInfo.Verb),
			http.StatusMethodNotAllowed)
	}
}

// addStorage registers a new GVR/GVK dynamically.
func (h *resourceHandler) addStorage(gvr schema.GroupVersionResource,
	gvk schema.GroupVersionKind,
	resource *Resource) error {
	h.storageLock.Lock()
	defer h.storageLock.Unlock()

	// Create serving info.
	info, err := h.createServingInfo(gvr, gvk, resource)
	if err != nil {
		return fmt.Errorf("failed to create serving info: %w", err)
	}

	// Clone current map.
	oldMap := h.storage.Load().(storageMap)
	newMap := make(storageMap, len(oldMap)+1)
	maps.Copy(newMap, oldMap)

	// Add new entry.
	newMap[gvr] = info

	// Atomically swap maps (lock-free for readers).
	h.storage.Store(newMap)

	h.log.V(2).Info("storage registered", "GVR", gvr.String())
	return nil
}

// removeStorage unregisters a GVR dynamically.
func (h *resourceHandler) removeStorage(gvr schema.GroupVersionResource) {
	h.storageLock.Lock()
	defer h.storageLock.Unlock()

	// Clone current map without the target GVR.
	oldMap := h.storage.Load().(storageMap)
	newMap := make(storageMap, len(oldMap))
	for k, v := range oldMap {
		if k != gvr {
			newMap[k] = v
		}
	}

	// Atomically swap maps.
	h.storage.Store(newMap)

	h.log.V(2).Info("storage unregistered", "GVR", gvr.String())
}

// createServingInfo builds the serving info for a GVR/GVK.
func (h *resourceHandler) createServingInfo(
	gvr schema.GroupVersionResource,
	gvk schema.GroupVersionKind,
	resource *Resource,
) (*servingInfo, error) {
	// Create ClientDelegatedStorage.
	delegatingWatcher, ok := h.delegatingClient.(client.WithWatch)
	if !ok {
		return nil, fmt.Errorf("delegating client does not support Watch")
	}

	storage := &ClientDelegatedStorage{
		delegatingClient:  h.delegatingClient,
		delegatingWatcher: delegatingWatcher,
		gvk:               gvk,
		gvr:               gvr,
		namespaced:        resource.APIResource.Namespaced,
		hasStatus:         resource.HasStatus,
		log:               h.log,
	}

	// Create FieldManager for managedFields tracking.
	// Use the default CRD field manager which works with unstructured objects.
	typeConverter := managedfields.NewDeducedTypeConverter()
	fieldManager, err := managedfields.NewDefaultCRDFieldManager(
		typeConverter,
		h.scheme, // objectConverter
		h.scheme, // objectDefaulter
		h.scheme, // objectCreater
		gvk,
		gvk.GroupVersion(),
		"",  // subresource (empty for main resource)
		nil, // resetFields (nil means no fields to reset)
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create field manager: %w", err)
	}

	// Build RequestScope for handlers.
	// This is what handlers.* functions need to operate.
	scope := &handlers.RequestScope{
		Namer: handlers.ContextBasedNaming{
			Namer:         runtime.Namer(meta.NewAccessor()),
			ClusterScoped: !resource.APIResource.Namespaced,
		},
		Serializer:      h.codecs,
		ParameterCodec:  runtime.NewParameterCodec(h.scheme),
		Creater:         h.scheme, //nolint:misspell
		Convertor:       h.scheme,
		Defaulter:       h.scheme,
		Typer:           h.scheme,
		UnsafeConvertor: runtime.UnsafeObjectConvertor(h.scheme),
		TableConvertor:  storage, // For kubectl output.

		Resource:         gvr,
		Kind:             gvk,
		MetaGroupVersion: metav1.SchemeGroupVersion,
		HubGroupVersion:  gvk.GroupVersion(),

		MaxRequestBodyBytes: 3 * 1024 * 1024, // 3 MB default.

		// FieldManager for server-side apply and managedFields tracking.
		FieldManager: fieldManager,
	}

	return &servingInfo{
		storage:      storage,
		requestScope: scope,
	}, nil
}

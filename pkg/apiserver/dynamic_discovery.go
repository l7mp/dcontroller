package apiserver

import (
	"net/http"
	"strings"
	"sync"

	"github.com/go-logr/logr"
	"k8s.io/apiserver/pkg/endpoints/discovery"
)

// dynamicGroupHandler is a custom HTTP handler that dynamically routes API group discovery
// requests based on an internal map. This allows for adding/removing/replacing groups without
// modifying the underlying go-restful container.
//
// It implements a pattern similar to how Kubernetes CRDs handle dynamic API groups.
type dynamicGroupHandler struct {
	mu       sync.RWMutex
	handlers map[string]*discovery.APIGroupHandler // group name -> handler
	delegate http.Handler                          // fallback handler
	log      logr.Logger
}

// newDynamicGroupHandler creates a new dynamic group discovery handler.
func newDynamicGroupHandler(delegate http.Handler, log logr.Logger) *dynamicGroupHandler {
	return &dynamicGroupHandler{
		handlers: make(map[string]*discovery.APIGroupHandler),
		delegate: delegate,
		log:      log,
	}
}

// ServeHTTP implements http.Handler and routes requests to registered group handlers.
func (h *dynamicGroupHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	pathParts := splitPath(req.URL.Path)

	// Only match /apis/<group>
	if len(pathParts) != 2 || pathParts[0] != "apis" {
		h.log.V(5).Info("not a group discovery request, delegating",
			"path", req.URL.Path, "pathParts", len(pathParts))
		// h.delegate.ServeHTTP(w, req)
		http.Error(w, "not a group discovery request", http.StatusBadRequest)
		return
	}

	groupName := pathParts[1]

	h.mu.RLock()
	handler, ok := h.handlers[groupName]
	h.mu.RUnlock()

	if !ok {
		h.log.V(2).Info("group not found in dynamic handler, delegating",
			"group", groupName, "path", req.URL.Path)
		// h.delegate.ServeHTTP(w, req)
		http.Error(w, "not a group discovery request", http.StatusNotFound)
		return
	}

	h.log.V(2).Info("serving group from dynamic handler",
		"group", groupName, "path", req.URL.Path)
	handler.ServeHTTP(w, req)
}

// setHandler registers or updates a handler for the given group.
// This is safe to call multiple times for the same group (idempotent).
func (h *dynamicGroupHandler) setHandler(group string, handler *discovery.APIGroupHandler) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.handlers[group] = handler
	h.log.V(4).Info("registered dynamic group handler", "group", group)
}

// removeHandler removes the handler for the given group.
func (h *dynamicGroupHandler) removeHandler(group string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	delete(h.handlers, group)
	h.log.V(4).Info("removed dynamic group handler", "group", group)
}

// dynamicVersionHandler is a custom HTTP handler that dynamically routes API version discovery
// requests based on an internal map. This allows for adding/removing/replacing versions without
// modifying the underlying go-restful container.
type dynamicVersionHandler struct {
	mu       sync.RWMutex
	handlers map[string]*discovery.APIVersionHandler // "group/version" -> handler
	delegate http.Handler                            // fallback handler
	log      logr.Logger
}

// newDynamicVersionHandler creates a new dynamic version discovery handler.
func newDynamicVersionHandler(delegate http.Handler, log logr.Logger) *dynamicVersionHandler {
	return &dynamicVersionHandler{
		handlers: make(map[string]*discovery.APIVersionHandler),
		delegate: delegate,
		log:      log,
	}
}

// ServeHTTP implements http.Handler and routes requests to registered version handlers.
func (h *dynamicVersionHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	pathParts := splitPath(req.URL.Path)

	// Only match /apis/<group>/<version>
	if len(pathParts) != 3 || pathParts[0] != "apis" {
		h.delegate.ServeHTTP(w, req)
		return
	}

	groupVersion := pathParts[1] + "/" + pathParts[2]

	h.mu.RLock()
	handler, ok := h.handlers[groupVersion]
	h.mu.RUnlock()

	if !ok {
		h.log.V(5).Info("version not found in dynamic handler, delegating",
			"groupVersion", groupVersion, "path", req.URL.Path)
		h.delegate.ServeHTTP(w, req)
		return
	}

	h.log.V(5).Info("serving version from dynamic handler",
		"groupVersion", groupVersion, "path", req.URL.Path)
	handler.ServeHTTP(w, req)
}

// setHandler registers or updates a handler for the given group/version.
// This is safe to call multiple times for the same version (idempotent).
func (h *dynamicVersionHandler) setHandler(group, version string, handler *discovery.APIVersionHandler) {
	h.mu.Lock()
	defer h.mu.Unlock()

	key := group + "/" + version
	h.handlers[key] = handler
	h.log.V(4).Info("registered dynamic version handler", "group", group, "version", version)
}

// removeHandler removes the handler for the given group/version.
func (h *dynamicVersionHandler) removeHandler(group, version string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	key := group + "/" + version
	delete(h.handlers, key)
	h.log.V(4).Info("removed dynamic version handler", "group", group, "version", version)
}

// splitPath returns the segments for a URL path.
func splitPath(path string) []string {
	path = strings.Trim(path, "/")
	if path == "" {
		return []string{}
	}
	return strings.Split(path, "/")
}

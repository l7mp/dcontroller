package composite

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ ClientMultiplexer = &clientMultiplexer{}

// ClientMultiplexer multiplexes controller-runtime clients by API group
type ClientMultiplexer interface {
	client.WithWatch

	RegisterClient(group string, client client.Client) error
	UnregisterClient(group string) error
}

// clientMultiplexer implements ClientMultiplexer
type clientMultiplexer struct {
	mu      sync.RWMutex
	clients map[string]client.Client
	log     logr.Logger
}

// NewClientMultiplexer creates a new ClientMultiplexer instance
func NewClientMultiplexer(logger logr.Logger) ClientMultiplexer {
	if logger.GetSink() == nil {
		logger = logr.Discard()
	}

	return &clientMultiplexer{
		clients: make(map[string]client.Client),
		log:     logger.WithName("client-mpx"),
	}
}

// RegisterClient registers a client for the specified API group
func (m *clientMultiplexer) RegisterClient(group string, c client.Client) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.clients[group]; exists {
		return fmt.Errorf("client for group %q already registered", group)
	}

	m.clients[group] = c

	m.log.V(3).Info("API group registered", "group", group)

	return nil
}

// UnregisterClient removes the client for the specified API group
func (m *clientMultiplexer) UnregisterClient(group string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.clients[group]; !exists {
		return fmt.Errorf("no client registered for group %q", group)
	}

	delete(m.clients, group)

	m.log.V(3).Info("API group unregistered", "group", group)

	return nil
}

// getClientForObject determines the appropriate client based on the object's API group
func (m *clientMultiplexer) getClientForObject(obj client.Object) (client.Client, error) {
	gvk := obj.GetObjectKind().GroupVersionKind()
	if gvk.Empty() {
		return nil, apierrors.NewBadRequest("object does not have GroupVersionKind set")
	}

	return m.getClientForGroup(gvk.Group, gvk.Kind)
}

// getClientForObject determines the appropriate client based on the object's API group
func (m *clientMultiplexer) getClientForObjectList(list client.ObjectList) (client.Client, error) {
	gvk := list.GetObjectKind().GroupVersionKind()
	if gvk.Empty() {
		return nil, apierrors.NewBadRequest("object does not have GroupVersionKind set")
	}

	return m.getClientForGroup(gvk.Group, gvk.Kind)
}

// getClientForGroup returns the client for the specified API group
func (m *clientMultiplexer) getClientForGroup(group, kind string) (client.Client, error) {
	m.mu.RLock()
	c, exists := m.clients[group]
	defer m.mu.RUnlock()

	if !exists {
		return nil, apierrors.NewNotFound(schema.GroupResource{
			Group:    group,
			Resource: kind,
		}, "client-mpx")
	}

	return c, nil
}

// Client interface implementation

// Get retrieves an obj for the given object key from the Kubernetes Cluster
func (m *clientMultiplexer) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	m.log.V(3).Info("GET", "GVK", client.ObjectKeyFromObject(obj).String())

	c, err := m.getClientForObject(obj)
	if err != nil {
		return err
	}
	return c.Get(ctx, key, obj, opts...)
}

// List retrieves list of objects for a given namespace and list options
func (m *clientMultiplexer) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	m.log.V(3).Info("LIST", "GVK", list.GetObjectKind().GroupVersionKind().String())

	c, err := m.getClientForObjectList(list)
	if err != nil {
		return err
	}

	return c.List(ctx, list, opts...)
}

// Create saves the object obj in the Kubernetes cluster
func (m *clientMultiplexer) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	m.log.V(3).Info("CREATE", "GVK", client.ObjectKeyFromObject(obj).String())

	c, err := m.getClientForObject(obj)
	if err != nil {
		return err
	}
	return c.Create(ctx, obj, opts...)
}

// Delete deletes the given obj from Kubernetes cluster
func (m *clientMultiplexer) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	m.log.V(3).Info("DELETE", "GVK", client.ObjectKeyFromObject(obj).String())

	c, err := m.getClientForObject(obj)
	if err != nil {
		return err
	}
	return c.Delete(ctx, obj, opts...)
}

// Update updates the given obj in the Kubernetes cluster
func (m *clientMultiplexer) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	m.log.V(3).Info("UPDATE", "GVK", client.ObjectKeyFromObject(obj).String())

	c, err := m.getClientForObject(obj)
	if err != nil {
		return err
	}
	return c.Update(ctx, obj, opts...)
}

// Patch patches the given obj in the Kubernetes cluster
func (m *clientMultiplexer) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	m.log.V(3).Info("PATCH", "GVK", client.ObjectKeyFromObject(obj).String())

	c, err := m.getClientForObject(obj)
	if err != nil {
		return err
	}
	return c.Patch(ctx, obj, patch, opts...)
}

// DeleteAllOf deletes all objects of the given type matching the given options
func (m *clientMultiplexer) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error {
	m.log.V(3).Info("DELETE-ALL", "GVK", client.ObjectKeyFromObject(obj).String())

	c, err := m.getClientForObject(obj)
	if err != nil {
		return err
	}
	return c.DeleteAllOf(ctx, obj, opts...)
}

// Status returns a client which can update status subresource for kubernetes objects
func (m *clientMultiplexer) Status() client.StatusWriter {
	return &statusWriter{multiplexer: m}
}

// Scheme returns the scheme this client is using
func (m *clientMultiplexer) Scheme() *runtime.Scheme {
	// No scheme available in this multiplexer
	return nil
}

// RESTMapper returns the rest this client is using
func (m *clientMultiplexer) RESTMapper() meta.RESTMapper {
	// This is more complex as we'd need to merge RESTMappers from all clients
	// For now, return the first available one
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, c := range m.clients {
		if rm := c.RESTMapper(); rm != nil {
			return rm
		}
	}
	return nil
}

// GroupVersionKindFor returns the GroupVersionKind for the given object
func (m *clientMultiplexer) GroupVersionKindFor(obj runtime.Object) (schema.GroupVersionKind, error) {
	// First try to get GVK directly from the object
	if gvkObj, ok := obj.(schema.ObjectKind); ok {
		if gvk := gvkObj.GroupVersionKind(); !gvk.Empty() {
			return gvk, nil
		}
	}

	// If no GVK is set, we need to try each client's scheme to identify the object
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, c := range m.clients {
		if scheme := c.Scheme(); scheme != nil {
			if gvks, _, err := scheme.ObjectKinds(obj); err == nil && len(gvks) > 0 {
				// Found a match in this client's scheme
				return gvks[0], nil
			}
		}
	}

	return schema.GroupVersionKind{}, fmt.Errorf("unable to determine GroupVersionKind for object type %T", obj)
}

// IsObjectNamespaced returns true if the object is namespaced
func (m *clientMultiplexer) IsObjectNamespaced(obj runtime.Object) (bool, error) {
	gvk, err := m.GroupVersionKindFor(obj)
	if err != nil {
		return false, err
	}

	c, err := m.getClientForGroup(gvk.Group, gvk.Kind)
	if err != nil {
		return false, err
	}

	return c.IsObjectNamespaced(obj)
}

// SubResource returns a client for the specified subresource
func (m *clientMultiplexer) SubResource(subResource string) client.SubResourceClient {
	return &subResourceMultiplexer{
		multiplexer: m,
		subResource: subResource,
	}
}

// Watch interface implementation

// Watch watches objects of type obj and sends events on the returned channel
func (m *clientMultiplexer) Watch(ctx context.Context, list client.ObjectList, opts ...client.ListOption) (watch.Interface, error) {
	m.log.V(3).Info("WATCH", "GVK", list.GetObjectKind().GroupVersionKind().String())

	c, err := m.getClientForObjectList(list)
	if err != nil {
		return nil, err
	}

	// Check if the client supports watching
	if wc, ok := c.(client.WithWatch); ok {
		return wc.Watch(ctx, list, opts...)
	}

	return nil, fmt.Errorf("client for group %q does not support watch",
		list.GetObjectKind().GroupVersionKind().String())
}

// statusWriter implements client.StatusWriter by delegating to the appropriate client
type statusWriter struct {
	multiplexer *clientMultiplexer
}

// Update updates the status subresource of the given object
func (sw *statusWriter) Create(ctx context.Context, obj client.Object, subResource client.Object, opts ...client.SubResourceCreateOption) error {
	c, err := sw.multiplexer.getClientForObject(obj)
	if err != nil {
		return err
	}
	return c.Status().Create(ctx, obj, subResource, opts...)
}

// Update updates the status subresource of the given object
func (sw *statusWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	c, err := sw.multiplexer.getClientForObject(obj)
	if err != nil {
		return err
	}
	return c.Status().Update(ctx, obj, opts...)
}

// Patch patches the status subresource of the given object
func (sw *statusWriter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	c, err := sw.multiplexer.getClientForObject(obj)
	if err != nil {
		return err
	}
	return c.Status().Patch(ctx, obj, patch, opts...)
}

// subResourceMultiplexer implements client.SubResourceClient by delegating to the appropriate client
type subResourceMultiplexer struct {
	multiplexer *clientMultiplexer
	subResource string
}

// Get retrieves the subresource for the given object
func (sr *subResourceMultiplexer) Get(ctx context.Context, obj, subResource client.Object, opts ...client.SubResourceGetOption) error {
	c, err := sr.multiplexer.getClientForObject(obj)
	if err != nil {
		return err
	}
	return c.SubResource(sr.subResource).Get(ctx, obj, subResource, opts...)
}

// Create creates the subresource for the given object
func (sr *subResourceMultiplexer) Create(ctx context.Context, obj client.Object, subResource client.Object, opts ...client.SubResourceCreateOption) error {
	c, err := sr.multiplexer.getClientForObject(obj)
	if err != nil {
		return err
	}
	return c.SubResource(sr.subResource).Create(ctx, obj, subResource, opts...)
}

// Update updates the subresource for the given object
func (sr *subResourceMultiplexer) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	c, err := sr.multiplexer.getClientForObject(obj)
	if err != nil {
		return err
	}
	return c.SubResource(sr.subResource).Update(ctx, obj, opts...)
}

// Patch patches the subresource for the given object
func (sr *subResourceMultiplexer) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	c, err := sr.multiplexer.getClientForObject(obj)
	if err != nil {
		return err
	}
	return c.SubResource(sr.subResource).Patch(ctx, obj, patch, opts...)
}

package composite

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"

	viewv1a1 "github.com/l7mp/dcontroller/pkg/api/view/v1alpha1"
)

var _ meta.RESTMapper = &ViewRESTMapper{}

// ViewRESTMapper implements meta.RESTMapper for view resources.
type ViewRESTMapper struct{}

// NewViewRESTMapper creates a new view REST mapper.
func NewViewRESTMapper() *ViewRESTMapper {
	return &ViewRESTMapper{}
}

// KindFor returns the Kind for the given view resource.
func (m *ViewRESTMapper) KindFor(resource schema.GroupVersionResource) (schema.GroupVersionKind, error) {
	if viewv1a1.IsViewGroup(resource.Group) {
		return schema.GroupVersionKind{}, fmt.Errorf("not a view group: %s", resource.Group)
	}

	// Convert resource name to Kind (title case)
	kind := strings.Title(strings.ToLower(resource.Resource)) //nolint:staticcheck

	return schema.GroupVersionKind{
		Group:   resource.Group,
		Version: resource.Version,
		Kind:    kind,
	}, nil
}

// KindsFor returns all Kinds for the given view resource (just one).
func (m *ViewRESTMapper) KindsFor(resource schema.GroupVersionResource) ([]schema.GroupVersionKind, error) {
	gvk, err := m.KindFor(resource)
	if err != nil {
		return nil, err
	}
	return []schema.GroupVersionKind{gvk}, nil
}

// ResourceFor returns the Resource for the given view input.
func (m *ViewRESTMapper) ResourceFor(input schema.GroupVersionResource) (schema.GroupVersionResource, error) {
	if viewv1a1.IsViewGroup(input.Group) {
		return schema.GroupVersionResource{}, fmt.Errorf("not a view group: %s", input.Group)
	}

	// For views, resource names are lowercase kinds
	return schema.GroupVersionResource{
		Group:    input.Group,
		Version:  input.Version,
		Resource: strings.ToLower(input.Resource),
	}, nil
}

// ResourcesFor returns all Resources for the given view input (just one).
func (m *ViewRESTMapper) ResourcesFor(input schema.GroupVersionResource) ([]schema.GroupVersionResource, error) {
	gvr, err := m.ResourceFor(input)
	if err != nil {
		return nil, err
	}
	return []schema.GroupVersionResource{gvr}, nil
}

// RESTMapping returns the RESTMapping for the given view GroupKind.
func (m *ViewRESTMapper) RESTMapping(gk schema.GroupKind, versions ...string) (*meta.RESTMapping, error) {
	if viewv1a1.IsViewGroup(gk.Group) {
		return nil, fmt.Errorf("not a view group: %s", gk.Group)
	}

	// Use provided version or default to v1alpha1
	version := viewv1a1.Version
	if len(versions) > 0 && versions[0] != "" {
		version = versions[0]
	}

	gvk := schema.GroupVersionKind{
		Group:   gk.Group,
		Version: version,
		Kind:    gk.Kind,
	}

	gvr := schema.GroupVersionResource{
		Group:    gk.Group,
		Version:  version,
		Resource: strings.ToLower(gk.Kind),
	}

	return &meta.RESTMapping{
		Resource:         gvr,
		GroupVersionKind: gvk,
		Scope:            meta.RESTScopeNamespace, // All views are namespaced
	}, nil
}

// RESTMappings returns all RESTMappings for the given view GroupKind (just one).
func (m *ViewRESTMapper) RESTMappings(gk schema.GroupKind, versions ...string) ([]*meta.RESTMapping, error) {
	mapping, err := m.RESTMapping(gk, versions...)
	if err != nil {
		return nil, err
	}
	return []*meta.RESTMapping{mapping}, nil
}

// ResourceSingularizer returns the singular form (same as plural for views).
func (m *ViewRESTMapper) ResourceSingularizer(resource string) (string, error) {
	// For views, singular == plural (both lowercase kind)
	return strings.ToLower(resource), nil
}

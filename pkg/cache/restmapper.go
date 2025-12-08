package cache

import (
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/restmapper"
)

var _ meta.RESTMapper = &CompositeRESTMapper{}

// CompositeRESTMapper implements meta.RESTMapper by routing view groups to ViewRESTMapper and
// native groups to native RESTMapper.
type CompositeRESTMapper struct {
	viewMapper   *ViewRESTMapper
	nativeMapper meta.RESTMapper
	discovery    discovery.DiscoveryInterface
}

// NewCompositeRESTMapper creates a new composite REST mapper.
func NewCompositeRESTMapper(compositeDiscovery discovery.DiscoveryInterface) *CompositeRESTMapper {
	// Create native RESTMapper from discovery if available
	var nativeMapper meta.RESTMapper
	if compositeDiscovery != nil {
		// Use discovery to build a standard RESTMapper
		groupResources, err := restmapper.GetAPIGroupResources(compositeDiscovery)
		if err == nil {
			nativeMapper = restmapper.NewDiscoveryRESTMapper(groupResources)
		}
	}

	// Create view RESTMapper
	viewMapper := NewViewRESTMapper()

	return &CompositeRESTMapper{
		viewMapper:   viewMapper,
		nativeMapper: nativeMapper,
		discovery:    compositeDiscovery,
	}
}

// KindFor returns the Kind for the given resource.
func (m *CompositeRESTMapper) KindFor(resource schema.GroupVersionResource) (schema.GroupVersionKind, error) {
	if m.isViewGroup(resource.Group) {
		return m.viewMapper.KindFor(resource)
	}

	if m.nativeMapper != nil {
		return m.nativeMapper.KindFor(resource)
	}

	return schema.GroupVersionKind{}, fmt.Errorf("no RESTMapper available for resource %s", resource)
}

// KindsFor returns all Kinds for the given resource.
func (m *CompositeRESTMapper) KindsFor(resource schema.GroupVersionResource) ([]schema.GroupVersionKind, error) {
	if m.isViewGroup(resource.Group) {
		return m.viewMapper.KindsFor(resource)
	}

	if m.nativeMapper != nil {
		return m.nativeMapper.KindsFor(resource)
	}

	return nil, fmt.Errorf("no RESTMapper available for resource %s", resource)
}

// ResourceFor returns the Resource for the given input.
func (m *CompositeRESTMapper) ResourceFor(input schema.GroupVersionResource) (schema.GroupVersionResource, error) {
	if m.isViewGroup(input.Group) {
		return m.viewMapper.ResourceFor(input)
	}

	if m.nativeMapper != nil {
		return m.nativeMapper.ResourceFor(input)
	}

	return schema.GroupVersionResource{}, fmt.Errorf("no RESTMapper available for resource %s", input)
}

// ResourcesFor returns all Resources for the given input.
func (m *CompositeRESTMapper) ResourcesFor(input schema.GroupVersionResource) ([]schema.GroupVersionResource, error) {
	if m.isViewGroup(input.Group) {
		return m.viewMapper.ResourcesFor(input)
	}

	if m.nativeMapper != nil {
		return m.nativeMapper.ResourcesFor(input)
	}

	return nil, fmt.Errorf("no RESTMapper available for resource %s", input)
}

// RESTMapping returns the RESTMapping for the given GroupKind.
func (m *CompositeRESTMapper) RESTMapping(gk schema.GroupKind, versions ...string) (*meta.RESTMapping, error) {
	if m.isViewGroup(gk.Group) {
		return m.viewMapper.RESTMapping(gk, versions...)
	}

	if m.nativeMapper != nil {
		return m.nativeMapper.RESTMapping(gk, versions...)
	}

	return nil, fmt.Errorf("no RESTMapper available for GroupKind %s", gk)
}

// RESTMappings returns all RESTMappings for the given GroupKind.
func (m *CompositeRESTMapper) RESTMappings(gk schema.GroupKind, versions ...string) ([]*meta.RESTMapping, error) {
	if m.isViewGroup(gk.Group) {
		return m.viewMapper.RESTMappings(gk, versions...)
	}

	if m.nativeMapper != nil {
		return m.nativeMapper.RESTMappings(gk, versions...)
	}

	return nil, fmt.Errorf("no RESTMapper available for GroupKind %s", gk)
}

// ResourceSingularizer returns the singular form of the resource.
func (m *CompositeRESTMapper) ResourceSingularizer(resource string) (string, error) {
	// For views, singular == plural (both lowercase kind)
	// For native resources, delegate to native mapper
	if m.nativeMapper != nil {
		singular, err := m.nativeMapper.ResourceSingularizer(resource)
		if err == nil {
			return singular, nil
		}
	}

	// Default: return as-is (works for views)
	return resource, nil
}

// isViewGroup checks if a group is a view group.
func (m *CompositeRESTMapper) isViewGroup(group string) bool {
	if cd, ok := m.discovery.(*CompositeDiscoveryClient); ok {
		return cd.IsViewGroup(group)
	}
	// Fallback check
	return group == "view.dcontroller.io"
}

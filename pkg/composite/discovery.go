package composite

import (
	"fmt"

	openapiv2 "github.com/google/gnostic-models/openapiv2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/version"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/openapi"
	restclient "k8s.io/client-go/rest"
)

var _ discovery.DiscoveryInterface = &CompositeDiscoveryClient{}

// CompositeDiscoveryClient implements discovery.DiscoveryInterface by routing
// view groups to ViewDiscovery and native groups to native discovery
type CompositeDiscoveryClient struct {
	viewDiscovery   ViewDiscoveryInterface
	nativeDiscovery discovery.DiscoveryInterface
}

// NewCompositeDiscoveryClient creates a new composite discovery client
func NewCompositeDiscoveryClient(nativeDiscovery discovery.DiscoveryInterface) *CompositeDiscoveryClient {
	return &CompositeDiscoveryClient{
		viewDiscovery:   NewViewDiscovery(),
		nativeDiscovery: nativeDiscovery,
	}
}

// RESTClient implements discovery.DiscoveryInterface
func (c *CompositeDiscoveryClient) RESTClient() restclient.Interface {
	if c.nativeDiscovery != nil {
		return c.nativeDiscovery.RESTClient()
	}
	return nil
}

// ServerResourcesForGroupVersion implements discovery.DiscoveryInterface
func (c *CompositeDiscoveryClient) ServerResourcesForGroupVersion(groupVersion string) (*metav1.APIResourceList, error) {
	gv, err := schema.ParseGroupVersion(groupVersion)
	if err != nil {
		return nil, fmt.Errorf("invalid group version %s: %w", groupVersion, err)
	}

	if c.viewDiscovery.IsViewGroup(gv.Group) {
		return c.viewDiscovery.ServerResourcesForGroupVersion(groupVersion)
	}

	if c.nativeDiscovery == nil {
		return nil, fmt.Errorf("native discovery not available for group version %s", groupVersion)
	}

	return c.nativeDiscovery.ServerResourcesForGroupVersion(groupVersion)
}

// ServerGroups implements discovery.DiscoveryInterface
func (c *CompositeDiscoveryClient) ServerGroups() (*metav1.APIGroupList, error) {
	var groups []metav1.APIGroup

	// Add view groups
	viewGroups, err := c.viewDiscovery.ServerGroups()
	if err != nil {
		return nil, err
	}
	groups = append(groups, viewGroups.Groups...)

	// Add native groups
	if c.nativeDiscovery != nil {
		nativeGroupList, err := c.nativeDiscovery.ServerGroups()
		if err != nil {
			return nil, fmt.Errorf("failed to get native server groups: %w", err)
		}
		groups = append(groups, nativeGroupList.Groups...)
	}

	return &metav1.APIGroupList{Groups: groups}, nil
}

// ServerGroupsAndResources implements discovery.DiscoveryInterface
func (c *CompositeDiscoveryClient) ServerGroupsAndResources() ([]*metav1.APIGroup, []*metav1.APIResourceList, error) {
	var allGroups []*metav1.APIGroup
	var allResources []*metav1.APIResourceList

	// Add view groups and resources
	viewGroups, viewResources, err := c.viewDiscovery.ServerGroupsAndResources()
	if err != nil {
		return nil, nil, err
	}
	allGroups = append(allGroups, viewGroups...)
	allResources = append(allResources, viewResources...)

	// Add native groups and resources
	if c.nativeDiscovery != nil {
		nativeGroups, nativeResources, err := c.nativeDiscovery.ServerGroupsAndResources()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get native groups and resources: %w", err)
		}
		allGroups = append(allGroups, nativeGroups...)
		allResources = append(allResources, nativeResources...)
	}

	return allGroups, allResources, nil
}

// ServerPreferredResources implements discovery.DiscoveryInterface
func (c *CompositeDiscoveryClient) ServerPreferredResources() ([]*metav1.APIResourceList, error) {
	var allResources []*metav1.APIResourceList

	// Add view preferred resources
	viewResources, err := c.viewDiscovery.ServerPreferredResources()
	if err != nil {
		return nil, err
	}
	allResources = append(allResources, viewResources...)

	// Add native preferred resources
	if c.nativeDiscovery != nil {
		nativeResources, err := c.nativeDiscovery.ServerPreferredResources()
		if err != nil {
			return nil, fmt.Errorf("failed to get native preferred resources: %w", err)
		}
		allResources = append(allResources, nativeResources...)
	}

	return allResources, nil
}

// ServerPreferredNamespacedResources implements discovery.DiscoveryInterface
func (c *CompositeDiscoveryClient) ServerPreferredNamespacedResources() ([]*metav1.APIResourceList, error) {
	var allResources []*metav1.APIResourceList

	// Add view namespaced resources (all views are namespaced)
	viewResources, err := c.viewDiscovery.ServerPreferredResources()
	if err != nil {
		return nil, err
	}
	allResources = append(allResources, viewResources...)

	// Add native namespaced resources
	if c.nativeDiscovery != nil {
		nativeResources, err := c.nativeDiscovery.ServerPreferredNamespacedResources()
		if err != nil {
			return nil, fmt.Errorf("failed to get native namespaced resources: %w", err)
		}
		allResources = append(allResources, nativeResources...)
	}

	return allResources, nil
}

// ServerVersion implements discovery.DiscoveryInterface
func (c *CompositeDiscoveryClient) ServerVersion() (*version.Info, error) {
	if c.nativeDiscovery != nil {
		return c.nativeDiscovery.ServerVersion()
	}

	// Return a default version if no native discovery
	return &version.Info{
		Major:      "1",
		Minor:      "33",
		GitVersion: "v1.33.0-dcontroller",
	}, nil
}

// OpenAPISchema implements discovery.DiscoveryInterface
func (c *CompositeDiscoveryClient) OpenAPISchema() (*openapiv2.Document, error) {
	if c.nativeDiscovery != nil {
		return c.nativeDiscovery.OpenAPISchema()
	}
	return nil, fmt.Errorf("OpenAPI schema not available")
}

// OpenAPIV3 implements discovery.DiscoveryInterface (for newer K8s versions)
func (c *CompositeDiscoveryClient) OpenAPIV3() openapi.Client {
	if c.nativeDiscovery != nil {
		return c.nativeDiscovery.OpenAPIV3()
	}
	return nil
}

// WithLegacy implements discovery.DiscoveryInterface
func (c *CompositeDiscoveryClient) WithLegacy() discovery.DiscoveryInterface {
	if c.nativeDiscovery != nil {
		return NewCompositeDiscoveryClient(c.nativeDiscovery.WithLegacy())
	}
	// If no native discovery, return self (views don't have legacy concerns)
	return c
}

// View-specific extensions (non-standard but essential for view Kind/List mapping)
func (c *CompositeDiscoveryClient) ViewDiscovery() ViewDiscoveryInterface {
	return c.viewDiscovery
}

// RegisterViewGVK registers a new view GVK for discovery
func (c *CompositeDiscoveryClient) RegisterViewGVK(gvk schema.GroupVersionKind) error {
	return c.viewDiscovery.RegisterViewGVK(gvk)
}

// IsViewGroup returns true if the group is a view group
func (c *CompositeDiscoveryClient) IsViewGroup(group string) bool {
	return c.viewDiscovery.IsViewGroup(group)
}

// IsViewKind returns true if this is a view object kind (not a list)
func (c *CompositeDiscoveryClient) IsViewKind(gvk schema.GroupVersionKind) bool {
	return c.viewDiscovery.IsViewKind(gvk)
}

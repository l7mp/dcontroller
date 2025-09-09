package composite

import (
	"fmt"
	"strings"
	"sync"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"

	viewv1a1 "github.com/l7mp/dcontroller/pkg/api/view/v1alpha1"
)

// ViewDiscoveryInterface handles view-specific discovery operations.
type ViewDiscoveryInterface interface {
	discovery.ServerResourcesInterface
	discovery.ServerGroupsInterface

	// View-specific methods
	IsViewGroup(group string) bool
	IsViewKind(gvk schema.GroupVersionKind) bool
	IsViewListKind(gvk schema.GroupVersionKind) bool
	ObjectGVKFromListGVK(listGVK schema.GroupVersionKind) schema.GroupVersionKind
	ListGVKFromObjectGVK(objGVK schema.GroupVersionKind) schema.GroupVersionKind
	ResourceFromKind(kind string) string
	KindFromResource(resource string) (string, error)

	// Dynamic registration
	RegisterViewGVK(gvk schema.GroupVersionKind) error
	UnregisterViewGVK(gvk schema.GroupVersionKind) error
	GetRegisteredViewGVKs() []schema.GroupVersionKind
}

var _ ViewDiscoveryInterface = &ViewDiscovery{}

// ViewDiscovery implements discovery for view resources.
type ViewDiscovery struct {
	registeredViews map[schema.GroupVersionKind]*metav1.APIResource
	mu              sync.RWMutex
}

// NewViewDiscovery creates a new view discovery instance.
func NewViewDiscovery() *ViewDiscovery {
	return &ViewDiscovery{
		registeredViews: make(map[schema.GroupVersionKind]*metav1.APIResource),
	}
}

// ServerResourcesForGroupVersion returns API resources for a view group version.
func (d *ViewDiscovery) ServerResourcesForGroupVersion(groupVersion string) (*metav1.APIResourceList, error) {
	gv, err := schema.ParseGroupVersion(groupVersion)
	if err != nil {
		return nil, err
	}

	d.mu.RLock()
	defer d.mu.RUnlock()

	var resources []metav1.APIResource
	for gvk, apiResource := range d.registeredViews {
		if gvk.GroupVersion() == gv {
			resources = append(resources, *apiResource)
		}
	}

	return &metav1.APIResourceList{
		GroupVersion: groupVersion,
		APIResources: resources,
	}, nil
}

// ServerGroups returns available view API groups.
func (d *ViewDiscovery) ServerGroups() (*metav1.APIGroupList, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	groupVersions := make(map[string][]metav1.GroupVersionForDiscovery)

	for gvk := range d.registeredViews {
		gv := gvk.GroupVersion()
		if _, exists := groupVersions[gv.Group]; !exists {
			groupVersions[gv.Group] = []metav1.GroupVersionForDiscovery{}
		}

		// Check if this version already exists
		for _, existing := range groupVersions[gv.Group] {
			if existing.Version == gv.Version {
				continue
			}
		}

		groupVersions[gv.Group] = append(groupVersions[gv.Group], metav1.GroupVersionForDiscovery{
			GroupVersion: gv.String(),
			Version:      gv.Version,
		})
	}

	var groups metav1.APIGroupList
	for group, versions := range groupVersions {
		apiGroup := metav1.APIGroup{
			Name:     group,
			Versions: versions,
		}

		// Set preferred version (use the first one, or v1alpha1 if available)
		if len(versions) > 0 {
			apiGroup.PreferredVersion = versions[0]
			for _, v := range versions {
				if v.Version == viewv1a1.Version {
					apiGroup.PreferredVersion = v
					break
				}
			}
		}

		groups.Groups = append(groups.Groups, apiGroup)
	}

	return &groups, nil
}

// ServerGroupsAndResources returns groups and resources separately.
func (d *ViewDiscovery) ServerGroupsAndResources() ([]*metav1.APIGroup, []*metav1.APIResourceList, error) {
	groupList, err := d.ServerGroups()
	if err != nil {
		return nil, nil, err
	}

	groups := make([]*metav1.APIGroup, len(groupList.Groups))
	for i := range groupList.Groups {
		groups[i] = &groupList.Groups[i]
	}

	// Get all resource lists
	var resourceLists []*metav1.APIResourceList
	groupVersions := make(map[schema.GroupVersion]bool)

	d.mu.RLock()
	for gvk := range d.registeredViews {
		groupVersions[gvk.GroupVersion()] = true
	}
	d.mu.RUnlock()

	for gv := range groupVersions {
		resourceList, err := d.ServerResourcesForGroupVersion(gv.String())
		if err != nil {
			return nil, nil, err
		}
		if len(resourceList.APIResources) > 0 {
			resourceLists = append(resourceLists, resourceList)
		}
	}

	return groups, resourceLists, nil
}

// ServerPreferredResources returns preferred API resources.
func (d *ViewDiscovery) ServerPreferredResources() ([]*metav1.APIResourceList, error) {
	_, resourceLists, err := d.ServerGroupsAndResources()
	if err != nil {
		return nil, err
	}
	return resourceLists, nil
}

// ServerPreferredNamespacedResources returns the supported namespaced resources with the version
// preferred by the server.
func (d *ViewDiscovery) ServerPreferredNamespacedResources() ([]*metav1.APIResourceList, error) {
	// All view resources are namespaced, so this is the same as ServerPreferredResources
	return d.ServerPreferredResources()
}

// IsViewGroup returns true if the group is a view group.
func (d *ViewDiscovery) IsViewGroup(group string) bool {
	return viewv1a1.IsViewGroup(group)
}

// IsViewKind returns true if this is a view object kind (not a list).
func (d *ViewDiscovery) IsViewKind(gvk schema.GroupVersionKind) bool {
	return viewv1a1.IsViewKind(gvk) && !strings.HasSuffix(gvk.Kind, "List")
}

// IsViewListKind returns true if this is a view list kind.
func (d *ViewDiscovery) IsViewListKind(gvk schema.GroupVersionKind) bool {
	return viewv1a1.IsViewKind(gvk) && strings.HasSuffix(gvk.Kind, "List")
}

// ObjectGVKFromListGVK converts a list GVK to its object GVK.
func (d *ViewDiscovery) ObjectGVKFromListGVK(listGVK schema.GroupVersionKind) schema.GroupVersionKind {
	if d.IsViewListKind(listGVK) {
		return schema.GroupVersionKind{
			Group:   listGVK.Group,
			Version: listGVK.Version,
			Kind:    strings.TrimSuffix(listGVK.Kind, "List"),
		}
	}
	return listGVK // Already an object GVK
}

// ListGVKFromObjectGVK converts an object GVK to its list GVK.
func (d *ViewDiscovery) ListGVKFromObjectGVK(objGVK schema.GroupVersionKind) schema.GroupVersionKind {
	if d.IsViewKind(objGVK) {
		return schema.GroupVersionKind{
			Group:   objGVK.Group,
			Version: objGVK.Version,
			Kind:    objGVK.Kind + "List",
		}
	}
	return objGVK // Already a list GVK
}

// ResourceFromKind converts view kind to resource name. The convention is that the resource name
// is the lowercase kind, which is typically singular. This is a digression from Kubernetes where
// the resource name is plural. Since views are declared implicitly with the singular name (as a
// controller source or target) we cannot know the plural.
func (d *ViewDiscovery) ResourceFromKind(kind string) string {
	if strings.HasSuffix(kind, "List") {
		return strings.ToLower(strings.TrimSuffix(kind, "List"))
	}

	return strings.ToLower(kind)
}

// KindFromResource converts resource name to view kind (Title case). Works only for registered views.
func (d *ViewDiscovery) KindFromResource(resource string) (string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	// Look for a registered view that matches this resource
	for gvk, apiResource := range d.registeredViews {
		if apiResource.Name == resource || apiResource.SingularName == resource {
			return gvk.Kind, nil
		}
		// Also check if the lowercase kind matches the base resource
		if strings.ToLower(gvk.Kind) == resource {
			return gvk.Kind, nil
		}
	}

	return "", fmt.Errorf("view resource %q not registered", resource)
}

// RegisterViewGVK registers a new view GVK for discovery.
func (d *ViewDiscovery) RegisterViewGVK(gvk schema.GroupVersionKind) error {
	if !d.IsViewKind(gvk) {
		return nil // Ignore non-view or list kinds
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if _, exists := d.registeredViews[gvk]; exists {
		return nil // Already registered
	}

	// Create APIResource for this view
	resource := &metav1.APIResource{
		Name:         d.ResourceFromKind(gvk.Kind),
		SingularName: d.ResourceFromKind(gvk.Kind),
		Namespaced:   true,
		Kind:         gvk.Kind,
		Group:        gvk.Group,
		Version:      gvk.Version,
		Verbs:        []string{"get", "list", "create", "update", "patch", "delete", "watch"},
		Categories:   []string{"dcontroller", "views"},
	}

	d.registeredViews[gvk] = resource
	return nil
}

// UnregisterViewGVK removes a view GVK from discovery.
func (d *ViewDiscovery) UnregisterViewGVK(gvk schema.GroupVersionKind) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	delete(d.registeredViews, gvk)
	return nil
}

// GetRegisteredViewGVKs returns all registered view GVKs.
func (d *ViewDiscovery) GetRegisteredViewGVKs() []schema.GroupVersionKind {
	d.mu.RLock()
	defer d.mu.RUnlock()

	gvks := make([]schema.GroupVersionKind, 0, len(d.registeredViews))
	for gvk := range d.registeredViews {
		gvks = append(gvks, gvk)
	}

	return gvks
}

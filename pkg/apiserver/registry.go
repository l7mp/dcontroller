package apiserver

import (
	"fmt"

	apidiscoveryv2 "k8s.io/api/apidiscovery/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/endpoints/discovery"

	viewv1a1 "github.com/l7mp/dcontroller/pkg/api/view/v1alpha1"
)

type GroupGVKs = map[string]map[schema.GroupVersionKind]bool

// RegisterGVKs registers a set of GVks with the embedded API server. Divides the GVKs per group,
// checks if none of the groups have already been registered, and registers each group and the
// corresponding GVKs.
func (s *APIServer) RegisterGVKs(gvks []schema.GroupVersionKind) error {
	// Group GVKs by Group for API group registration
	groupGVKs := make(map[string][]schema.GroupVersionKind)
	for _, gvk := range gvks {
		groupGVKs[gvk.Group] = append(groupGVKs[gvk.Group], gvk)
	}

	for group, gvkList := range groupGVKs {
		if err := s.RegisterAPIGroup(group, gvkList); err != nil {
			return fmt.Errorf("failed to register API group %s: %w", group, err)
		}
	}

	return nil
}

// UnregisterGVKs unregisters a set of GVks.
func (s *APIServer) UnregisterGVKs(gvks []schema.GroupVersionKind) {
	// Group GVKs by Group for API group registration
	groups := make(map[string]bool)
	for _, gvk := range gvks {
		groups[gvk.Group] = true
	}

	for group := range groups {
		s.UnregisterAPIGroup(group)
	}
}

// RegisterAPIGroup installs an API group with all its registered GVKs to the API server.
// This function is fully idempotent and can be called multiple times for the same group.
// All registrations (first and subsequent) work identically using the dynamic resource handler.
func (s *APIServer) RegisterAPIGroup(group string, gvks []schema.GroupVersionKind) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Ignore native Kubernetes API resources.
	if !viewv1a1.IsViewGroup(group) {
		return nil
	}

	// 1. Add types to scheme (idempotent operation).
	if err := s.addTypesToScheme(gvks); err != nil {
		return fmt.Errorf("failed to add types to scheme: %w", err)
	}

	// 2. Register storage in resourceHandler (fully dynamic, no InstallAPIGroup).
	for _, gvk := range gvks {
		resource, err := s.findAPIResource(gvk)
		if err != nil {
			s.log.V(4).Info("skipping GVK", "gvk", gvk.String(), "error", err)
			continue
		}

		gvr := schema.GroupVersionResource{
			Group:    gvk.Group,
			Version:  gvk.Version,
			Resource: resource.APIResource.Name,
		}

		if err := s.resourceHandler.addStorage(gvr, gvk, resource); err != nil {
			return fmt.Errorf("failed to add storage for %s: %w", gvk.String(), err)
		}
	}

	// 3. Register discovery handlers (idempotent).
	s.registerDiscoveryHandlers(group, gvks)

	// 4. Update GVK cache.
	groupedGVKs := make(map[schema.GroupVersionKind]bool)
	for _, gvk := range gvks {
		groupedGVKs[gvk] = true
	}
	s.groupGVKs[group] = groupedGVKs

	// 5. Invalidate OpenAPI caches.
	s.cachedOpenAPIDefs = nil
	s.cachedOpenAPIV3Defs = nil

	s.log.Info("API group registered", "group", group, "GVKs", len(gvks))
	return nil
}

// UnregisterAPIGroup removes an API group with all its registered GVKs.
func (s *APIServer) UnregisterAPIGroup(group string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	gvks, ok := s.groupGVKs[group]
	if !ok {
		// Not registered, silently return.
		return
	}

	// Remove storage from resourceHandler.
	for gvk := range gvks {
		resource, err := s.findAPIResource(gvk)
		if err != nil {
			continue
		}

		gvr := schema.GroupVersionResource{
			Group:    gvk.Group,
			Version:  gvk.Version,
			Resource: resource.APIResource.Name,
		}

		s.resourceHandler.removeStorage(gvr)
	}

	// Remove discovery handlers.
	s.dynamicGroupHandler.removeHandler(group)
	for gvk := range gvks {
		s.dynamicVersionHandler.removeHandler(gvk.Group, gvk.Version)
	}

	// Remove from both discovery managers.
	s.server.DiscoveryGroupManager.RemoveGroup(group)
	if s.server.AggregatedDiscoveryGroupManager != nil {
		s.server.AggregatedDiscoveryGroupManager.RemoveGroup(group)
	}

	// Remove from cache.
	delete(s.groupGVKs, group)

	// Invalidate OpenAPI caches.
	s.cachedOpenAPIDefs = nil
	s.cachedOpenAPIV3Defs = nil

	s.log.Info("API group unregistered", "group", group)
}

// registerDiscoveryHandlers creates and registers discovery handlers for a group.
// This function is idempotent and can be called multiple times to update discovery info.
func (s *APIServer) registerDiscoveryHandlers(group string, gvks []schema.GroupVersionKind) {
	// Group by version
	versionedGVKs := make(map[string][]schema.GroupVersionKind)
	for _, gvk := range gvks {
		versionedGVKs[gvk.Version] = append(versionedGVKs[gvk.Version], gvk)
	}

	// Build API group metadata for discovery
	apiVersionsForDiscovery := []metav1.GroupVersionForDiscovery{}
	for version := range versionedGVKs {
		gv := schema.GroupVersion{Group: group, Version: version}
		apiVersionsForDiscovery = append(apiVersionsForDiscovery, metav1.GroupVersionForDiscovery{
			GroupVersion: gv.String(),
			Version:      version,
		})
	}

	// Create group-level discovery handler
	apiGroup := metav1.APIGroup{
		Name:             group,
		Versions:         apiVersionsForDiscovery,
		PreferredVersion: apiVersionsForDiscovery[0], // First version is preferred
	}

	// Register with our dynamic handler for /apis/<group>
	s.dynamicGroupHandler.setHandler(group, discovery.NewAPIGroupHandler(s.codecs, apiGroup))

	// Register with BOTH discovery managers for /apis endpoint:
	// 1. Legacy v1 discovery (returns APIGroupList)
	s.server.DiscoveryGroupManager.AddGroup(apiGroup)

	// Create version-level discovery handlers for each version
	for version, versionGVKs := range versionedGVKs {
		gv := schema.GroupVersion{Group: group, Version: version}

		// Build API resources for this version (v1 format)
		apiResources := []metav1.APIResource{}
		// Build API resources for v2 aggregated discovery
		discoveryResources := []apidiscoveryv2.APIResourceDiscovery{}

		for _, gvk := range versionGVKs {
			resource, err := s.findAPIResource(gvk)
			if err != nil {
				s.log.V(4).Info("skipping resource for discovery", "GVK", gvk.String(), "error", err)
				continue
			}

			// v1 format
			apiResources = append(apiResources, *resource.APIResource)

			// v2 format for aggregated discovery
			discoveryResources = append(discoveryResources, apidiscoveryv2.APIResourceDiscovery{
				Resource:         resource.APIResource.Name,
				ResponseKind:     &metav1.GroupVersionKind{Group: gvk.Group, Version: gvk.Version, Kind: gvk.Kind},
				Scope:            apidiscoveryv2.ScopeNamespace, // All views are namespaced
				SingularResource: resource.APIResource.SingularName,
				Verbs:            []string{"get", "list", "create", "update", "patch", "delete", "watch"},
			})
		}

		// Register version handler for /apis/<group>/<version>
		versionHandler := discovery.NewAPIVersionHandler(s.codecs, gv, discovery.APIResourceListerFunc(func() []metav1.APIResource {
			return apiResources
		}))
		s.dynamicVersionHandler.setHandler(group, version, versionHandler)

		// 2. Register with Aggregated v2 discovery manager
		// This is what modern discovery clients use (with Accept: apidiscovery.k8s.io/v2)
		if s.server.AggregatedDiscoveryGroupManager != nil {
			s.server.AggregatedDiscoveryGroupManager.AddGroupVersion(
				group,
				apidiscoveryv2.APIVersionDiscovery{
					Freshness: apidiscoveryv2.DiscoveryFreshnessCurrent,
					Version:   version,
					Resources: discoveryResources,
				},
			)
		}
	}
}

// addTypesToScheme adds a set of GVKs to the scheme.
func (s *APIServer) addTypesToScheme(gvks []schema.GroupVersionKind) error {
	// Group GVKs by GroupVersion
	groupVersions := make(map[schema.GroupVersion][]schema.GroupVersionKind)
	for _, gvk := range gvks {
		gv := gvk.GroupVersion()
		groupVersions[gv] = append(groupVersions[gv], gvk)
	}

	// Add types to existing scheme
	for gv, gvkList := range groupVersions {
		for _, gvk := range gvkList {
			// Add meta types for this group version if not already added
			metav1.AddToGroupVersion(s.scheme, gv)

			// Register the unstructured types
			s.scheme.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
			s.scheme.AddKnownTypeWithName(listGVK(gvk), &unstructured.UnstructuredList{})

			s.log.V(4).Info("added types to scheme", "GVK", gvk.String())
		}

		// Update version priority for this group
		if err := s.scheme.SetVersionPriority(gv); err != nil {
			return fmt.Errorf("failed to set version priority for %s: %w", gv.String(), err)
		}
	}

	return nil
}

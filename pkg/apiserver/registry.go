package apiserver

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	apiserverrest "k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	// metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
)

type GroupGVKs = map[string]map[schema.GroupVersionKind]bool

// RegisterGVKs registers a set of GVks with the API server. First divides the GVKs per group,
// checks if none of the groups have already been registered, and then registers each group and the
// corresponding GVKs.
func (s *APIServer) RegisterGVKs(gvks []schema.GroupVersionKind) error {
	// Group GVKs by Group for API group registration
	groupGVKs := make(map[string][]schema.GroupVersionKind)
	for _, gvk := range gvks {
		groupGVKs[gvk.Group] = append(groupGVKs[gvk.Group], gvk)
	}

	return s.registerGroupGVKs(groupGVKs)
}

// UnRegisterGVKs unregisters a set of GVks.
func (s *APIServer) UnRegisterGVKs(gvks []schema.GroupVersionKind) {
	// Group GVKs by Group for API group registration
	groups := make(map[string]bool)
	for _, gvk := range gvks {
		groups[gvk.Group] = true
	}

	for group, _ := range groups {
		s.UnregisterAPIGroup(group)
	}
}

// RegisterAPIGroup installs an API group with all its registered GVKs to the API server.
func (s *APIServer) RegisterAPIGroup(group string, gvks []schema.GroupVersionKind) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Err if group is already registered
	if _, ok := s.groupGVKs[group]; ok {
		return fmt.Errorf("API group %s already registered", group)
	}

	// Add new types to scheme
	if err := s.addTypesToScheme(gvks); err != nil {
		return fmt.Errorf("failed to add types to scheme: %w", err)
	}

	// Register the new group in the API server
	if err := s.registerAPIGroup(group, gvks); err != nil {
		return err
	}

	// Update the GVK cache
	groupedGVKs := make(map[schema.GroupVersionKind]bool)
	for _, gvk := range gvks {
		groupedGVKs[gvk] = true
	}
	s.groupGVKs[group] = groupedGVKs

	// Get all regitered GVKs
	allGVKs := []schema.GroupVersionKind{}
	for _, gvks := range s.groupGVKs {
		for gvk := range gvks {
			allGVKs = append(allGVKs, gvk)
		}
	}

	// Invalidate OpenAPI caches
	s.cachedOpenAPIDefs = nil
	s.cachedOpenAPIV3Defs = nil

	return nil
}

// UnregisterGVK removes a GVK from the server (silently ignores GVK if not registered)
func (s *APIServer) UnregisterAPIGroup(group string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.groupGVKs[group]; !ok {
		// Not registered, silently return
		return
	}

	delete(s.groupGVKs, group)

	// Regenerate OpenAPI specs after unregistering
	allGVKs := []schema.GroupVersionKind{}
	for _, gvks := range s.groupGVKs {
		for gvk := range gvks {
			allGVKs = append(allGVKs, gvk)
		}
	}

	// Invalidate OpenAPI caches
	s.cachedOpenAPIDefs = nil
	s.cachedOpenAPIV3Defs = nil

	s.log.V(1).Info("API group unregistered", "group", group)
}

func (s *APIServer) registerGroupGVKs(groupGVKs map[string][]schema.GroupVersionKind) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if any of the groups have already been registered
	for group, _ := range groupGVKs {
		if _, ok := s.groupGVKs[group]; ok {
			return fmt.Errorf("API group %s already registered", group)
		}
	}

	// Register each API group
	for group, gvkList := range groupGVKs {
		if err := s.registerAPIGroup(group, gvkList); err != nil {
			return fmt.Errorf("failed to register API group %s: %w", group, err)
		}
	}

	return nil
}

// registerAPIGroup registers a single API group with its resources
func (s *APIServer) registerAPIGroup(group string, gvks []schema.GroupVersionKind) error {
	// Group by version within this group
	versionedGVKs := make(map[string][]schema.GroupVersionKind)
	for _, gvk := range gvks {
		versionedGVKs[gvk.Version] = append(versionedGVKs[gvk.Version], gvk)
	}

	// Create storage map for all versions in this group
	versionedResourcesStorageMap := make(map[string]map[string]apiserverrest.Storage)

	failedGVKs := 0
	for version, versionGVKs := range versionedGVKs {
		resourceStorage := make(map[string]apiserverrest.Storage)

		for _, gvk := range versionGVKs {
			// Convert GVK to GVR
			resource, err := s.findAPIResource(gvk)
			if err != nil {
				// This is not fatal: since no API discovery ius available when
				// running without a real Kubernetes API server, we just note the
				// failure and move on
				// return fmt.Errorf("failed to complete API discovery for GVK %s: %w", gvk.String(), err)
				failedGVKs++
				continue
			}
			resourceName := resource.APIResource.Name

			// Create storage for this specific GVK
			restOptionsGetter := &RESTOptionsGetter{}
			storageProvider := NewClientDelegatedStorage(s.delegatingClient, resource, s.log)
			storage, err := storageProvider(s.scheme, restOptionsGetter)
			if err != nil {
				return fmt.Errorf("failed to create delegaing storage for %s: %w", gvk.String(), err)
			}

			resourceStorage[resourceName] = storage
			s.log.V(4).Info("registered storage", "GVK", gvk.String(), "resource", resourceName)
		}

		versionedResourcesStorageMap[version] = resourceStorage
	}

	// Create APIGroupInfo
	groupVersions := make([]schema.GroupVersion, 0, len(versionedGVKs))
	for version := range versionedGVKs {
		groupVersions = append(groupVersions, schema.GroupVersion{Group: group, Version: version})
	}
	apiGroupInfo := &genericapiserver.APIGroupInfo{
		PrioritizedVersions:          groupVersions,
		VersionedResourcesStorageMap: versionedResourcesStorageMap,
		OptionsExternalVersion:       &schema.GroupVersion{Version: "v1"},
		Scheme:                       s.scheme,
		ParameterCodec:               runtime.NewParameterCodec(s.scheme),
		NegotiatedSerializer:         s.codecs,
	}

	// Install the API group
	if err := s.server.InstallAPIGroup(apiGroupInfo); err != nil {
		return fmt.Errorf("failed to install API group %s: %w", group, err)
	}

	s.log.V(1).Info("API group registered", "group", group, "versions", groupVersions,
		"failed-GVKs", failedGVKs)

	return nil
}

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

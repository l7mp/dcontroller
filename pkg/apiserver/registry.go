package apiserver

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	apiserverrest "k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	// metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
)

type GroupGVKs = map[string]map[schema.GroupVersionKind]bool

// RegisterAPIGroup installs an API group with all its registered GVKs to the API server.
func (s *APIServer) RegisterAPIGroup(group string, gvks []schema.GroupVersionKind) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.groupGVKs[group]; ok {
		return fmt.Errorf("API group %s already registered with different GVKs", group)
	}

	if err := validateGVKsForGroup(group, gvks); err != nil {
		return err
	}

	if err := s.registerAPIGroup(group, gvks); err != nil {
		return err
	}

	groupedGVKs := make(map[schema.GroupVersionKind]bool)
	for _, gvk := range gvks {
		groupedGVKs[gvk] = true
	}
	s.groupGVKs[group] = groupedGVKs

	s.log.V(1).Info("API group registered", "group", group, "GVKs", gvks)

	return nil
}

// UnregisterGVK removes a GVK from the server (silently ignores if not registered)
func (s *APIServer) UnregisterAPIGroup(group string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.groupGVKs[group]; !ok {
		// Not registered, silently return
		return nil
	}

	delete(s.groupGVKs, group)

	s.log.V(1).Info("API group unregistered", "group", group)

	return nil
}

func (s *APIServer) RegisterBuiltinAPIGroups() error {
	var gvks []schema.GroupVersionKind
	for gvk := range s.scheme.AllKnownTypes() {
		// Skip internal versions and list types
		if gvk.Version == runtime.APIVersionInternal || strings.HasSuffix(gvk.Kind, "List") {
			continue
		}
		gvks = append(gvks, gvk)
	}

	if err := s.registerAPIGroups(gvks); err != nil {
		return fmt.Errorf("failed to register apps group: %w", err)
	}

	// Add other groups as needed
	return nil
}

// registerAPIGroups registers API groups and resources with the server
func (s *APIServer) registerAPIGroups(gvks []schema.GroupVersionKind) error {
	// Group GVKs by Group for API group registration
	groupedGVKs := make(map[string][]schema.GroupVersionKind)
	for _, gvk := range gvks {
		groupedGVKs[gvk.Group] = append(groupedGVKs[gvk.Group], gvk)
	}

	// Register each API group
	for group, gvkList := range groupedGVKs {
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

	for version, versionGVKs := range versionedGVKs {
		resourceStorage := make(map[string]apiserverrest.Storage)

		for _, gvk := range versionGVKs {
			// Convert GVK to GVR
			resource, err := s.findAPIResource(gvk)
			if err != nil {
				return fmt.Errorf("failed to complete API discovery for GVK %s: %w", gvk.String(), err)
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

	s.log.V(1).Info("API group registered successfully", "group", group, "versions", groupVersions)

	return nil
}

func validateGVKsForGroup(group string, gvks []schema.GroupVersionKind) error {
	for _, gvk := range gvks {
		if gvk.Group != group {
			return fmt.Errorf("refusing to register unknown API group: %s does not belong to group %s",
				gvk.String(), group)
		}
	}
	return nil
}

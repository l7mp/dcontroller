package apiserver

import (
	"fmt"
	"net"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	openapiendpoints "k8s.io/apiserver/pkg/endpoints/openapi"
	apiserverrest "k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	genericoptions "k8s.io/apiserver/pkg/server/options"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/component-base/compatibility"
	openapicommon "k8s.io/kube-openapi/pkg/common"
	"k8s.io/kube-openapi/pkg/validation/spec"
	generatedopenapi "k8s.io/kubernetes/pkg/generated/openapi"

	viewv1a1 "github.com/l7mp/dcontroller/pkg/api/view/v1alpha1"
)

// buildServer creates a new API server instance with the given GVKs using direct k8s.io/apiserver
func (s *APIServer) buildServer(gvks []schema.GroupVersionKind) (*genericapiserver.GenericAPIServer, error) {
	// Step 1: Create minimal scheme with unstructured types
	scheme, codecs, err := s.createSchemeAndCodecs(gvks)
	if err != nil {
		return nil, fmt.Errorf("failed to create scheme: %w", err)
	}

	// Step 2: Setup server configuration
	config, err := s.createServerConfig(gvks, codecs, scheme)
	if err != nil {
		return nil, fmt.Errorf("failed to create server config: %w", err)
	}

	// Step 3: Create the GenericAPIServer
	server, err := config.Complete().New("dynamic-apiserver", genericapiserver.NewEmptyDelegate())
	if err != nil {
		return nil, fmt.Errorf("failed to create server: %w", err)
	}

	// Step 4: Register API groups for each GVK
	if err := s.registerAPIGroups(server, scheme, gvks); err != nil {
		return nil, fmt.Errorf("failed to register API groups: %w", err)
	}

	s.log.Info("built server successfully", "GVKs", len(gvks))

	return server, nil
}

// createSchemeAndCodecs creates a minimal scheme with unstructured types and codecs
func (s *APIServer) createSchemeAndCodecs(gvks []schema.GroupVersionKind) (*runtime.Scheme, serializer.CodecFactory, error) {
	scheme := runtime.NewScheme()

	// Add client-go scheme for core types
	clientgoscheme.AddToScheme(scheme)

	// Add meta types (required for API machinery)
	metav1.AddToGroupVersion(scheme, schema.GroupVersion{Version: "v1"})

	// Group GVKs by GroupVersion for registration
	groupVersions := make(map[schema.GroupVersion][]schema.GroupVersionKind)
	for _, gvk := range gvks {
		gv := gvk.GroupVersion()
		groupVersions[gv] = append(groupVersions[gv], gvk)
	}

	// Register unstructured types for each GroupVersion
	for gv, gvkList := range groupVersions {
		// Add the GroupVersion itself
		metav1.AddToGroupVersion(scheme, gv)

		// Register unstructured types for each kind in this GroupVersion
		for _, gvk := range gvkList {
			scheme.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
			scheme.AddKnownTypeWithName(listGVK(gvk), &unstructured.UnstructuredList{})
			s.log.V(4).Info("registered unstructured types in scheme", "GVK", gvk.String(),
				"list-GVK", listGVK(gvk))
		}

		// Set version priority for this group
		allVersions := []schema.GroupVersion{gv}
		if err := scheme.SetVersionPriority(allVersions...); err != nil {
			return nil, serializer.CodecFactory{},
				fmt.Errorf("failed to set version priority for %s: %w", gv.String(), err)
		}
	}

	// Create codecs
	codecs := serializer.NewCodecFactory(scheme)

	return scheme, codecs, nil
}

// getOpenAPIDefinitions generates an OpenAPI definitions for the registered GVKs.
func (s *APIServer) getOpenAPIDefinitions(gvks []schema.GroupVersionKind) openapicommon.GetOpenAPIDefinitions {
	return func(ref openapicommon.ReferenceCallback) map[string]openapicommon.OpenAPIDefinition {
		// Add base definitions from Kubernetes for common types like ObjectMeta, ListMeta
		// and common APIs from core/v1 annd apps/v1
		defs := generatedopenapi.GetOpenAPIDefinitions(ref)

		unstructuredDefName := "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured.Unstructured"
		defs[unstructuredDefName] = s.genOpenAPIUnstructDef(ref)

		unstructuredListDefName := "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured.UnstructuredList"
		defs[unstructuredListDefName] = s.genOpenAPIUnstructListDef(ref, unstructuredDefName)

		for _, gvk := range gvks {
			if gvk.Group != viewv1a1.GroupVersion.Group {
				continue
			}

			// resource e.g., "view.dcontroller.io.v1alpha1.MyView"
			defName := fmt.Sprintf("%s.%s.%s", gvk.Group, gvk.Version, gvk.Kind)
			defs[defName] = s.genOpenAPIDef(gvk, ref)

			// resourcelist
			listDefName := fmt.Sprintf("%s/%s.%s", gvk.Group, gvk.Version, listGVK(gvk).Kind)
			defs[listDefName] = s.genOpenAPIListDef(gvk, ref, defName)
		}

		return defs
	}
}

// createServerConfig creates the server configuration
func (s *APIServer) createServerConfig(gvks []schema.GroupVersionKind, codecs serializer.CodecFactory, scheme *runtime.Scheme) (*genericapiserver.RecommendedConfig, error) {
	config := genericapiserver.NewConfig(codecs)

	// Use SecureServingOptions to create the listener
	secureServingOptions := &genericoptions.SecureServingOptions{
		BindAddress: s.bindAddress,
		BindPort:    s.bindPort,
		ServerCert: genericoptions.GeneratableKeyCert{
			// CertDirectory: "/tmp", // For self-signed cert generation
			PairName: "apiserver",
		},
	}

	// Apply secure serving options
	if err := secureServingOptions.ApplyTo(&config.SecureServing); err != nil {
		return nil, fmt.Errorf("failed to apply secure serving: %w", err)
	}

	// Now create loopback config manually based on the secure serving info
	addr := config.SecureServing.Listener.Addr().(*net.TCPAddr)
	config.LoopbackClientConfig = &rest.Config{
		Host: fmt.Sprintf("https://%s:%d", addr.IP.String(), addr.Port),
		TLSClientConfig: rest.TLSClientConfig{
			Insecure: true, // Skip TLS verification for self-signed certs
		},
	}

	// Set other required fields
	config.EffectiveVersion = compatibility.NewEffectiveVersionFromString("1.33", "", "")

	// The openAPINamer uses the scheme to generate definition names.
	openAPINamer := openapiendpoints.NewDefinitionNamer(scheme)

	// Replace `GetOpenAPIDefinitions` with your actual function if it's in a different package/name
	openAPIConfig := genericapiserver.DefaultOpenAPIConfig(s.getOpenAPIDefinitions(gvks), openAPINamer)
	openAPIConfig.Info = &spec.Info{
		InfoProps: spec.InfoProps{
			Title:   "Dynamic API Server for dcontroller",
			Version: "v1.0.0",
			Contact: &spec.ContactInfo{ /* ... */ },
			License: &spec.License{ /* ... */ },
		},
	}
	config.OpenAPIConfig = openAPIConfig

	openAPIV3Config := genericapiserver.DefaultOpenAPIV3Config(s.getOpenAPIDefinitions(gvks), openAPINamer)
	openAPIV3Config.Info = openAPIConfig.Info // Reuse info
	config.OpenAPIV3Config = openAPIV3Config

	return &genericapiserver.RecommendedConfig{Config: *config}, nil
}

// registerAPIGroups registers API groups and resources with the server
func (s *APIServer) registerAPIGroups(server *genericapiserver.GenericAPIServer, scheme *runtime.Scheme, gvks []schema.GroupVersionKind) error {
	// Group GVKs by Group for API group registration
	groupedGVKs := make(map[string][]schema.GroupVersionKind)
	for _, gvk := range gvks {
		groupedGVKs[gvk.Group] = append(groupedGVKs[gvk.Group], gvk)
	}

	// Register each API group
	for group, gvkList := range groupedGVKs {
		if err := s.registerAPIGroup(server, scheme, group, gvkList); err != nil {
			return fmt.Errorf("failed to register API group %s: %w", group, err)
		}
	}

	return nil
}

// registerAPIGroup registers a single API group with its resources
func (s *APIServer) registerAPIGroup(server *genericapiserver.GenericAPIServer, scheme *runtime.Scheme, group string, gvks []schema.GroupVersionKind) error {
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
			storageProvider := NewClientDelegatedStorage(s.delegatingClient, resource, s.log)
			storage, err := storageProvider(scheme, nil) // No RESTOptionsGetter needed for our custom storage
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
		Scheme:                       scheme,
		ParameterCodec:               runtime.NewParameterCodec(scheme),
		NegotiatedSerializer:         serializer.NewCodecFactory(scheme),
	}

	// Install the API group
	if err := server.InstallAPIGroup(apiGroupInfo); err != nil {
		return fmt.Errorf("failed to install API group %s: %w", group, err)
	}

	s.log.V(1).Info("successfully registered API group", "group", group, "versions", groupVersions)

	return nil
}

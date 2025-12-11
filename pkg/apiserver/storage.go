package apiserver

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ rest.StandardStorage = &ClientDelegatedStorage{}
var _ rest.Scoper = &ClientDelegatedStorage{}
var _ rest.TableConvertor = &ClientDelegatedStorage{}

// ClientDelegatedStorage implements REST storage by delegating all operations
// to a controller-runtime client.
type ClientDelegatedStorage struct {
	delegatingClient      client.Client
	delegatingWatcher     client.WithWatch
	gvk                   schema.GroupVersionKind
	gvr                   schema.GroupVersionResource
	namespaced, hasStatus bool
	log                   logr.Logger
}

// Implement the rest.Storage interface.

// New returns a new empty object for this resource.
func (s *ClientDelegatedStorage) New() runtime.Object {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(s.gvk)
	return obj
}

// NewList returns a new empty list object for this resource.
func (s *ClientDelegatedStorage) NewList() runtime.Object {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   s.gvk.Group,
		Version: s.gvk.Version,
		Kind:    s.gvk.Kind + "List",
	})
	return list
}

// Destroy cleans up any resources (no-op for client-delegated storage).
func (s *ClientDelegatedStorage) Destroy() {}

// NamespaceScoped returns true if the resource is namespace-scoped.
func (s *ClientDelegatedStorage) NamespaceScoped() bool {
	return s.namespaced
}

// SingularNameProvider returns singular name of resources.
func (s *ClientDelegatedStorage) GetSingularName() string { return s.gvr.Resource } // same as the plural

// Implement the rest.Getter interface.

// Get retrieves a single object by name.
func (s *ClientDelegatedStorage) Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
	s.log.V(4).Info("GET", "GVR", s.gvr.String(), "name", name)

	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(s.gvk)

	// Extract namespace from context if resource is namespaced
	key := client.ObjectKey{Name: name}
	if s.NamespaceScoped() {
		if ns, ok := genericapirequest.NamespaceFrom(ctx); ok {
			key.Namespace = ns
		} else {
			return nil, apierrors.NewBadRequest("namespace required for namespaced resource")
		}
	}

	if err := s.delegatingClient.Get(ctx, key, obj, &client.GetOptions{Raw: options}); err != nil {
		if client.IgnoreNotFound(err) == nil {
			return nil, apierrors.NewNotFound(s.gvr.GroupResource(), name)
		}
		return nil, apierrors.NewInternalError(fmt.Errorf("failed to get %s: %w", name, err))
	}

	return obj, nil
}

// Implement the rest.Lister interface.

// List retrieves a list of objects.
func (s *ClientDelegatedStorage) List(ctx context.Context, options *metainternalversion.ListOptions) (runtime.Object, error) {
	if ri, ok := genericapirequest.RequestInfoFrom(ctx); ok {
		s.log.V(2).Info("LIST", "GVR", s.gvr.String(), "verb", ri.Verb, "resource", ri.Resource,
			"namespace", ri.Namespace, "label-selector", ri.LabelSelector,
			"field-selector", ri.FieldSelector)
	} else {
		s.log.V(2).Info("LIST", "GVR", s.gvr.String())
	}

	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   s.gvk.Group,
		Version: s.gvk.Version,
		Kind:    s.gvk.Kind + "List",
	})

	// Convert list options
	listOpts := []client.ListOption{}

	// Handle namespace filtering for namespaced resources
	if s.NamespaceScoped() {
		if ns, ok := genericapirequest.NamespaceFrom(ctx); ok && ns != metav1.NamespaceAll {
			listOpts = append(listOpts, client.InNamespace(ns))
		}
	}

	// Handle label selector
	if options != nil && options.LabelSelector != nil {
		listOpts = append(listOpts, client.MatchingLabelsSelector{Selector: options.LabelSelector})
	}

	// Handle field selector
	if options != nil && options.FieldSelector != nil {
		listOpts = append(listOpts, client.MatchingFieldsSelector{Selector: options.FieldSelector})
	}

	if err := s.delegatingClient.List(ctx, list, listOpts...); err != nil {
		return nil, apierrors.NewInternalError(fmt.Errorf("failed to list %s: %w", s.gvr.String(), err))
	}

	return list, nil
}

//nolint:misspell
// Implement the "rest.Creater" interface.

// Create creates a new object.
func (s *ClientDelegatedStorage) Create(ctx context.Context, obj runtime.Object, createValidation rest.ValidateObjectFunc, options *metav1.CreateOptions) (runtime.Object, error) {
	s.log.V(4).Info("CREATE", "GVR", s.gvr.String())

	// Validate the object if validation function is provided.
	if createValidation != nil {
		if err := createValidation(ctx, obj); err != nil {
			return nil, err
		}
	}

	unstructuredObj, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, apierrors.NewBadRequest("object is not unstructured")
	}

	// Ensure GVK is set correctly.
	unstructuredObj.SetGroupVersionKind(s.gvk)

	// Handle namespace for namespaced resources.
	if s.NamespaceScoped() {
		if ns, ok := genericapirequest.NamespaceFrom(ctx); ok {
			unstructuredObj.SetNamespace(ns)
		} else {
			return nil, apierrors.NewBadRequest("namespace required for namespaced resource")
		}
	}

	if err := s.delegatingClient.Create(ctx, unstructuredObj, &client.CreateOptions{Raw: options}); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil, apierrors.NewAlreadyExists(s.gvr.GroupResource(), unstructuredObj.GetName())
		}
		return nil, apierrors.NewInternalError(fmt.Errorf("failed to create %s: %w", unstructuredObj.GetName(), err))
	}

	return unstructuredObj, nil
}

// Implement the rest.Updater interface.

// Update updates an existing object.
func (s *ClientDelegatedStorage) Update(ctx context.Context, name string, objInfo rest.UpdatedObjectInfo, createValidation rest.ValidateObjectFunc, updateValidation rest.ValidateObjectUpdateFunc, forceAllowCreate bool, options *metav1.UpdateOptions) (runtime.Object, bool, error) {
	s.log.V(4).Info("UPDATE", "GVR", s.gvr.String(), "name", name)

	// Get current object.
	currentObj, err := s.Get(ctx, name, &metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) && forceAllowCreate {
			// Create new object
			newObj, err := objInfo.UpdatedObject(ctx, nil)
			if err != nil {
				return nil, false, err
			}
			createdObj, err := s.Create(ctx, newObj, createValidation, &metav1.CreateOptions{})
			return createdObj, true, err
		}
		return nil, false, err
	}

	// Get updated object.
	updatedObj, err := objInfo.UpdatedObject(ctx, currentObj)
	if err != nil {
		return nil, false, err
	}

	// Validate update if validation function is provided.
	if updateValidation != nil {
		if err := updateValidation(ctx, updatedObj, currentObj); err != nil {
			return nil, false, err
		}
	}

	unstructuredObj, ok := updatedObj.(*unstructured.Unstructured)
	if !ok {
		return nil, false, apierrors.NewBadRequest("object is not unstructured")
	}

	// Ensure GVK is set correctly.
	unstructuredObj.SetGroupVersionKind(s.gvk)

	if err := s.delegatingClient.Update(ctx, unstructuredObj, &client.UpdateOptions{Raw: options}); err != nil {
		return nil, false, apierrors.NewInternalError(fmt.Errorf("failed to update %s: %w", name, err))
	}

	return unstructuredObj, false, nil
}

// Implement the rest.GracefulDeleter interface.

// Delete deletes an object.
func (s *ClientDelegatedStorage) Delete(ctx context.Context, name string, deleteValidation rest.ValidateObjectFunc, options *metav1.DeleteOptions) (runtime.Object, bool, error) {
	s.log.V(4).Info("DELETE", "GVR", s.gvr.String(), "name", name)

	// Get the object before deletion.
	obj, err := s.Get(ctx, name, &metav1.GetOptions{})
	if err != nil {
		return nil, false, err
	}

	// Validate deletion if validation function is provided.
	if deleteValidation != nil {
		if err := deleteValidation(ctx, obj); err != nil {
			return nil, false, err
		}
	}

	unstructuredObj := obj.(*unstructured.Unstructured)
	if err := s.delegatingClient.Delete(ctx, unstructuredObj, &client.DeleteOptions{Raw: options}); err != nil {
		if client.IgnoreNotFound(err) == nil {
			return nil, false, apierrors.NewNotFound(s.gvr.GroupResource(), name)
		}
		return nil, false, apierrors.NewInternalError(fmt.Errorf("failed to delete %s: %w", name, err))
	}

	// Return the deleted object.
	return unstructuredObj, true, nil
}

// Implement the rest.CollectionDeleter interface.

// DeleteCollection deletes a collection of objects.
func (s *ClientDelegatedStorage) DeleteCollection(ctx context.Context, deleteValidation rest.ValidateObjectFunc, options *metav1.DeleteOptions, listOptions *metainternalversion.ListOptions) (runtime.Object, error) {
	s.log.V(4).Info("DELETE COLLECTION", "GVR", s.gvr.String())

	// First list all objects to be deleted.
	listObj, err := s.List(ctx, listOptions)
	if err != nil {
		return nil, err
	}

	list := listObj.(*unstructured.UnstructuredList)

	// Delete each object.
	for _, item := range list.Items {
		if deleteValidation != nil {
			if err := deleteValidation(ctx, &item); err != nil {
				continue // Skip this item but continue with others
			}
		}

		if err := s.delegatingClient.Delete(ctx, &item, &client.DeleteOptions{Raw: options}); err != nil {
			s.log.Info("Failed to delete object", "item",
				fmt.Sprintf("%s/%s", item.GetNamespace(), item.GetName()), "error", err.Error())
		}
	}

	return list, nil
}

// Implement the rest.Watcher interface.

// Watch returns a watch interface for the resource.
func (s *ClientDelegatedStorage) Watch(ctx context.Context, options *metainternalversion.ListOptions) (watch.Interface, error) {
	s.log.V(4).Info("WATCH", "GVR", s.gvr.String())

	if s.delegatingWatcher == nil {
		return nil, apierrors.NewServiceUnavailable("watch unavailable")
	}

	// Create an empty list object for the watch.
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   s.gvk.Group,
		Version: s.gvk.Version,
		Kind:    s.gvk.Kind + "List",
	})

	// Convert list options to client options.
	listOpts := []client.ListOption{}

	// Handle namespace filtering for namespaced resources.
	if s.NamespaceScoped() {
		if ns, ok := genericapirequest.NamespaceFrom(ctx); ok && ns != metav1.NamespaceAll {
			listOpts = append(listOpts, client.InNamespace(ns))
		}
	}

	// Handle label selectors.
	if options != nil && options.LabelSelector != nil {
		listOpts = append(listOpts, client.MatchingLabelsSelector{Selector: options.LabelSelector})
	}

	// Handle field selectors.
	if options != nil && options.FieldSelector != nil {
		listOpts = append(listOpts, client.MatchingFieldsSelector{Selector: options.FieldSelector})
	}

	// Try to get the watch from the delegating client.  This assumes the delegating client
	// supports watching.
	watcher, err := s.delegatingWatcher.Watch(ctx, list, listOpts...)
	if err != nil {
		s.log.V(2).Info("failed to create watch", "error", err, "GVR", s.gvr.String())
		return nil, apierrors.NewInternalError(fmt.Errorf("failed to create watch for %s: %w", s.gvr.String(), err))
	}

	s.log.V(2).Info("watch created successfully", "GVR", s.gvr.String())
	return watcher, nil
}

// Implement the rest.TableConvertor interface.

// ConvertToTable converts objects to table format for kubectl output.
func (s *ClientDelegatedStorage) ConvertToTable(ctx context.Context, object runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
	s.log.V(4).Info("CONVERT-TO-TABLE", "GVR", s.gvr.String())

	table := &metav1.Table{
		TypeMeta: metav1.TypeMeta{
			APIVersion: metav1.SchemeGroupVersion.String(),
			Kind:       "Table",
		},
		ColumnDefinitions: s.getColumnDefinitions(),
	}

	// Handle both single objects and lists.
	switch obj := object.(type) {
	case *unstructured.Unstructured:
		row, err := s.objectToTableRow(obj)
		if err != nil {
			return nil, err
		}
		table.Rows = []metav1.TableRow{row}

	case *unstructured.UnstructuredList:
		table.Rows = make([]metav1.TableRow, len(obj.Items))
		for i, item := range obj.Items {
			row, err := s.objectToTableRow(&item)
			if err != nil {
				return nil, err
			}
			table.Rows[i] = row
		}
		// Set resource version from the list.
		table.ResourceVersion = obj.GetResourceVersion()

	default:
		return nil, fmt.Errorf("unsupported object type for table conversion: %T", object)
	}

	return table, nil
}

// getColumnDefinitions returns the column definitions for table output.
// Priority 0 columns are shown by default, priority 1 columns are shown with -o wide.
func (s *ClientDelegatedStorage) getColumnDefinitions() []metav1.TableColumnDefinition {
	columns := []metav1.TableColumnDefinition{
		{
			Name:        "Name",
			Type:        "string",
			Format:      "name",
			Description: "Name of the resource",
			Priority:    0,
		},
		{
			Name:        "Conditions",
			Type:        "string",
			Description: "Status conditions summary",
			Priority:    0,
		},
		{
			Name:        "Labels",
			Type:        "string",
			Description: "Resource labels",
			Priority:    1,
		},
		{
			Name:        "Annotations",
			Type:        "string",
			Description: "Number of annotations",
			Priority:    1,
		},
	}

	// Add namespace column at the beginning for namespaced resources.
	if s.NamespaceScoped() {
		columns = append([]metav1.TableColumnDefinition{
			{
				Name:        "Namespace",
				Type:        "string",
				Description: "Namespace of the resource",
				Priority:    0,
			},
		}, columns...)
	}

	return columns
}

// objectToTableRow converts a single unstructured object to a table row.
func (s *ClientDelegatedStorage) objectToTableRow(obj *unstructured.Unstructured) (metav1.TableRow, error) { //nolint:unparam
	name := obj.GetName()
	namespace := obj.GetNamespace()

	// Extract conditions summary.
	conditions := extractConditionsSummary(obj)

	// Format labels and annotations.
	labels := formatLabels(obj.GetLabels())
	annotations := formatAnnotations(obj.GetAnnotations())

	// Build cells - namespace comes first for namespaced resources.
	var cells []interface{}
	if s.NamespaceScoped() {
		cells = []interface{}{namespace, name, conditions, labels, annotations}
	} else {
		cells = []interface{}{name, conditions, labels, annotations}
	}

	row := metav1.TableRow{
		Cells:  cells,
		Object: runtime.RawExtension{Object: obj},
	}

	return row, nil
}

// extractConditionsSummary extracts and formats status.conditions from an unstructured object.
// It looks for status.conditions as a list of maps with "type" and "status" fields.
// Returns a comma-separated list of "Type:Status" pairs, or "<none>" if no conditions exist.
// Well-known condition types (Ready, Available, Progressing) are prioritized and shown first.
func extractConditionsSummary(obj *unstructured.Unstructured) string {
	// Try to extract status.conditions.
	status, found, err := unstructured.NestedFieldNoCopy(obj.Object, "status", "conditions")
	if !found || err != nil {
		return "<none>"
	}

	// status.conditions should be a slice of maps.
	conditions, ok := status.([]interface{})
	if !ok || len(conditions) == 0 {
		return "<none>"
	}

	// Extract type:status pairs and prioritize well-known types.
	priorityConditions := []string{} // Ready, Available, Progressing, etc.
	otherConditions := []string{}

	// Define well-known condition types in priority order.
	wellKnownTypes := map[string]int{
		"Ready":       1,
		"Available":   2,
		"Progressing": 3,
		"Degraded":    4,
	}

	for _, cond := range conditions {
		condMap, ok := cond.(map[string]interface{})
		if !ok {
			continue
		}

		condType, typeOk := condMap["type"].(string)
		condStatus, statusOk := condMap["status"].(string)
		if typeOk && statusOk {
			pair := fmt.Sprintf("%s:%s", condType, condStatus)
			if priority, isWellKnown := wellKnownTypes[condType]; isWellKnown {
				// Insert in priority order.
				inserted := false
				for i, existing := range priorityConditions {
					existingType := strings.Split(existing, ":")[0]
					if wellKnownTypes[existingType] > priority {
						priorityConditions = append(priorityConditions[:i], append([]string{pair}, priorityConditions[i:]...)...)
						inserted = true
						break
					}
				}
				if !inserted {
					priorityConditions = append(priorityConditions, pair)
				}
			} else {
				otherConditions = append(otherConditions, pair)
			}
		}
	}

	// Combine priority conditions first, then others.
	allPairs := append(priorityConditions, otherConditions...)

	if len(allPairs) == 0 {
		return "<none>"
	}

	// Limit to first 3 conditions to avoid excessive width.
	if len(allPairs) > 3 {
		remaining := len(allPairs) - 3
		pairs := allPairs[:3]
		return fmt.Sprintf("%s +%d more", strings.Join(pairs, ","), remaining)
	}

	return strings.Join(allPairs, ",")
}

// formatLabels formats labels as a comma-separated list of key=value pairs.
// Returns "<none>" if no labels exist.
func formatLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return "<none>"
	}

	// Convert to key=value pairs.
	pairs := make([]string, 0, len(labels))
	for k, v := range labels {
		pairs = append(pairs, fmt.Sprintf("%s=%s", k, v))
	}

	// Limit display to avoid excessive width.
	if len(pairs) > 3 {
		remaining := len(pairs) - 3
		pairs = pairs[:3]
		return fmt.Sprintf("%s +%d more", strings.Join(pairs, ","), remaining)
	}

	return strings.Join(pairs, ",")
}

// formatAnnotations formats annotations count or key annotations.
// Returns the count as "N annotations" or "<none>" if no annotations exist.
func formatAnnotations(annotations map[string]string) string {
	if len(annotations) == 0 {
		return "<none>"
	}

	// Just show the count for annotations as they can be verbose.
	return fmt.Sprintf("%d", len(annotations))
}

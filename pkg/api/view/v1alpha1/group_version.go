package v1alpha1

import (
	"errors"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	GroupSuffix     = "view.dcontroller.io"
	fullGroupSuffix = "." + GroupSuffix
	Version         = "v1alpha1"
)

// Group returns the group for the view objects created by an operator.
func Group(operator string) string {
	return fmt.Sprintf("%s.%s", operator, GroupSuffix)
}

// GroupVersion returns the group-version for the view objects created by an operator.
func GroupVersion(operator string) schema.GroupVersion {
	return schema.GroupVersion{Group: Group(operator), Version: Version}
}

// GroupVersionKind returns the group-version-kind for the view objects created by an operator.
func GroupVersionKind(operator, view string) schema.GroupVersionKind {
	return GroupVersion(operator).WithKind(view)
}

// MapIntoView maps a native object into an operator view resource.
func MapIntoView(operator string, gvk schema.GroupVersionKind) schema.GroupVersionKind {
	// specialcase corev1
	group := gvk.Group
	if group == "" {
		group = "core"
	}
	return schema.GroupVersionKind{
		Group:   fmt.Sprintf("%s.%s.%s", group, gvk.Version, Group(operator)),
		Version: Version,
		Kind:    gvk.Kind,
	}
}

// MapIntoView restores a native object back from an operator view into its native GVK.
func MapFromView(gvk schema.GroupVersionKind) (schema.GroupVersionKind, error) {
	if !IsViewGroup(gvk.Group) {
		return schema.GroupVersionKind{}, errors.New("not a view resource")
	}
	ps := strings.SplitN(gvk.Group, ".", 3)
	if len(ps) != 3 {
		return schema.GroupVersionKind{}, errors.New("invalid view resource")
	}
	// un-specialcase corev1
	if ps[0] == "core" {
		ps[0] = ""
	}

	return schema.GroupVersionKind{
		Group:   ps[0],
		Version: ps[1],
		Kind:    gvk.Kind,
	}, nil
}

// GetOperator returns the operator name for a view resource.
func GetOperator(gvk schema.GroupVersionKind) string {
	s := gvk.Group
	if strings.HasSuffix(s, fullGroupSuffix) {
		prefix := s[:len(s)-len(fullGroupSuffix)]
		ps := strings.Split(prefix, ".")
		return ps[len(ps)-1]
	}
	return ""
}

// IsViewGroup checks whether a group belongs to a view resource.
func IsViewGroup(group string) bool { return strings.HasSuffix(group, GroupSuffix) }

// IsViewGroupVersion checks whether a group-version belongs to a view resource.
func IsViewGroupVersion(gv schema.GroupVersion) bool {
	return IsViewGroup(gv.Group) && gv.Version == Version
}

// IsViewGroupVersion checks whether a group-version-kind belongs to a view resource.
func IsViewKind(gvk schema.GroupVersionKind) bool {
	return IsViewGroupVersion(gvk.GroupVersion())
}

// HasViewGroupVersion checks whether a group-version belongs to a view resource for a particular
// group.
func HasViewGroupVersion(group string, gv schema.GroupVersion) bool {
	return gv.Group == group && gv.Version == Version
}

// HasViewGroupVersionKind checks whether a group-version-kind belongs to a view resource for a
// particular group.
func HasViewGroupVersionKind(group string, gvk schema.GroupVersionKind) bool {
	return HasViewGroupVersion(group, gvk.GroupVersion())
}

// // Scheme
// func AddGroupToScheme(operator string, s *runtime.Scheme) error {
// 	schemeBuilder := &scheme.Builder{GroupVersion: GroupVersion(operator)}
// 	return schemeBuilder.AddToScheme(s)
// }

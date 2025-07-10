package v1alpha1

import (
	"errors"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

const (
	GroupSuffix     = "view.dcontroller.io"
	fullGroupSuffix = "." + GroupSuffix
	Version         = "v1alpha1"
)

// Constuctors
func Group(operator string) string {
	return fmt.Sprintf("%s.%s", operator, GroupSuffix)
}

func GroupVersion(operator string) schema.GroupVersion {
	return schema.GroupVersion{Group: Group(operator), Version: Version}
}

func GroupVersionKind(operator, view string) schema.GroupVersionKind {
	return GroupVersion(operator).WithKind(view)
}

// Mappers for native objects
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

// Getters
func GetOperator(gvk schema.GroupVersionKind) string {
	s := gvk.Group
	if strings.HasSuffix(s, fullGroupSuffix) {
		prefix := s[:len(s)-len(fullGroupSuffix)]
		ps := strings.Split(prefix, ".")
		return ps[len(ps)-1]
	}
	return ""
}

// Checkers
func IsViewGroup(group string) bool { return strings.HasSuffix(group, GroupSuffix) }

func IsViewGroupVersion(gv schema.GroupVersion) bool {
	return IsViewGroup(gv.Group) && gv.Version == Version
}

func IsViewKind(gvk schema.GroupVersionKind) bool {
	return IsViewGroupVersion(gvk.GroupVersion())
}

// Scheme
func AddGroupToScheme(operator string, s *runtime.Scheme) error {
	schemeBuilder := &scheme.Builder{GroupVersion: GroupVersion(operator)}
	return schemeBuilder.AddToScheme(s)
}

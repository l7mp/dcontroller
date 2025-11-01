package v1alpha1

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	// GroupVersion is group version used to register operators.
	GroupVersion = schema.GroupVersion{Group: "dcontroller.io", Version: "v1alpha1"}

	// SchemeBuilder is used to add go types to the GroupVersionKind scheme.
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}

	// AddToScheme adds the types in this group-version to the given scheme.
	AddToScheme = SchemeBuilder.AddToScheme

	VirtualSourceGroupPrefix = "virtual-source"
)

var (
	// OneShotSourceGroupVersion is group-version for a virtual operator that triggers exactly once when the
	// controller starts with injecting an empty object into the parent operator's view cache.
	OneShotSourceGroupVersion = makeVirtualSourceGV("oneshot")

	// PeriodicSourceGroupVersion is the group for a virtual operator that triggers periodically based
	// on a timer with injecting an empty object into the parent operator's view cache.
	PeriodicSourceGroupVersion = makeVirtualSourceGV("periodic")
)

func makeVirtualSourceGV(groupName string) schema.GroupVersion {
	return schema.GroupVersion{
		Group:   fmt.Sprintf("%s.%s.%s", groupName, VirtualSourceGroupPrefix, GroupVersion.Group),
		Version: GroupVersion.Version,
	}
}

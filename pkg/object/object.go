package object

import "sigs.k8s.io/controller-runtime/pkg/client"

type UnstructuredContent = map[string]any

type Unstructured interface {
	// UnstructuredContent returns a non-nil map with this object's contents. Values may be
	// []any, map[string]any, or any primitive type. Contents are typically serialized to and
	// from JSON. SetUnstructuredContent should be used to mutate the contents.
	UnstructuredContent() UnstructuredContent
	// SetUnstructuredContent updates the object content to match the provided map.
	SetUnstructuredContent(UnstructuredContent)
}

type DeepComparable interface {
	DeepEqual(DeepComparable) bool
}

type Object interface {
	client.Object
	Unstructured
}

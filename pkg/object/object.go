package object

import (
	"k8s.io/apimachinery/pkg/api/equality"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type UnstructuredContent = map[string]any

type Unstructured interface {
	// UnstructuredContent returns a non-nil map with this object's contents. Values may be
	// []any, map[string]any, or any primitive type. Contents are typically serialized to and
	// from JSON. SetUnstructuredContent should be used to mutate the contents.
	UnstructuredContent() UnstructuredContent
	// SetUnstructuredContent updates the object content to match the provided map.
	SetUnstructuredContent(UnstructuredContent)
}

type Object interface {
	client.Object
	Unstructured
}

func DeepEqual(a, b Object) bool {
	return equality.Semantic.DeepEqual(a, b)
}

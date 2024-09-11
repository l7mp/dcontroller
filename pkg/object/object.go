package object

import (
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
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

// Object is a general resource, can be either an unstructured.Unstructured or a ViewObject.
type Object interface {
	client.Object
	Unstructured
}

func DeepEqual(a, b Object) bool {
	return equality.Semantic.DeepEqual(a, b)
}

func DeepCopyInto(in, out Object) {
	if in == nil || out == nil {
		return
	}
	out.SetUnstructuredContent(runtime.DeepCopyJSON(in.UnstructuredContent()))
	out.GetObjectKind().SetGroupVersionKind(in.GetObjectKind().GroupVersionKind())
	return
}

func DeepCopy(in Object) Object {
	if in == nil {
		return nil
	}

	switch in.(type) {
	case *ViewObject:
		out := new(ViewObject)
		DeepCopyInto(in.(*ViewObject), out)
		return out
	case *unstructured.Unstructured:
		out := new(unstructured.Unstructured)
		DeepCopyInto(in.(*unstructured.Unstructured), out)
		return out
	}

	return nil
}

type ObjectList interface {
	client.ObjectList
}

func AppendToListItem(list ObjectList, obj Object) {
	lu, okl := list.(*unstructured.UnstructuredList)
	if ou, oko := obj.(*unstructured.Unstructured); okl && oko {
		lu.Items = append(lu.Items, *ou)
		return
	}

	lv, okl := list.(*ViewObjectList)
	if ov, oko := obj.(*ViewObject); okl && oko {
		lv.Items = append(lv.Items, *ov)
		return
	}
}

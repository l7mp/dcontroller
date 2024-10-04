package util

import (
	"fmt"

	"k8s.io/apimachinery/pkg/util/json"
)

// Map implements a functional map with the signature: (a -> b) -> [a] -> [b].
func Map[T, U any](f func(T) U, s []T) []U {
	result := make([]U, len(s))
	for i, v := range s {
		result[i] = f(v)
	}
	return result
}

// Stringify returns a JSON encoded string representation of an arbitrary input.
func Stringify(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%#v", v)
	}
	return string(b)
}

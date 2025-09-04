// Package predicate provides serializable predicate support for filtering Kubernetes resources in
// Δ-controller source specifications.
//
// Predicates allow controllers to selectively watch and process resource changes that match
// specific criteria, improving performance and reducing unnecessary reconciliation work.
//
// Key components:
//   - Predicate: Serializable predicate interface.
//   - BasicPredicate: Simple predicate types (GenerationChanged, etc.).
//   - BoolPredicate: Boolean logic combinators (And, Or, Not).
//   - Interface: Conversion to controller-runtime predicates.
//
// Supported predicate types:
//   - GenerationChanged: Triggers on metadata.generation changes.
//   - ResourceVersionChanged: Triggers on any resource changes.
//   - LabelChanged: Triggers on label modifications.
//   - AnnotationChanged: Triggers on annotation modifications.
//
// Boolean combinators allow complex predicate logic:
//   - And: All predicates must be true.
//   - Or: Any predicate must be true.
//   - Not: Inverts predicate result.
//
// Predicates are JSON-serializable and can be embedded in Operator
// specifications to control source resource filtering behavior.
//
// Example usage:
//
//	predicate := Predicate{
//	    BoolPredicate: BoolPredicate{
//	        "And": []Predicate{
//	            {BasicPredicate: &BasicPredicate("GenerationChanged")},
//	            {BasicPredicate: &BasicPredicate("LabelChanged")},
//	        },
//	    },
//	}
package predicate

import (
	encodingjson "encoding/json"
	"errors"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/json"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

var _ Interface = &Predicate{}
var _ encodingjson.Marshaler = &Predicate{}
var _ encodingjson.Unmarshaler = &Predicate{}

// Interface is the general interface for predicates.
type Interface interface {
	// ToPredicate converts a serialized predicate into a native controller runtime predicate.
	ToPredicate() (predicate.TypedPredicate[client.Object], error)
}

// FromPredicate converts a serialized Predicate into a native controller runtime predicate.
func FromPredicate(predicate Predicate) (predicate.TypedPredicate[client.Object], error) {
	return predicate.ToPredicate()
}

// FromLabelSelector creates converts a seriaized label selector into a native controller runtime
// label selector predicate.
func FromLabelSelector(labelSelector metav1.LabelSelector) (predicate.TypedPredicate[client.Object], error) {
	return predicate.LabelSelectorPredicate(labelSelector)
}

// FromNamespace creates a namespace selector predicate from a namespace.
func FromNamespace(namespace string) predicate.TypedPredicate[client.Object] {
	return predicate.NewPredicateFuncs(func(object client.Object) bool {
		return object.GetNamespace() == namespace
	})
}

// BasicPredicate represents an elemental predicate, namely one of "GenerationChanged",
// "ResourceVersionChanged", "LabelChanged" or "AnnotationChanged".
type BasicPredicate string

// BoolPredicate is a complex predicate composed of basic predicates and other bool predicates.
type BoolPredicate map[string]([]Predicate)

// Predicate is the top level representation of a predicate.
type Predicate struct {
	*BasicPredicate `json:",inline"`
	*BoolPredicate  `json:",inline"`
}

// ToPredicate converts a serialized predicate into a native controller runtime predicate.
func (p *Predicate) ToPredicate() (predicate.TypedPredicate[client.Object], error) {
	if p.BasicPredicate != nil {
		return p.BasicPredicate.ToPredicate()
	}
	if p.BoolPredicate != nil {
		return p.BoolPredicate.ToPredicate()
	}
	return nil, errors.New("invalid predicate")
}

// MarshalJSON encodes a predicate in JSON format.
func (p Predicate) MarshalJSON() ([]byte, error) {
	if p.BasicPredicate != nil {
		return json.Marshal(p.BasicPredicate)
	}
	if p.BoolPredicate != nil {
		return json.Marshal(p.BoolPredicate)
	}
	return nil, errors.New("invalid predicate")
}

// UnmarshalJSON decodes a predicate from JSON format.
func (p *Predicate) UnmarshalJSON(data []byte) error {
	// try as a simple pred
	var pw BasicPredicate
	err := json.Unmarshal(data, &pw)
	if err == nil {
		*p = Predicate{BasicPredicate: &pw}
		return nil
	}

	// try as bool
	var bp BoolPredicate
	err = json.Unmarshal(data, &bp)
	if err == nil {
		*p = Predicate{BoolPredicate: &bp}
		return nil
	}

	return err
}

// ToPredicate implements ToPredicate() for basic predicates.
func (pw *BasicPredicate) ToPredicate() (predicate.TypedPredicate[client.Object], error) {
	switch string(*pw) {
	case "GenerationChanged":
		return predicate.GenerationChangedPredicate{}, nil
	case "ResourceVersionChanged":
		return predicate.ResourceVersionChangedPredicate{}, nil
	case "LabelChanged":
		return predicate.LabelChangedPredicate{}, nil
	case "AnnotationChanged":
		return predicate.AnnotationChangedPredicate{}, nil
	default:
		return nil, fmt.Errorf("unknown predicate type: %s", *pw)
	}
}

// ToPredicate implements ToPredicate() for bool predicates.
func (cp *BoolPredicate) ToPredicate() (predicate.TypedPredicate[client.Object], error) {
	if len(*cp) != 1 {
		return nil, errors.New("expecting a single predicate op")
	}

	for k, v := range *cp {
		predicates := make([]predicate.Predicate, len(v))
		for i, pw := range v {
			p, err := pw.ToPredicate()
			if err != nil {
				return nil, err
			}
			predicates[i] = p
		}

		switch k {
		case "And":
			return predicate.And(predicates...), nil
		case "Or":
			return predicate.Or(predicates...), nil
		case "Not":
			if len(predicates) != 1 {
				return nil, errors.New("invalid arguments to Not predicate")
			}
			return predicate.Not(predicates[0]), nil
		default:
			return nil, fmt.Errorf("unknown bool predicate type: %s", k)
		}
	}

	return nil, errors.New("invalid bool predicate")
}

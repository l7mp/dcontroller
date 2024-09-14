package view

import (
	"encoding/json"
	"errors"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

var _ Interface = &Predicate{}

// var _ json.Marshaler = &Predicate{}
// var _ json.Unmarshaler = &Predicate{}

type Interface interface {
	ToPredicate() (predicate.Predicate, error)
}

type Predicate struct {
	basic *BasicPredicate
	boolp *BoolPredicate
}

func (p *Predicate) ToPredicate() (predicate.Predicate, error) {
	if p.basic != nil {
		return p.basic.ToPredicate()
	}
	if p.boolp != nil {
		return p.boolp.ToPredicate()
	}
	return nil, errors.New("invalid predicate")
}

func (p Predicate) MarshalJSON() ([]byte, error) {
	if p.basic != nil {
		return json.Marshal(p.basic)
	}
	if p.boolp != nil {
		return json.Marshal(p.boolp)
	}
	return nil, errors.New("invalid predicate")
}

func (p *Predicate) UnmarshalJSON(data []byte) error {
	// try as a simple pred
	var pw BasicPredicate
	err := json.Unmarshal(data, &pw)
	if err == nil {
		*p = Predicate{basic: &pw}
		return nil
	}

	// try as bool
	var bp BoolPredicate
	err = json.Unmarshal(data, &bp)
	if err == nil {
		*p = Predicate{boolp: &bp}
		return nil
	}

	return err
}

//////////////

type BasicPredicate struct {
	Type string `json:"type"`
}

type BoolPredicate struct {
	Type       string           `json:"type"`
	Predicates []BasicPredicate `json:"predicates"`
}

func (pw *BasicPredicate) ToPredicate() (predicate.Predicate, error) {
	switch pw.Type {
	case "GenerationChanged":
		return predicate.GenerationChangedPredicate{}, nil
	case "ResourceVersionChanged":
		return predicate.ResourceVersionChangedPredicate{}, nil
	case "LabelChanged":
		return predicate.LabelChangedPredicate{}, nil
	case "AnnotationChanged":
		return predicate.AnnotationChangedPredicate{}, nil
	default:
		return nil, fmt.Errorf("unknown predicate type: %s", pw.Type)
	}
}

func (cp *BoolPredicate) ToPredicate() (predicate.Predicate, error) {
	predicates := make([]predicate.Predicate, len(cp.Predicates))
	for i, pw := range cp.Predicates {
		p, err := pw.ToPredicate()
		if err != nil {
			return nil, err
		}
		predicates[i] = p
	}

	switch cp.Type {
	case "And":
		return predicate.And(predicates...), nil
	case "Or":
		return predicate.Or(predicates...), nil
	default:
		return nil, fmt.Errorf("unknown composite predicate type: %s", cp.Type)
	}
}

func MarshalPredicate(p predicate.Predicate) ([]byte, error) {
	var pw BasicPredicate

	switch p.(type) {
	case predicate.GenerationChangedPredicate:
		pw = BasicPredicate{Type: "GenerationChanged"}
	case predicate.ResourceVersionChangedPredicate:
		pw = BasicPredicate{Type: "ResourceVersionChanged"}
	case predicate.LabelChangedPredicate:
		pw = BasicPredicate{Type: "LabelChanged"}
	case predicate.AnnotationChangedPredicate:
		pw = BasicPredicate{Type: "AnnotationChanged"}
	default:
		return nil, fmt.Errorf("unsupported predicate type: %T", p)
	}

	return json.Marshal(pw)
}

func UnmarshalPredicate(data []byte) (predicate.Predicate, error) {
	var pw BasicPredicate
	err := json.Unmarshal(data, &pw)
	if err != nil {
		return nil, err
	}
	return pw.ToPredicate()
}

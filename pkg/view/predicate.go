package view

import (
	"encoding/json"
	"errors"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/client"
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
	Type    *string `json:"type"`
	TypeArg *map[string]string
}

type BoolPredicate struct {
	Type       string           `json:"type"`
	Predicates []BasicPredicate `json:"predicates"`
}

func (pw *BasicPredicate) ToPredicate() (predicate.Predicate, error) {
	if pw.Type != nil {
		switch *pw.Type {
		case "GenerationChanged":
			return predicate.GenerationChangedPredicate{}, nil
		case "ResourceVersionChanged":
			return predicate.ResourceVersionChangedPredicate{}, nil
		case "LabelChanged":
			return predicate.LabelChangedPredicate{}, nil
		case "AnnotationChanged":
			return predicate.AnnotationChangedPredicate{}, nil
		default:
			return nil, fmt.Errorf("unknown predicate type: %s", *pw.Type)
		}
	}

	if pw.TypeArg != nil {
		if len(*pw.TypeArg) != 1 {
			return nil, errors.New("expecting a single type-argument pair")
		}

		for k, v := range *pw.TypeArg {
			switch k {
			case "Namespace":
				return predicate.NewPredicateFuncs(func(object client.Object) bool {
					return object.GetNamespace() == v
				}), nil
			default:
				return nil, fmt.Errorf("unknown predicate type: %s", k)
			}
		}
	}

	return nil, errors.New("invalid basic predicate")
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
	case "Not":
		if len(predicates) != 1 {
			return nil, errors.New("invalid arguments to Not predicate")
		}
		return predicate.Not(predicates[0]), nil
	default:
		return nil, fmt.Errorf("unknown bool predicate type: %s", cp.Type)
	}
}

// these to are useless but simplify testing
func MarshalBasicPredicate(p predicate.Predicate) ([]byte, error) {
	var pw BasicPredicate

	switch p.(type) {
	case predicate.GenerationChangedPredicate:
		t := "GenerationChanged"
		pw = BasicPredicate{Type: &t}
	case predicate.ResourceVersionChangedPredicate:
		t := "ResourceVersionChanged"
		pw = BasicPredicate{Type: &t}
	case predicate.LabelChangedPredicate:
		t := "LabelChanged"
		pw = BasicPredicate{Type: &t}
	case predicate.AnnotationChangedPredicate:
		t := "AnnotationChanged"
		pw = BasicPredicate{Type: &t}
	case predicate.Funcs:
		return nil, errors.New("cannot parse predicate functions")
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

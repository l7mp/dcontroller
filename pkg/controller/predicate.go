package controller

import (
	encodingjson "encoding/json"
	"errors"
	"fmt"

	"k8s.io/apimachinery/pkg/util/json"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

var _ Interface = &Predicate{}
var _ encodingjson.Marshaler = &Predicate{}
var _ encodingjson.Unmarshaler = &Predicate{}

type Interface interface {
	ToPredicate() (predicate.TypedPredicate[client.Object], error)
}

type BasicPredicate string
type BoolPredicate map[string]([]Predicate)

type Predicate struct {
	*BasicPredicate
	*BoolPredicate
}

func (p *Predicate) ToPredicate() (predicate.TypedPredicate[client.Object], error) {
	if p.BasicPredicate != nil {
		return p.BasicPredicate.ToPredicate()
	}
	if p.BoolPredicate != nil {
		return p.BoolPredicate.ToPredicate()
	}
	return nil, errors.New("invalid predicate")
}

func (p Predicate) MarshalJSON() ([]byte, error) {
	if p.BasicPredicate != nil {
		return json.Marshal(p.BasicPredicate)
	}
	if p.BoolPredicate != nil {
		return json.Marshal(p.BoolPredicate)
	}
	return nil, errors.New("invalid predicate")
}

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

// // these to are useless but simplify testing
// func MarshalBasicPredicate(p predicate.Predicate) ([]byte, error) {
// 	var pw BasicPredicate

// 	switch p.(type) {
// 	case predicate.GenerationChangedPredicate:
// 		t := PredType("GenerationChanged")
// 		pw = BasicPredicate{PredType: &t}
// 	case predicate.ResourceVersionChangedPredicate:
// 		t := PredType("ResourceVersionChanged")
// 		pw = BasicPredicate{PredType: &t}
// 	case predicate.LabelChangedPredicate:
// 		t := PredType("LabelChanged")
// 		pw = BasicPredicate{PredType: &t}
// 	case predicate.AnnotationChangedPredicate:
// 		t := PredType("AnnotationChanged")
// 		pw = BasicPredicate{PredType: &t}
// 	case predicate.Funcs:
// 		return nil, errors.New("cannot parse predicate functions")
// 	default:
// 		return nil, fmt.Errorf("unsupported predicate type: %T", p)
// 	}

// 	return json.Marshal(pw)
// }

// func UnmarshalPredicate(data []byte) (predicate.Predicate, error) {
// 	var pw BasicPredicate
// 	err := json.Unmarshal(data, &pw)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return pw.ToPredicate()
// }

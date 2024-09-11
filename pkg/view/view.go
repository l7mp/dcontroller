package view

import (
	"encoding/json"
	"errors"
	"fmt"
)

type View struct {
	*BaseView
}

type GroupKind struct {
	ApiGroup string `json:"apiGroup"`
	Kind     string `json:"kind"`
}

type ViewType int

const (
	ViewTypeUnknown = iota
	ViewTypeBaseView
	ViewTypeView
)

func NewViewType(s string) ViewType {
	switch s {
	case "BaseView":
		return ViewTypeBaseView
	case "View":
		return ViewTypeView
	default:
		return ViewTypeUnknown
	}
}

func (v ViewType) String() string {
	switch v {
	case ViewTypeBaseView:
		return "BaseView"
	case ViewTypeView:
		return "View"
	default:
		return "<unknown>"
	}
}

type ViewTypeDiscriminiator struct {
	Type string `json:"type"`
}

func (v *View) UnmarshalJSON(b []byte) error {
	// try to unmarshal as a type discriminator
	var td ViewTypeDiscriminiator
	if err := json.Unmarshal(b, &td); err != nil {
		return fmt.Errorf("could not parse view: no view type: %w",
			string(b), err)
	}

	switch NewViewType(td.Type) {
	case ViewTypeBaseView:
		var bv BaseView
		if err := json.Unmarshal(b, &bv); err != nil {
			return fmt.Errorf("could not parse base view: %w", err)
		}
		*v = View{BaseView: &bv}
		return nil

	// case ViewTypeView:
	default:
		return errors.New("could not parse view: unknown type")
	}
}

package predicate

import "k8s.io/apimachinery/pkg/util/json"

func (p Predicate) DeepCopyInto(d *Predicate) {
	j, err := json.Marshal(p)
	if err != nil {
		d = nil
		return
	}

	if err := json.Unmarshal(j, d); err != nil {
		d = nil
		return
	}
}
func (in *Predicate) DeepCopy() *Predicate {
	if in == nil {
		return nil
	}
	out := new(Predicate)
	in.DeepCopyInto(out)
	return out
}

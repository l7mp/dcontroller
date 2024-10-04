package predicate

import "k8s.io/apimachinery/pkg/util/json"

// DeepCopyInto copies a predicate into target predicate.
func (p Predicate) DeepCopyInto(d *Predicate) {
	j, err := json.Marshal(p)
	if err != nil {
		return
	}

	if err := json.Unmarshal(j, d); err != nil {
		return
	}
}

// DeepCopy copies a predicate.
func (p *Predicate) DeepCopy() *Predicate {
	if p == nil {
		return nil
	}
	out := new(Predicate)
	p.DeepCopyInto(out)
	return out
}

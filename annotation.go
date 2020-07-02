package influxdb

import (
	"encoding/json"
	"sort"
)

// not exporting this so users are unable to create annotations.
// this can change but for now want to keep this under lock and key.
type annotation int

const (
	annotationUnknown annotation = iota
	annotationStackOwner
	annotationStackReference
)

type Annotations struct {
	// Hide the internals so it can't be manipulated freely.
	m map[annotation]interface{}
}

func (a Annotations) MarshalJSON() ([]byte, error) {
	return json.Marshal(a.m)
}

func (a *Annotations) UnmarshalJSON(b []byte) error {
	return json.Unmarshal(b, &a.m)
}

func (a *Annotations) Stacks() struct {
	Owner      ID
	References []ID
} {
	var out struct {
		Owner      ID
		References []ID
	}
	if a == nil {
		return out
	}

	ownerIDRaw, ok := a.m[annotationStackOwner].(string)
	if ok {
		if id, err := IDFromString(ownerIDRaw); err == nil {
			out.Owner = *id
		}
	}

	refs, _ := a.m[annotationStackReference].([]interface{})
	for _, ref := range refs {
		refIDRaw, ok := ref.(string)
		if ok {
			if id, err := IDFromString(refIDRaw); err == nil {
				out.References = append(out.References, *id)
			}
		}
	}

	return out
}

type AnnotationSetFn func(a *Annotations)

func (a *Annotations) Set(setFn AnnotationSetFn) {
	if a.m == nil {
		a.m = make(map[annotation]interface{})
	}
	setFn(a)
}

func (a *Annotations) Clone() Annotations {
	m := make(map[annotation]interface{})
	for k, v := range a.m {
		m[k] = v
	}
	return Annotations{m: m}
}

func AnnotationSetStack(stackID ID) AnnotationSetFn {
	return func(a *Annotations) {
		stAnnot := a.Stacks()
		if stackID == stAnnot.Owner {
			return
		}
		if stAnnot.Owner == 0 {
			AnnotationSetStackOwner(stackID)(a)
			return
		}
		AnnotationSetStackReference(stackID)(a)
	}
}

func AnnotationSetStackOwner(stackID ID) AnnotationSetFn {
	return func(a *Annotations) {
		a.m[annotationStackOwner] = stackID.String()
	}
}

func AnnotationSetStackReference(stackID ID) AnnotationSetFn {
	return func(a *Annotations) {
		refs, _ := a.m[annotationStackReference].([]interface{})

		m := map[string]bool{
			stackID.String(): true,
		}
		for _, refRaw := range refs {
			if id, ok := refRaw.(string); ok {
				m[id] = true
			}
		}

		var newRefs []interface{}
		for ref := range m {
			newRefs = append(newRefs, ref)
		}
		sort.Slice(newRefs, func(i, j int) bool {
			return newRefs[i].(string) < newRefs[j].(string)
		})
		a.m[annotationStackReference] = newRefs
	}
}

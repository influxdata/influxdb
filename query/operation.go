package query

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
)

// Operation denotes a single operation in a query.
type Operation struct {
	ID   OperationID   `json:"id"`
	Spec OperationSpec `json:"spec"`
}

func (o *Operation) UnmarshalJSON(data []byte) error {
	type Alias Operation
	raw := struct {
		*Alias
		Kind OperationKind   `json:"kind"`
		Spec json.RawMessage `json:"spec"`
	}{}
	err := json.Unmarshal(data, &raw)
	if err != nil {
		return err
	}
	if raw.Alias != nil {
		*o = *(*Operation)(raw.Alias)
	}
	spec, err := unmarshalOpSpec(raw.Kind, raw.Spec)
	if err != nil {
		return errors.Wrapf(err, "failed to unmarshal operation %q", o.ID)
	}
	o.Spec = spec
	return nil
}

func unmarshalOpSpec(k OperationKind, data []byte) (OperationSpec, error) {
	createOpSpec, ok := kindToOp[k]
	if !ok {
		return nil, fmt.Errorf("unknown operation spec kind %v", k)
	}
	spec := createOpSpec()

	if len(data) > 0 {
		err := json.Unmarshal(data, spec)
		if err != nil {
			return nil, err
		}
	}
	return spec, nil
}

func (o Operation) MarshalJSON() ([]byte, error) {
	type Alias Operation
	raw := struct {
		Kind OperationKind `json:"kind"`
		Alias
	}{
		Kind:  o.Spec.Kind(),
		Alias: (Alias)(o),
	}
	return json.Marshal(raw)
}

type NewOperationSpec func() OperationSpec

// OperationSpec specifies an operation as part of a query.
type OperationSpec interface {
	// Kind returns the kind of the operation.
	Kind() OperationKind
}

// OperationID is a unique ID within a query for the operation.
type OperationID string

// OperationKind denotes the kind of operations.
type OperationKind string

var kindToOp = make(map[OperationKind]NewOperationSpec)

// RegisterOpSpec registers an operation spec with a given kind.
// k is a label that uniquely identifies this operation. If the kind has already been registered the call panics.
// c is a function reference that creates a new, default-initialized opSpec for the given kind.
// TODO:(nathanielc) make this part of RegisterMethod/RegisterFunction
func RegisterOpSpec(k OperationKind, c NewOperationSpec) {
	if kindToOp[k] != nil {
		panic(fmt.Errorf("duplicate registration for operation kind %v", k))
	}
	kindToOp[k] = c
}

func NumberOfOperations() int {
	return len(kindToOp)
}

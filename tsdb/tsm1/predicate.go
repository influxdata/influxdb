package tsm1

import "errors"

// Predicate is something that can match on a series key.
type Predicate interface {
	Matches(key []byte) bool
	Marshal() ([]byte, error)
}

func UnmarshalPredicate(data []byte) (Predicate, error) {
	if data != nil {
		return nil, errors.New("unimplemented")
	}
	return nil, nil
}

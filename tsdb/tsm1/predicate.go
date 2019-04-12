package tsm1

// Predicate is something that can match on a series key.
type Predicate interface {
	Matches(key []byte) bool
	Marshal() ([]byte, error)
}

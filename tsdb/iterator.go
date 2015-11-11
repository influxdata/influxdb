package tsdb

import "time"

// Iterator represents a generic interface for all Iterators.
// Most iterator operations are done on the typed sub-interfaces.
type Iterator interface {
	Close() error
}

// Iterators represents a list of iterators.
type Iterators []Iterator

// Close closes all iterators.
func (a Iterators) Close() error {
	for _, itr := range a {
		itr.Close()
	}
	return nil
}

// IteratorCreator represents an interface for objects that can create Iterators.
type IteratorCreator interface {
	CreateIterator(name string, start, end time.Time) (Iterator, error)
}

package estimator

import "encoding"

// Sketch is the interface representing a sketch for estimating cardinality.
type Sketch interface {
	// Add adds a single value to the sketch.
	Add(v []byte)

	// Count returns a cardinality estimate for the sketch.
	Count() uint64

	// Merge merges another sketch into this one.
	Merge(s Sketch) error

	// Bytes estimates the memory footprint of the sketch, in bytes.
	Bytes() int

	// Clone returns a deep copy of the sketch.
	Clone() Sketch

	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}

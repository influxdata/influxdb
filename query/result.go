package query

import (
	"io"

	"github.com/influxdata/platform/query/values"
)

type Result interface {
	Name() string
	// Blocks returns a BlockIterator for iterating through results
	Blocks() BlockIterator
}

type BlockIterator interface {
	Do(f func(Block) error) error
}

type Block interface {
	Key() PartitionKey

	Cols() []ColMeta

	// Do calls f to process the data contained within the block.
	// The function f will be called zero or more times.
	Do(f func(ColReader) error) error

	// RefCount modifies the reference count on the block by n.
	// When the RefCount goes to zero, the block is freed.
	RefCount(n int)

	// Empty returns whether the block contains no records.
	Empty() bool
}

type ColMeta struct {
	Label string
	Type  DataType
}

type DataType int

const (
	TInvalid DataType = iota
	TBool
	TInt
	TUInt
	TFloat
	TString
	TTime
)

func (t DataType) String() string {
	switch t {
	case TInvalid:
		return "invalid"
	case TBool:
		return "bool"
	case TInt:
		return "int"
	case TUInt:
		return "uint"
	case TFloat:
		return "float"
	case TString:
		return "string"
	case TTime:
		return "time"
	default:
		return "unknown"
	}
}

// ColReader allows access to reading slices of column data.
// All data the ColReader exposes is guaranteed to be in memory.
// Once a ColReader goes out of scope all slices are considered invalid.
type ColReader interface {
	Key() PartitionKey
	// Cols returns a list of column metadata.
	Cols() []ColMeta
	// Len returns the length of the slices.
	// All slices will have the same length.
	Len() int
	Bools(j int) []bool
	Ints(j int) []int64
	UInts(j int) []uint64
	Floats(j int) []float64
	Strings(j int) []string
	Times(j int) []values.Time
}

type PartitionKey interface {
	Cols() []ColMeta

	HasCol(label string) bool

	ValueBool(j int) bool
	ValueUInt(j int) uint64
	ValueInt(j int) int64
	ValueFloat(j int) float64
	ValueString(j int) string
	ValueDuration(j int) values.Duration
	ValueTime(j int) values.Time
	Value(j int) values.Value

	Hash() uint64
	Equal(o PartitionKey) bool
	Less(o PartitionKey) bool
	String() string
}

// ResultDecoder can decode a result from a reader.
type ResultDecoder interface {
	// Decode decodes data from r into a result.
	Decode(r io.Reader) (Result, error)
}

// ResultEncoder can encode a result into a writer.
type ResultEncoder interface {
	// Encode encodes data from the result into w.
	Encode(w io.Writer, result Result) error
}

// MultiResultDecoder can decode multiple results from a reader.
type MultiResultDecoder interface {
	// Decode decodes multiple results from r.
	Decode(r io.ReadCloser) (ResultIterator, error)
}

// MultiResultEncoder can encode multiple results into a writer.
type MultiResultEncoder interface {
	// Encode writes multiple results from r into w.
	Encode(w io.Writer, results ResultIterator) error
}

// DelimitedMultiResultEncoder encodes multiple results using a trailing delimiter.
// The delimiter is written after every result.
//
// If an error is encountered when iterating EncodeError will be called on the Encoder.
//
// If the io.Writer implements flusher, it will be flushed after each delimiter.
type DelimitedMultiResultEncoder struct {
	Delimiter []byte
	Encoder   interface {
		ResultEncoder
		// EncodeError encodes an error on the writer.
		EncodeError(w io.Writer, err error) error
	}
}

type flusher interface {
	Flush()
}

func (e *DelimitedMultiResultEncoder) Encode(w io.Writer, results ResultIterator) error {
	for results.More() {
		result := results.Next()
		if err := e.Encoder.Encode(w, result); err != nil {
			return err
		}
		if _, err := w.Write(e.Delimiter); err != nil {
			return err
		}
		// Flush the writer after each result
		if f, ok := w.(flusher); ok {
			f.Flush()
		}
	}
	err := results.Err()
	if err != nil {
		return e.Encoder.EncodeError(w, err)
	}
	return nil
}

package query

import (
	"io"

	"github.com/influxdata/platform/query/iocounter"
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
	Key() GroupKey

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
	Key() GroupKey
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

type GroupKey interface {
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

	Equal(o GroupKey) bool
	Less(o GroupKey) bool
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
	// Returns the number of bytes written to w and any error.
	Encode(w io.Writer, result Result) (int64, error)
}

// MultiResultDecoder can decode multiple results from a reader.
type MultiResultDecoder interface {
	// Decode decodes multiple results from r.
	Decode(r io.ReadCloser) (ResultIterator, error)
}

// MultiResultEncoder can encode multiple results into a writer.
type MultiResultEncoder interface {
	// Encode writes multiple results from r into w.
	// Returns the number of bytes written to w and any error.
	Encode(w io.Writer, results ResultIterator) (int64, error)
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

func (e *DelimitedMultiResultEncoder) Encode(w io.Writer, results ResultIterator) (int64, error) {
	wc := &iocounter.Writer{Writer: w}
	for results.More() {
		result := results.Next()
		if _, err := e.Encoder.Encode(wc, result); err != nil {
			return wc.Count(), err
		}
		if _, err := wc.Write(e.Delimiter); err != nil {
			return wc.Count(), err
		}
		// Flush the writer after each result
		if f, ok := w.(flusher); ok {
			f.Flush()
		}
	}
	err := results.Err()
	if err != nil {
		err := e.Encoder.EncodeError(wc, err)
		return wc.Count(), err
	}
	return wc.Count(), nil
}

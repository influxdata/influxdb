package query

import (
	"io"

	"github.com/influxdata/ifql/query/execute"
	"github.com/influxdata/platform"
)

// ResultDecoder can decode a result from a reader.
type ResultDecoder interface {
	// Decode decodes data from r into a result.
	Decode(r io.Reader) (execute.Result, error)
}

// ResultEncoder can encode a result into a writer.
type ResultEncoder interface {
	// Encode encodes data from the result into w.
	Encode(w io.Writer, result execute.Result) error
}

// MultiResultDecoder can decode multiple results from a reader.
type MultiResultDecoder interface {
	// Decode decodes multiple results from r.
	Decode(r io.Reader) (platform.ResultIterator, error)
}

// MultiResultEncoder can encode multiple results into a writer.
type MultiResultEncoder interface {
	// Encode writes multiple results from r into w.
	Encode(w io.Writer, results platform.ResultIterator) error
}

// DelimitedMultiResultEncoder encodes multiple results using a trailing delimiter.
// The delimiter is written after every result.
// If the io.Writer implements flusher, it will be flushed after each delimiter.
type DelimitedMultiResultEncoder struct {
	Delimiter []byte
	Encoder   ResultEncoder
}

type flusher interface {
	Flush()
}

func (e *DelimitedMultiResultEncoder) Encode(w io.Writer, results platform.ResultIterator) error {
	for results.More() {
		//TODO(nathanielc): Make the result name a property of a result.
		_, result := results.Next()
		if err := e.Encoder.Encode(w, result); err != nil {
			return err
		}
		if _, err := w.Write(e.Delimiter); err != nil {
			return nil
		}
		// Flush the writer after each result
		if f, ok := w.(flusher); ok {
			f.Flush()
		}
	}
	return results.Err()
}

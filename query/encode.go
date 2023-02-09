package query

import (
	"io"
	"net/http"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/csv"
)

const (
	NoContentDialectType     = "no-content"
	NoContentWErrDialectType = "no-content-with-error"
)

// AddDialectMappings adds the mappings for the no-content dialects.
func AddDialectMappings(mappings flux.DialectMappings) error {
	if err := mappings.Add(NoContentDialectType, func() flux.Dialect {
		return NewNoContentDialect()
	}); err != nil {
		return err
	}
	return mappings.Add(NoContentWErrDialectType, func() flux.Dialect {
		return NewNoContentWithErrorDialect()
	})
}

// NoContentDialect is a dialect that provides an Encoder that discards query results.
// When invoking `dialect.Encoder().Encode(writer, results)`, `results` get consumed,
// while the `writer` is left intact.
// It is an HTTPDialect that sets the response status code to 204 NoContent.
type NoContentDialect struct{}

func NewNoContentDialect() *NoContentDialect {
	return &NoContentDialect{}
}

func (d *NoContentDialect) Encoder() flux.MultiResultEncoder {
	return &NoContentEncoder{}
}

func (d *NoContentDialect) DialectType() flux.DialectType {
	return NoContentDialectType
}

func (d *NoContentDialect) SetHeaders(w http.ResponseWriter) {
	w.WriteHeader(http.StatusNoContent)
}

type NoContentEncoder struct{}

func (e *NoContentEncoder) Encode(w io.Writer, results flux.ResultIterator) (int64, error) {
	defer results.Release()
	// Consume and discard results.
	for results.More() {
		if err := results.Next().Tables().Do(func(tbl flux.Table) error {
			return tbl.Do(func(cr flux.ColReader) error {
				return nil
			})
		}); err != nil {
			return 0, err
		}
	}
	// Do not write anything.
	return 0, nil
}

// NoContentWithErrorDialect is a dialect that provides an Encoder that discards query results,
// but it encodes runtime errors from the Flux query in CSV format.
// To discover if there was any runtime error in the query, one should check the response size.
// If it is equal to zero, then no error was present.
// Otherwise one can decode the response body to get the error. For example:
// ```
// _, err = csv.NewResultDecoder(csv.ResultDecoderConfig{}).Decode(bytes.NewReader(res))
//
//	if err != nil {
//	  // we got some runtime error
//	}
//
// ```
type NoContentWithErrorDialect struct {
	csv.ResultEncoderConfig
}

func NewNoContentWithErrorDialect() *NoContentWithErrorDialect {
	return &NoContentWithErrorDialect{
		ResultEncoderConfig: csv.DefaultEncoderConfig(),
	}
}

func (d *NoContentWithErrorDialect) Encoder() flux.MultiResultEncoder {
	return &NoContentWithErrorEncoder{
		errorEncoder: csv.NewResultEncoder(d.ResultEncoderConfig),
	}
}

func (d *NoContentWithErrorDialect) DialectType() flux.DialectType {
	return NoContentWErrDialectType
}

func (d *NoContentWithErrorDialect) SetHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Transfer-Encoding", "chunked")
}

type NoContentWithErrorEncoder struct {
	errorEncoder *csv.ResultEncoder
}

func (e *NoContentWithErrorEncoder) Encode(w io.Writer, results flux.ResultIterator) (int64, error) {
	// Make sure we release results.
	// Remember, it is safe to call `Release` multiple times.
	defer results.Release()
	// Consume and discard results, but keep an eye on errors.
	for results.More() {
		if err := results.Next().Tables().Do(func(tbl flux.Table) error {
			return tbl.Do(func(cr flux.ColReader) error {
				return nil
			})
		}); err != nil {
			// If there is an error, then encode it in the response.
			if encErr := e.errorEncoder.EncodeError(w, err); encErr != nil {
				return 0, encErr
			}
		}
	}
	// Now Release in order to populate the error, if present.
	results.Release()
	err := results.Err()
	if err != nil {
		return 0, e.errorEncoder.EncodeError(w, err)
	}
	return 0, nil
}

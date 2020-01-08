package query

import (
	"io"
	"net/http"

	"github.com/influxdata/flux"
)

const DialectType = "no-content"

// AddDialectMappings adds the no-content dialect mapping.
func AddDialectMappings(mappings flux.DialectMappings) error {
	return mappings.Add(DialectType, func() flux.Dialect {
		return NewNoContentDialect()
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
	return DialectType
}

func (d *NoContentDialect) SetHeaders(w http.ResponseWriter) {
	w.WriteHeader(http.StatusNoContent)
}

type NoContentEncoder struct {
	flux.MultiResultEncoder
}

func (e *NoContentEncoder) Encode(w io.Writer, results flux.ResultIterator) (int64, error) {
	defer results.Release()
	// Consume and discard results.
	for results.More() {
		if err := results.Next().Tables().Do(func(tbl flux.Table) error {
			return tbl.Do(func(cr flux.ColReader) error {
				cr.Release()
				return nil
			})
		}); err != nil {
			return 0, err
		}
	}
	// Do not write anything.
	return 0, nil
}

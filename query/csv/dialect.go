package csv

import (
	"net/http"

	"github.com/influxdata/platform/query"
)

const DialectType = "csv"

// AddDialectMappings adds the influxql specific dialect mappings.
func AddDialectMappings(mappings query.DialectMappings) error {
	return mappings.Add(DialectType, func() query.Dialect {
		return &Dialect{
			ResultEncoderConfig: DefaultEncoderConfig(),
		}
	})
}

// Dialect describes the output format of queries in CSV.
type Dialect struct {
	ResultEncoderConfig
}

func (d Dialect) SetHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Transfer-Encoding", "chunked")
}

func (d Dialect) Encoder() query.MultiResultEncoder {
	return NewMultiResultEncoder(d.ResultEncoderConfig)
}
func (d Dialect) DialectType() query.DialectType {
	return DialectType
}

func DefaultDialect() *Dialect {
	return &Dialect{
		ResultEncoderConfig: DefaultEncoderConfig(),
	}
}

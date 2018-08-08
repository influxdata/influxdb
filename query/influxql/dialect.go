package influxql

import (
	"net/http"

	"github.com/influxdata/platform/query"
)

const DialectType = "influxql"

// AddDialectMappings adds the influxql specific dialect mappings.
func AddDialectMappings(mappings query.DialectMappings) error {
	return mappings.Add(DialectType, func() query.Dialect {
		return new(Dialect)
	})
}

// Dialect describes the output format of InfluxQL queries.
type Dialect struct {
	TimeFormat  TimeFormat        // TimeFormat is the format of the timestamp; defaults to RFC3339Nano.
	Encoding    EncodingFormat    // Encoding is the format of the results; defaults to JSON.
	ChunkSize   int               // Chunks is the number of points per chunk encoding batch; defaults to 0 or no chunking.
	Compression CompressionFormat // Compression is the compression of the result output; defaults to None.
}

func (d *Dialect) SetHeaders(w http.ResponseWriter) {
	switch d.Encoding {
	case JSON, JSONPretty:
		w.Header().Set("Content-Type", "application/json")
	case CSV:
		w.Header().Set("Content-Type", "text/csv")
	case Msgpack:
		w.Header().Set("Content-Type", "application/x-msgpack")
	}
}

func (d *Dialect) Encoder() query.MultiResultEncoder {
	switch d.Encoding {
	case JSON, JSONPretty:
		return new(MultiResultEncoder)
	default:
		panic("not implemented")
	}
}
func (d *Dialect) DialectType() query.DialectType {
	return DialectType
}

// TimeFormat specifies the format of the timestamp in the query results.
type TimeFormat int

const (
	// RFC3339Nano is the default format for timestamps for InfluxQL.
	RFC3339Nano TimeFormat = iota
	// Hour formats time as the number of hours in the unix epoch.
	Hour
	// Minute formats time as the number of minutes in the unix epoch.
	Minute
	// Second formats time as the number of seconds in the unix epoch.
	Second
	// Millisecond formats time as the number of milliseconds in the unix epoch.
	Millisecond
	// Microsecond formats time as the number of microseconds in the unix epoch.
	Microsecond
	// Nanosecond formats time as the number of nanoseconds in the unix epoch.
	Nanosecond
)

// CompressionFormat is the format to compress the query results.
type CompressionFormat int

const (
	// None does not compress the results and is the default.
	None CompressionFormat = iota
	// Gzip compresses the query results with gzip.
	Gzip
)

// EncodingFormat is the output format for the query response content.
type EncodingFormat int

const (
	// JSON marshals the response to JSON octets.
	JSON EncodingFormat = iota
	// JSONPretty marshals the response to JSON octets with idents.
	JSONPretty
	// CSV marshals the response to CSV.
	CSV
	// Msgpack has a similar structure as the  JSON response. Used?
	Msgpack
)

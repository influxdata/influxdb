package exporter

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"

	"github.com/influxdata/influxdb/cmd/influx_inspect/export/parquet/models"
	jsoniter "github.com/json-iterator/go"
)

// Meta contains information about an exported Parquet file.
type Meta struct {
	FirstSeriesKey models.Escaped
	FirstTimestamp int64
	LastSeriesKey  models.Escaped
	LastTimestamp  int64
}

// meta is an intermediate type to facilitate encoding and decoding
// series keys in JSON format.
type meta struct {
	FirstSeriesKey string `json:"firstSeriesKeyEscaped"`
	FirstTimestamp int64  `json:"firstTimestamp"`
	LastSeriesKey  string `json:"lastSeriesKeyEscaped"`
	LastTimestamp  int64  `json:"lastTimestamp"`
}

func (m *Meta) UnmarshalJSON(data []byte) error {
	var tmp meta
	if err := jsoniter.Unmarshal(data, &tmp); err != nil {
		return err
	}

	firstSeriesKey, err := base64.StdEncoding.DecodeString(tmp.FirstSeriesKey)
	if err != nil {
		return fmt.Errorf("failed to decode firstSeriesKeyEscaped: %w", err)
	}
	lastSeriesKey, err := base64.StdEncoding.DecodeString(tmp.LastSeriesKey)
	if err != nil {
		return fmt.Errorf("failed to decode lastSeriesKeyEscaped: %w", err)
	}

	m.FirstSeriesKey = models.MakeEscaped(firstSeriesKey)
	m.FirstTimestamp = tmp.FirstTimestamp
	m.LastSeriesKey = models.MakeEscaped(lastSeriesKey)
	m.LastTimestamp = tmp.LastTimestamp

	return nil
}

func (m *Meta) MarshalJSON() ([]byte, error) {
	var b bytes.Buffer
	enc := jsoniter.NewEncoder(&b)
	err := enc.Encode(meta{
		FirstSeriesKey: base64.StdEncoding.EncodeToString(m.FirstSeriesKey.B),
		FirstTimestamp: m.FirstTimestamp,
		LastSeriesKey:  base64.StdEncoding.EncodeToString(m.LastSeriesKey.B),
		LastTimestamp:  m.LastTimestamp,
	})
	if err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

func writeMeta(m Meta, w io.Writer) error {
	enc := jsoniter.NewEncoder(w)
	return enc.Encode(m)
}

// ReadMeta decodes a Meta instance from the reader.
func ReadMeta(r io.Reader) (Meta, error) {
	dec := jsoniter.NewDecoder(r)
	var m Meta
	return m, dec.Decode(&m)
}

package gather

import (
	"bytes"
	"io"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/models"
)

// MetricsCollection is the struct including metrics and other requirements.
type MetricsCollection struct {
	OrgID        platform.ID  `json:"orgID"`
	BucketID     platform.ID  `json:"bucketID"`
	MetricsSlice MetricsSlice `json:"metrics"`
}

// Metrics is the default influx based metrics.
type Metrics struct {
	Name      string                 `json:"name"`
	Tags      map[string]string      `json:"tags"`
	Fields    map[string]interface{} `json:"fields"`
	Timestamp time.Time              `json:"timestamp"`
	Type      MetricType             `json:"type"`
}

// MetricsSlice is a slice of Metrics
type MetricsSlice []Metrics

// Points convert the MetricsSlice to model.Points
func (ms MetricsSlice) Points() (models.Points, error) {
	ps := make([]models.Point, len(ms))
	for mi, m := range ms {
		point, err := models.NewPoint(m.Name, models.NewTags(m.Tags), m.Fields, m.Timestamp)
		if err != nil {
			return ps, err
		}

		ps[mi] = point
	}
	return ps, nil
}

// Reader returns an io.Reader that enumerates the metrics.
// All metrics are allocated into the underlying buffer.
func (ms MetricsSlice) Reader() (io.Reader, error) {
	buf := new(bytes.Buffer)
	for mi, m := range ms {
		point, err := models.NewPoint(m.Name, models.NewTags(m.Tags), m.Fields, m.Timestamp)
		if err != nil {
			return nil, err
		}

		_, err = buf.WriteString(point.String())
		if err != nil {
			return nil, err
		}

		if mi < len(ms)-1 && len(ms) > 1 {
			_, err = buf.WriteString("\n")
			if err != nil {
				return nil, err
			}
		}
	}
	return buf, nil
}

// MetricType is prometheus metrics type.
type MetricType int

// the set of metric types
const (
	MetricTypeCounter MetricType = iota
	MetricTypeGauge
	MetricTypeSummary
	MetricTypeUntyped
	MetricTypeHistogrm
)

var metricTypeName = []string{
	"COUNTER",
	"GAUGE",
	"SUMMARY",
	"UNTYPED",
	"HISTOGRAM",
}
var metricTypeValue = map[string]int32{
	"COUNTER":   0,
	"GAUGE":     1,
	"SUMMARY":   2,
	"UNTYPED":   3,
	"HISTOGRAM": 4,
}

// Valid returns whether the metrics type is valid.
func (x MetricType) Valid() bool {
	return x >= MetricTypeCounter && x <= MetricTypeHistogrm
}

// String returns the string value of MetricType.
func (x MetricType) String() string {
	return metricTypeName[x]
}

// UnmarshalJSON implements the unmarshaler interface.
func (x *MetricType) UnmarshalJSON(data []byte) error {
	value, err := proto.UnmarshalJSONEnum(metricTypeValue, data, "MetricType")
	if err != nil {
		return err
	}
	*x = MetricType(value)
	return nil
}

package gather

import (
	"github.com/gogo/protobuf/proto"
)

// Metrics is the default influx based metrics.
type Metrics struct {
	Name      string                 `json:"name"`
	Tags      map[string]string      `json:"tags"`
	Fields    map[string]interface{} `json:"fields"`
	Timestamp int64                  `json:"timestamp"`
	Type      MetricType             `json:"type"`
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

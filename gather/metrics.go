package gather

import (
	"bytes"
	"io"
	"time"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/models"
	dto "github.com/prometheus/client_model/go"
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
	Type      dto.MetricType         `json:"type"`
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

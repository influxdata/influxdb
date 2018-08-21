package gather

import dto "github.com/prometheus/client_model/go"

// Metrics is the default influx based metrics
type Metrics struct {
	Name      string                 `json:"name"`
	Tags      map[string]string      `json:"tags"`
	Fields    map[string]interface{} `json:"fields"`
	Timestamp int64                  `json:"timestamp"`
	Type      dto.MetricType         `json:"type"`
}

package prometheus

import dto "github.com/prometheus/client_model/go"

// Transformer modifies prometheus metrics families.
type Transformer interface {
	// Transform updates the metrics family
	Transform(mfs []*dto.MetricFamily) []*dto.MetricFamily
}

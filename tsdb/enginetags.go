package tsdb

import "github.com/prometheus/client_golang/prometheus"

// EngineTags holds tags for prometheus
//
// It should not be used for behaviour other than attaching tags to prometheus metrics
type EngineTags struct {
	Path, WalPath, Id, Bucket, EngineVersion string
}

func (et *EngineTags) GetLabels() prometheus.Labels {
	return prometheus.Labels{
		"path":    et.Path,
		"walPath": et.WalPath,
		"id":      et.Id,
		"bucket":  et.Bucket,
		"engine":  et.EngineVersion,
	}
}

func EngineLabelNames() []string {
	emptyLabels := (&EngineTags{}).GetLabels()
	val := make([]string, 0, len(emptyLabels))
	for k := range emptyLabels {
		val = append(val, k)
	}
	return val
}

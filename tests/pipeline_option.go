package tests

import (
	"github.com/influxdata/influxdb/v2/cmd/influxd/launcher"
	"go.uber.org/zap"
)

// PipelineOption configures a pipeline.
type PipelineOption interface {
	applyConfig(*pipelineConfig)
	applyOptSetter(*launcher.InfluxdOpts)
}

type pipelineOption struct {
	applyConfigFn func(*pipelineConfig)
	optSetter     launcher.OptSetter
}

var _ PipelineOption = pipelineOption{}

func (o pipelineOption) applyConfig(pc *pipelineConfig) {
	if o.applyConfigFn != nil {
		o.applyConfigFn(pc)
	}
}

func (o pipelineOption) applyOptSetter(opts *launcher.InfluxdOpts) {
	if o.optSetter != nil {
		o.optSetter(opts)
	}
}

// WithDefaults returns a slice of options for a default pipeline.
func WithDefaults() []PipelineOption {
	return []PipelineOption{}
}

// WithLogger sets the logger for the pipeline itself, and the underlying launcher.
func WithLogger(logger *zap.Logger) PipelineOption {
	return pipelineOption{
		applyConfigFn: func(config *pipelineConfig) {
			config.logger = logger
		},
	}
}

// WithInfluxQLMaxSelectSeriesN configures the maximum number of series returned by a select statement.
func WithInfluxQLMaxSelectSeriesN(n int) PipelineOption {
	return pipelineOption{
		optSetter: func(o *launcher.InfluxdOpts) {
			o.CoordinatorConfig.MaxSelectSeriesN = n
		},
	}
}

// WithInfluxQLMaxSelectBucketsN configures the maximum number of buckets returned by a select statement.
func WithInfluxQLMaxSelectBucketsN(n int) PipelineOption {
	return pipelineOption{
		optSetter: func(o *launcher.InfluxdOpts) {
			o.CoordinatorConfig.MaxSelectBucketsN = n
		},
	}
}

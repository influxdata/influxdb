package tests

import (
	"github.com/influxdata/influxdb/v2/cmd/influxd/launcher"
	"go.uber.org/zap"
)

// PipelineOption configures a pipeline.
type PipelineOption interface {
	applyConfig(*pipelineConfig)
	makeLauncherOption() launcher.Option
}

type pipelineOption struct {
	applyConfigFn        func(*pipelineConfig)
	makeLauncherOptionFn func() launcher.Option
}

var _ PipelineOption = pipelineOption{}

func (o pipelineOption) applyConfig(pc *pipelineConfig) {
	if o.applyConfigFn != nil {
		o.applyConfigFn(pc)
	}
}

func (o pipelineOption) makeLauncherOption() launcher.Option {
	if o.makeLauncherOptionFn != nil {
		return o.makeLauncherOptionFn()
	}
	return nil
}

// WithDefaults returns a slice of options for a default pipeline.
func WithDefaults() []PipelineOption {
	return []PipelineOption{}
}

// WithReplicas sets the number of replicas in the pipeline.
func WithLogger(logger *zap.Logger) PipelineOption {
	return pipelineOption{
		applyConfigFn: func(pc *pipelineConfig) {
			pc.logger = logger
		},
	}
}

// WithInfluxQLMaxSelectSeriesN configures the maximum number of series returned by a select statement.
func WithInfluxQLMaxSelectSeriesN(n int) PipelineOption {
	return pipelineOption{
		makeLauncherOptionFn: func() launcher.Option {
			return launcher.WithInfluxQLMaxSelectSeriesN(n)
		},
	}
}

// WithInfluxQLMaxSelectBucketsN configures the maximum number of buckets returned by a select statement.
func WithInfluxQLMaxSelectBucketsN(n int) PipelineOption {
	return pipelineOption{
		makeLauncherOptionFn: func() launcher.Option {
			return launcher.WithInfluxQLMaxSelectBucketsN(n)
		},
	}
}

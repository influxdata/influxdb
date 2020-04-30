package mock

import (
	"context"

	"github.com/influxdata/influxdb/v2/kit/feature"
)

// Flagger is a mock.
type Flagger struct {
	m map[string]interface{}
}

// NewFlagger returns a mock Flagger.
func NewFlagger(flags map[feature.Flag]interface{}) *Flagger {
	m := make(map[string]interface{}, len(flags))
	for k, v := range flags {
		m[k.Key()] = v
	}
	return &Flagger{m}
}

// Flags returns a map of flag keys to flag values according to its configured flag map.
// It never returns an error.
func (f Flagger) Flags(context.Context, ...feature.Flag) (map[string]interface{}, error) {
	return f.m, nil
}

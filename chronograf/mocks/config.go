package mocks

import (
	"context"

	"github.com/influxdata/influxdb/chronograf"
)

// ConfigStore stores global application configuration
type ConfigStore struct {
	Config *chronograf.Config
}

// Initialize is noop in mocks store
func (c ConfigStore) Initialize(ctx context.Context) error {
	return nil
}

// Get returns the whole global application configuration
func (c ConfigStore) Get(ctx context.Context) (*chronograf.Config, error) {
	return c.Config, nil
}

// Update updates the whole global application configuration
func (c ConfigStore) Update(ctx context.Context, config *chronograf.Config) error {
	c.Config = config
	return nil
}

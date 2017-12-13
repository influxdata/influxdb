package mocks

import (
	"context"

	"github.com/influxdata/chronograf"
)

// ConfigStore stores global application configuration
type ConfigStore struct {
	Config *chronograf.Config
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

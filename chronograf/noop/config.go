package noop

import (
	"context"
	"fmt"

	"github.com/influxdata/platform/chronograf"
)

// ensure ConfigStore implements chronograf.ConfigStore
var _ chronograf.ConfigStore = &ConfigStore{}

type ConfigStore struct{}

// TODO(desa): this really should be removed
func (s *ConfigStore) Initialize(context.Context) error {
	return fmt.Errorf("cannot initialize")
}

func (s *ConfigStore) Get(context.Context) (*chronograf.Config, error) {
	return nil, chronograf.ErrConfigNotFound
}

func (s *ConfigStore) Update(context.Context, *chronograf.Config) error {
	return fmt.Errorf("cannot update conifg")
}

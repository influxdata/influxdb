package bolt

import (
	"context"
	"fmt"

	"github.com/boltdb/bolt"
	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/bolt/internal"
)

// Ensure ConfigStore implements chronograf.ConfigStore.
var _ chronograf.ConfigStore = &ConfigStore{}

// ConfigBucket is used to store chronograf application state
var ConfigBucket = []byte("ConfigV1")

// configID is the boltDB key where the configuration object is stored
var configID = []byte("config/v1")

// ConfigStore uses bolt to store and retrieve global
// application configuration
type ConfigStore struct {
	client *Client
}

func (s *ConfigStore) Migrate(ctx context.Context) error {
	if _, err := s.Get(ctx); err != nil {
		return s.Initialize(ctx)
	}
	return nil
}

func (s *ConfigStore) Initialize(ctx context.Context) error {
	cfg := chronograf.Config{
		Auth: chronograf.AuthConfig{
			SuperAdminNewUsers: true,
		},
	}
	return s.Update(ctx, &cfg)
}

func (s *ConfigStore) Get(ctx context.Context) (*chronograf.Config, error) {
	var cfg chronograf.Config
	err := s.client.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(ConfigBucket).Get(configID)
		if v == nil {
			return chronograf.ErrConfigNotFound
		}
		return internal.UnmarshalConfig(v, &cfg)
	})

	if err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (s *ConfigStore) Update(ctx context.Context, cfg *chronograf.Config) error {
	if cfg == nil {
		return fmt.Errorf("config provided was nil")
	}
	return s.client.db.Update(func(tx *bolt.Tx) error {
		if v, err := internal.MarshalConfig(cfg); err != nil {
			return err
		} else if err := tx.Bucket(ConfigBucket).Put(configID, v); err != nil {
			return err
		}
		return nil
	})
}

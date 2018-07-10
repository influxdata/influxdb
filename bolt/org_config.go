package bolt

import (
	"context"
	"fmt"

	"github.com/boltdb/bolt"
	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/bolt/internal"
)

// Ensure OrganizationConfigStore implements chronograf.OrganizationConfigStore.
var _ chronograf.OrganizationConfigStore = &OrganizationConfigStore{}

// OrganizationConfigBucket is used to store chronograf organization configurations
var OrganizationConfigBucket = []byte("OrganizationConfigV1")

// OrganizationConfigStore uses bolt to store and retrieve organization configurations
type OrganizationConfigStore struct {
	client *Client
}

func (s *OrganizationConfigStore) Migrate(ctx context.Context) error {
	return nil
}

// Get retrieves an OrganizationConfig from the store
func (s *OrganizationConfigStore) Get(ctx context.Context, orgID string) (*chronograf.OrganizationConfig, error) {
	var cfg chronograf.OrganizationConfig

	err := s.client.db.View(func(tx *bolt.Tx) error {
		return s.get(ctx, tx, orgID, &cfg)
	})

	if err != nil {
		return nil, err
	}

	return &cfg, nil
}

func (s *OrganizationConfigStore) get(ctx context.Context, tx *bolt.Tx, orgID string, cfg *chronograf.OrganizationConfig) error {
	v := tx.Bucket(OrganizationConfigBucket).Get([]byte(orgID))
	if len(v) == 0 {
		return chronograf.ErrOrganizationConfigNotFound
	}
	return internal.UnmarshalOrganizationConfig(v, cfg)
}

// FindOrCreate gets an OrganizationConfig from the store or creates one if none exists for this organization
func (s *OrganizationConfigStore) FindOrCreate(ctx context.Context, orgID string) (*chronograf.OrganizationConfig, error) {
	var cfg chronograf.OrganizationConfig
	err := s.client.db.Update(func(tx *bolt.Tx) error {
		err := s.get(ctx, tx, orgID, &cfg)
		if err == chronograf.ErrOrganizationConfigNotFound {
			cfg = newOrganizationConfig(orgID)
			return s.update(ctx, tx, &cfg)
		}
		return err
	})

	if err != nil {
		return nil, err
	}
	return &cfg, nil
}

// Update replaces the OrganizationConfig in the store
func (s *OrganizationConfigStore) Update(ctx context.Context, cfg *chronograf.OrganizationConfig) error {
	if cfg == nil {
		return fmt.Errorf("config provided was nil")
	}
	return s.client.db.Update(func(tx *bolt.Tx) error {
		return s.update(ctx, tx, cfg)
	})
}

func (s *OrganizationConfigStore) update(ctx context.Context, tx *bolt.Tx, cfg *chronograf.OrganizationConfig) error {
	if v, err := internal.MarshalOrganizationConfig(cfg); err != nil {
		return err
	} else if err := tx.Bucket(OrganizationConfigBucket).Put([]byte(cfg.OrganizationID), v); err != nil {
		return err
	}
	return nil
}

func newOrganizationConfig(orgID string) chronograf.OrganizationConfig {
	return chronograf.OrganizationConfig{
		OrganizationID: orgID,
		LogViewer: chronograf.LogViewerConfig{
			Columns: []chronograf.LogViewerColumn{
				{
					Name:     "time",
					Position: 0,
					Encodings: []chronograf.ColumnEncoding{
						{
							Type:  "visibility",
							Value: "hidden",
						},
					},
				},
				{
					Name:     "severity",
					Position: 1,
					Encodings: []chronograf.ColumnEncoding{

						{
							Type:  "visibility",
							Value: "visible",
						},
						{
							Type:  "label",
							Value: "icon",
						},
						{
							Type:  "label",
							Value: "text",
						},
					},
				},
				{
					Name:     "timestamp",
					Position: 2,
					Encodings: []chronograf.ColumnEncoding{

						{
							Type:  "visibility",
							Value: "visible",
						},
					},
				},
				{
					Name:     "message",
					Position: 3,
					Encodings: []chronograf.ColumnEncoding{

						{
							Type:  "visibility",
							Value: "visible",
						},
					},
				},
				{
					Name:     "facility",
					Position: 4,
					Encodings: []chronograf.ColumnEncoding{

						{
							Type:  "visibility",
							Value: "visible",
						},
					},
				},
				{
					Name:     "procid",
					Position: 5,
					Encodings: []chronograf.ColumnEncoding{

						{
							Type:  "visibility",
							Value: "visible",
						},
						{
							Type:  "displayName",
							Value: "Proc ID",
						},
					},
				},
				{
					Name:     "appname",
					Position: 6,
					Encodings: []chronograf.ColumnEncoding{
						{
							Type:  "visibility",
							Value: "visible",
						},
						{
							Type:  "displayName",
							Value: "Application",
						},
					},
				},
				{
					Name:     "host",
					Position: 7,
					Encodings: []chronograf.ColumnEncoding{
						{
							Type:  "visibility",
							Value: "visible",
						},
					},
				},
			},
		},
	}
}

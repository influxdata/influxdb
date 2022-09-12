package bolt

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	platform "github.com/influxdata/influxdb/v2"
	platform2 "github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/rand"
	"github.com/influxdata/influxdb/v2/snowflake"
	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"
)

const DefaultFilename = "influxd.bolt"

// Client is a client for the boltDB data store.
type Client struct {
	Path string
	db   *bolt.DB
	log  *zap.Logger

	IDGenerator    platform2.IDGenerator
	TokenGenerator platform.TokenGenerator
	platform.TimeGenerator

	pluginsCollector *pluginMetricsCollector
}

// NewClient returns an instance of a Client.
func NewClient(log *zap.Logger) *Client {
	return &Client{
		log:            log,
		IDGenerator:    snowflake.NewIDGenerator(),
		TokenGenerator: rand.NewTokenGenerator(64),
		TimeGenerator:  platform.RealTimeGenerator{},
		// Refresh telegraf plugin metrics every hour.
		pluginsCollector: NewPluginMetricsCollector(time.Minute * 59),
	}
}

// DB returns the clients DB.
func (c *Client) DB() *bolt.DB {
	return c.db
}

// Open / create boltDB file.
func (c *Client) Open(ctx context.Context) error {
	// Ensure the required directory structure exists.
	if err := os.MkdirAll(filepath.Dir(c.Path), 0700); err != nil {
		return fmt.Errorf("unable to create directory %s: %v", c.Path, err)
	}

	if _, err := os.Stat(c.Path); err != nil && !os.IsNotExist(err) {
		return err
	}

	// Open database file.
	db, err := bolt.Open(c.Path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		// Hack to give a slightly nicer error message for a known failure mode when bolt calls
		// mmap on a file system that doesn't support the MAP_SHARED option.
		//
		// See: https://github.com/boltdb/bolt/issues/272
		// See: https://stackoverflow.com/a/18421071
		if err.Error() == "invalid argument" {
			return fmt.Errorf("unable to open boltdb: mmap of %q may not support the MAP_SHARED option", c.Path)
		}

		return fmt.Errorf("unable to open boltdb: %w", err)
	}
	c.db = db

	if err := c.initialize(ctx); err != nil {
		return err
	}

	c.pluginsCollector.Open(c.db)

	c.log.Info("Resources opened", zap.String("path", c.Path))
	return nil
}

// initialize creates Buckets that are missing
func (c *Client) initialize(ctx context.Context) error {
	if err := c.db.Update(func(tx *bolt.Tx) error {
		// Always create ID bucket.
		// TODO: is this still needed?
		if err := c.initializeID(tx); err != nil {
			return err
		}

		// TODO: make card to normalize everything under kv?
		bkts := [][]byte{
			authorizationBucket,
			bucketBucket,
			dashboardBucket,
			organizationBucket,
			scraperBucket,
			telegrafBucket,
			telegrafPluginsBucket,
			remoteBucket,
			replicationBucket,
			userBucket,
		}
		for _, bktName := range bkts {
			if _, err := tx.CreateBucketIfNotExists(bktName); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}

	return nil
}

// Close the connection to the bolt database
func (c *Client) Close() error {
	c.pluginsCollector.Close()
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

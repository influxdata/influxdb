package bolt

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	bolt "github.com/coreos/bbolt"
	platform "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/rand"
	"github.com/influxdata/influxdb/v2/snowflake"
	"go.uber.org/zap"
)

const DefaultFilename = "influxd.bolt"

// Client is a client for the boltDB data store.
type Client struct {
	Path string
	db   *bolt.DB
	log  *zap.Logger

	IDGenerator    platform.IDGenerator
	TokenGenerator platform.TokenGenerator
	platform.TimeGenerator
}

// NewClient returns an instance of a Client.
func NewClient(log *zap.Logger) *Client {
	return &Client{
		log:            log,
		IDGenerator:    snowflake.NewIDGenerator(),
		TokenGenerator: rand.NewTokenGenerator(64),
		TimeGenerator:  platform.RealTimeGenerator{},
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
		return fmt.Errorf("unable to open boltdb; is there a chronograf already running?  %v", err)
	}
	c.db = db

	if err := c.initialize(ctx); err != nil {
		return err
	}

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
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

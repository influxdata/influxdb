package bolt

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/coreos/bbolt"
	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/rand"
	"github.com/influxdata/influxdb/snowflake"
	"go.uber.org/zap"
)

// OpPrefix is the prefix for bolt ops
const OpPrefix = "bolt/"

func getOp(op string) string {
	return OpPrefix + op
}

// Client is a client for the boltDB data store.
type Client struct {
	Path   string
	db     *bolt.DB
	Logger *zap.Logger

	IDGenerator    platform.IDGenerator
	TokenGenerator platform.TokenGenerator
	time           func() time.Time
}

// NewClient returns an instance of a Client.
func NewClient() *Client {
	return &Client{
		Logger:         zap.NewNop(),
		IDGenerator:    snowflake.NewIDGenerator(),
		TokenGenerator: rand.NewTokenGenerator(64),
		time:           time.Now,
	}
}

// DB returns the clients DB.
func (c *Client) DB() *bolt.DB {
	return c.db
}

// WithLogger sets the logger an a client. It should not be called after
// the client has been open.
func (c *Client) WithLogger(l *zap.Logger) {
	c.Logger = l
}

// WithTime sets the function for computing the current time. Used for updating meta data
// about objects stored. Should only be used in tests for mocking.
func (c *Client) WithTime(fn func() time.Time) {
	c.time = fn
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

	c.Logger.Info("Resources opened", zap.String("path", c.Path))
	return nil
}

// initialize creates Buckets that are missing
func (c *Client) initialize(ctx context.Context) error {
	if err := c.db.Update(func(tx *bolt.Tx) error {
		// Always create Buckets bucket.
		if err := c.initializeBuckets(ctx, tx); err != nil {
			return err
		}

		// Always create Organizations bucket.
		if err := c.initializeOrganizations(ctx, tx); err != nil {
			return err
		}

		// Always create Dashboards bucket.
		if err := c.initializeDashboards(ctx, tx); err != nil {
			return err
		}

		// Always create User bucket.
		if err := c.initializeUsers(ctx, tx); err != nil {
			return err
		}

		// Always create Authorization bucket.
		if err := c.initializeAuthorizations(ctx, tx); err != nil {
			return err
		}

		// Always create Onboarding bucket.
		if err := c.initializeOnboarding(ctx, tx); err != nil {
			return err
		}

		// Always create Telegraf Config bucket.
		if err := c.initializeTelegraf(ctx, tx); err != nil {
			return err
		}

		// Always create Source bucket.
		if err := c.initializeSources(ctx, tx); err != nil {
			return err
		}

		// Always create Views bucket.
		if err := c.initializeViews(ctx, tx); err != nil {
			return err
		}

		// Always create Macros bucket.
		if err := c.initializeMacros(ctx, tx); err != nil {
			return err
		}

		// Always create Scraper bucket.
		if err := c.initializeScraperTargets(ctx, tx); err != nil {
			return err
		}

		// Always create UserResourceMapping bucket.
		if err := c.initializeUserResourceMappings(ctx, tx); err != nil {
			return err
		}

		// Always create labels bucket.
		if err := c.initializeLabels(ctx, tx); err != nil {
			return err
		}

		// Always create Session bucket.
		if err := c.initializeSessions(ctx, tx); err != nil {
			return err
		}

		// Always create KeyValueLog bucket.
		if err := c.initializeKeyValueLog(ctx, tx); err != nil {
			return err
		}

		// Always create SecretService bucket.
		if err := c.initializeSecretService(ctx, tx); err != nil {
			return err
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

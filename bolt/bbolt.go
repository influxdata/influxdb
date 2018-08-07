package bolt

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/coreos/bbolt"
	"github.com/influxdata/platform"
	"github.com/influxdata/platform/rand"
	"github.com/influxdata/platform/snowflake"
	"go.uber.org/zap"
)

const (
	// ErrUnableToOpen means we had an issue establishing a connection (or creating the database)
	ErrUnableToOpen = "Unable to open boltdb; is there a chronograf already running?  %v"
	// ErrUnableToBackup means we couldn't copy the db file into ./backup
	ErrUnableToBackup = "Unable to backup your database prior to migrations:  %v"
	// ErrUnableToInitialize means we couldn't create missing Buckets (maybe a timeout)
	ErrUnableToInitialize = "Unable to boot boltdb:  %v"
	// ErrUnableToMigrate means we had an issue changing the db schema
	ErrUnableToMigrate = "Unable to migrate boltdb:  %v"
)

// Client is a client for the boltDB data store.
type Client struct {
	Path   string
	db     *bolt.DB
	Logger *zap.Logger

	IDGenerator    platform.IDGenerator
	TokenGenerator platform.TokenGenerator
}

// NewClient returns an instance of a Client.
func NewClient() *Client {
	return &Client{
		Logger:         zap.NewNop(),
		IDGenerator:    snowflake.NewIDGenerator(),
		TokenGenerator: rand.NewTokenGenerator(64),
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

// Open / create boltDB file.
func (c *Client) Open(ctx context.Context) error {
	if _, err := os.Stat(c.Path); err != nil && !os.IsNotExist(err) {
		return err
	}

	// Open database file.
	db, err := bolt.Open(c.Path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return fmt.Errorf(ErrUnableToOpen, err)
	}
	c.db = db

	return c.initialize(context.TODO())
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

		// Always create Source bucket.
		if err := c.initializeSources(ctx, tx); err != nil {
			return err
		}

		// Always create Views bucket.
		if err := c.initializeViews(ctx, tx); err != nil {
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

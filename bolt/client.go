package bolt

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"time"

	"github.com/boltdb/bolt"
	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/uuid"
)

// Client is a client for the boltDB data store.
type Client struct {
	Path      string
	db        *bolt.DB
	isNew     bool
	Now       func() time.Time
	LayoutIDs chronograf.ID

	BuildStore         *BuildStore
	SourcesStore       *SourcesStore
	ServersStore       *ServersStore
	LayoutsStore       *LayoutsStore
	DashboardsStore    *DashboardsStore
	UsersStore         *UsersStore
	OrganizationsStore *OrganizationsStore
	ConfigStore        *ConfigStore
}

// NewClient initializes all stores
func NewClient() *Client {
	c := &Client{Now: time.Now}
	c.BuildStore = &BuildStore{client: c}
	c.SourcesStore = &SourcesStore{client: c}
	c.ServersStore = &ServersStore{client: c}
	c.LayoutsStore = &LayoutsStore{
		client: c,
		IDs:    &uuid.V4{},
	}
	c.DashboardsStore = &DashboardsStore{
		client: c,
		IDs:    &uuid.V4{},
	}
	c.UsersStore = &UsersStore{client: c}
	c.OrganizationsStore = &OrganizationsStore{client: c}
	c.ConfigStore = &ConfigStore{client: c}
	return c
}

// Open and initialize boltDB. Initial buckets are created if they do not exist.
func (c *Client) Open(ctx context.Context) error {
	if _, err := os.Stat(c.Path); os.IsNotExist(err) {
		c.isNew = true
	}

	// Open database file.
	db, err := bolt.Open(c.Path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return err
	}
	c.db = db

	return nil
}

func (c *Client) Initialize(ctx context.Context) error {
	if err := c.db.Update(func(tx *bolt.Tx) error {
		// Always create Organizations bucket.
		if _, err := tx.CreateBucketIfNotExists(OrganizationsBucket); err != nil {
			return err
		}
		// Always create Sources bucket.
		if _, err := tx.CreateBucketIfNotExists(SourcesBucket); err != nil {
			return err
		}
		// Always create Servers bucket.
		if _, err := tx.CreateBucketIfNotExists(ServersBucket); err != nil {
			return err
		}
		// Always create Layouts bucket.
		if _, err := tx.CreateBucketIfNotExists(LayoutsBucket); err != nil {
			return err
		}
		// Always create Dashboards bucket.
		if _, err := tx.CreateBucketIfNotExists(DashboardsBucket); err != nil {
			return err
		}
		// Always create Users bucket.
		if _, err := tx.CreateBucketIfNotExists(UsersBucket); err != nil {
			return err
		}
		// Always create Config bucket.
		if _, err := tx.CreateBucketIfNotExists(ConfigBucket); err != nil {
			return err
		}
		// Always create Build bucket.
		if _, err := tx.CreateBucketIfNotExists(BuildBucket); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	return nil
}

// Migrate moves data from an old schema to a new schema in each Store
func (c *Client) Migrate(ctx context.Context, build chronograf.BuildInfo) error {
	if c.db != nil {
		// Runtime migrations
		if err := c.OrganizationsStore.Migrate(ctx); err != nil {
			return err
		}
		if err := c.SourcesStore.Migrate(ctx); err != nil {
			return err
		}
		if err := c.ServersStore.Migrate(ctx); err != nil {
			return err
		}
		if err := c.LayoutsStore.Migrate(ctx); err != nil {
			return err
		}
		if err := c.DashboardsStore.Migrate(ctx); err != nil {
			return err
		}
		if err := c.ConfigStore.Migrate(ctx); err != nil {
			return err
		}
		if err := c.BuildStore.Migrate(ctx, build); err != nil {
			return err
		}
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

func (c *Client) Copy(ctx context.Context, version string) error {
	fromFile, err := os.Open(c.Path)
	if err != nil {
		return err
	}
	defer fromFile.Close()

	backupDir := path.Join(path.Dir(c.Path), "backup")
	_ = os.Mkdir(backupDir, 0700)

	toName := fmt.Sprintf("%s.%s", path.Base(c.Path), version)
	toPath := path.Join(backupDir, toName)
	toFile, err := os.OpenFile(toPath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	defer toFile.Close()

	_, err = io.Copy(toFile, fromFile)
	if err != nil {
		return err
	}

	log.Printf("Successfully created %s", toPath)

	return nil
}

// Makes a copy of the database to the backup/ directory, if necessary:
// - If this is a fresh install, don't create a backup and store the current version
// - If we are on the same version, don't create a backup
// - If the version has changed, create a backup and store the current version
func (c *Client) Backup(ctx context.Context, build chronograf.BuildInfo) error {
	lastBuild, err := c.BuildStore.Get(ctx)
	if err != nil {
		return err
	}
	if lastBuild.Version == build.Version {
		return nil
	}
	if c.isNew {
		return nil
	}

	// The database was pre-existing, and the version has changed
	// and so create a backup

	log.Printf("Moving from version " + lastBuild.Version)
	log.Printf("Moving to version " + build.Version)

	if err = c.Copy(ctx, lastBuild.Version); err != nil {
		return err
	}

	return nil
}

func bucket(b []byte, org string) []byte {
	return []byte(path.Join(string(b), org))
}

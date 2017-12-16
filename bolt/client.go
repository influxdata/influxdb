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

func (c *Client) Migrate(ctx context.Context) error {
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

func (c *Client) Backup(ctx context.Context, build chronograf.BuildInfo) error {
	lastBuild, err := c.BuildStore.Get(ctx)
	if err != nil {
		log.Fatal(err)
	}
	if c.isNew {
		if err = c.BuildStore.Update(ctx, build); err != nil {
			log.Fatal(err)
		}
		return nil
	}
	if lastBuild.Version == build.Version {
		return nil
	}

	log.Printf("Moving from version " + lastBuild.Version)
	log.Printf("Moving to version " + build.Version)

	from, err := os.Open(c.Path)
	if err != nil {
		log.Fatal(err)
	}
	defer from.Close()

	backupDir := path.Join(path.Dir(c.Path), "backup")
	_ = os.Mkdir(backupDir, 0700)

	toName := fmt.Sprintf("%s.%s", c.Path, lastBuild.Version)
	toPath := path.Join(backupDir, toName)
	to, err := os.OpenFile(toPath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		log.Fatal(err)
	}
	defer to.Close()

	_, err = io.Copy(to, from)
	if err != nil {
		log.Fatal(err)
	}

	if err = c.BuildStore.Update(ctx, build); err != nil {
		log.Fatal(err)
	}

	log.Printf("Successfully created %s", toPath)

	return nil
}

func bucket(b []byte, org string) []byte {
	return []byte(path.Join(string(b), org))
}

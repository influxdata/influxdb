package bolt

import (
	"context"
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
	Now       func() time.Time
	LayoutIDs chronograf.ID

	SourcesStore       *SourcesStore
	ServersStore       *ServersStore
	LayoutsStore       *LayoutsStore
	DashboardsStore    *DashboardsStore
	UsersStore         *UsersStore
	OrganizationsStore *OrganizationsStore
}

// NewClient initializes all stores
func NewClient() *Client {
	c := &Client{Now: time.Now}
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
	return c
}

// Open and initialize boltDB. Initial buckets are created if they do not exist.
func (c *Client) Open(ctx context.Context) error {
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
		return nil
	}); err != nil {
		return err
	}

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

	return nil
}

// Close the connection to the bolt database
func (c *Client) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

func bucket(b []byte, org string) []byte {
	return []byte(path.Join(string(b), org))
}

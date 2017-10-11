package bolt

import (
	"context"
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

	SourcesStore    *SourcesStore
	ServersStore    *ServersStore
	LayoutStore     *LayoutStore
	UsersStore      *UsersStore
	RolesStore      *RolesStore
	DashboardsStore *DashboardsStore
}

// NewClient initializes all stores
func NewClient() *Client {
	c := &Client{Now: time.Now}
	c.SourcesStore = &SourcesStore{client: c}
	c.ServersStore = &ServersStore{client: c}
	c.UsersStore = &UsersStore{client: c}
	c.RolesStore = &RolesStore{client: c}
	c.LayoutStore = &LayoutStore{
		client: c,
		IDs:    &uuid.V4{},
	}
	c.DashboardsStore = &DashboardsStore{
		client: c,
		IDs:    &uuid.V4{},
	}
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
		// Always create Sources bucket.
		if _, err := tx.CreateBucketIfNotExists(SourcesBucket); err != nil {
			return err
		}
		// Always create Servers bucket.
		if _, err := tx.CreateBucketIfNotExists(ServersBucket); err != nil {
			return err
		}
		// Always create Layouts bucket.
		if _, err := tx.CreateBucketIfNotExists(LayoutBucket); err != nil {
			return err
		}
		// Always create Dashboards bucket.
		if _, err := tx.CreateBucketIfNotExists(DashboardBucket); err != nil {
			return err
		}
		// Always create Users bucket.
		if _, err := tx.CreateBucketIfNotExists(UsersBucket); err != nil {
			return err
		}
		// Always create Roles bucket.
		if _, err := tx.CreateBucketIfNotExists(RolesBucket); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	// Runtime migrations
	return c.DashboardsStore.Migrate(ctx)
}

// Close the connection to the bolt database
func (c *Client) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

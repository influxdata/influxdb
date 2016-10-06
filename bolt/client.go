package bolt

import (
	"time"

	"github.com/boltdb/bolt"
)

// Client is a client for the boltDB data store.
type Client struct {
	Path string
	db   *bolt.DB
	Now  func() time.Time

	ExplorationStore *ExplorationStore
	SourcesStore     *SourcesStore
	ServersStore     *ServersStore
	LayoutStore      *LayoutStore
}

func NewClient() *Client {
	c := &Client{Now: time.Now}
	c.ExplorationStore = &ExplorationStore{client: c}
	c.SourcesStore = &SourcesStore{client: c}
	c.ServersStore = &ServersStore{client: c}
	c.LayoutStore = &LayoutStore{client: c}
	return c
}

// Open and initialize boltDB. Initial buckets are created if they do not exist.
func (c *Client) Open() error {
	// Open database file.
	db, err := bolt.Open(c.Path, 0644, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return err
	}
	c.db = db

	if err := c.db.Update(func(tx *bolt.Tx) error {
		// Always create explorations bucket.
		if _, err := tx.CreateBucketIfNotExists(ExplorationBucket); err != nil {
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
		if _, err := tx.CreateBucketIfNotExists(LayoutBucket); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return err
	}

	c.ExplorationStore = &ExplorationStore{client: c}
	c.SourcesStore = &SourcesStore{client: c}
	c.ServersStore = &ServersStore{client: c}
	c.LayoutStore = &LayoutStore{client: c}

	return nil
}

func (c *Client) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

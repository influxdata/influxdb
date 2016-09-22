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
}

func NewClient() *Client {
	return &Client{
		Now: time.Now,
	}
}

// Open and initializ boltDB. Initial buckets are created if they do not exist.
func (c *Client) Open() error {
	// Open database file.
	db, err := bolt.Open(c.Path, 0666, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return err
	}
	c.db = db

	tx, err := c.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Always create explorations bucket.
	if _, err := tx.CreateBucketIfNotExists([]byte("Explorations")); err != nil {
		return err
	}

	return tx.Commit()
}

func (c *Client) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

// Connect creates a new session for boltDB.
func (c *Client) Connect() *Session {
	s := newSession(c.db)
	s.now = c.Now()
	return s
}

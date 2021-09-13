package bolt

import (
	"errors"
	"fmt"
	"math/rand"

	platform2 "github.com/influxdata/influxdb/v2/kit/platform"
	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"
)

var (
	idsBucket     = []byte("idsv1")
	idKey         = []byte("id")
	errIDNotFound = errors.New("source not found")
)

var _ platform2.IDGenerator = (*Client)(nil)

func (c *Client) initializeID(tx *bolt.Tx) error {
	if _, err := tx.CreateBucketIfNotExists(idsBucket); err != nil {
		return err
	}

	_, err := c.getID(tx)
	if err != nil && err != errIDNotFound {
		return err
	}

	if err == errIDNotFound {
		if err := c.generateID(tx); err != nil {
			return err
		}
	}

	return nil
}

// ID retrieves the unique ID for this influx instance.
func (c *Client) ID() platform2.ID {
	// if any error occurs return a random number
	id := platform2.ID(rand.Int63())
	err := c.db.View(func(tx *bolt.Tx) error {
		val, err := c.getID(tx)
		if err != nil {
			return err
		}

		id = val
		return nil
	})

	if err != nil {
		c.log.Error("Unable to load id", zap.Error(err))
	}

	return id
}

func (c *Client) getID(tx *bolt.Tx) (platform2.ID, error) {
	v := tx.Bucket(idsBucket).Get(idKey)
	if len(v) == 0 {
		return platform2.InvalidID(), errIDNotFound
	}
	return decodeID(v)
}

func decodeID(val []byte) (platform2.ID, error) {
	if len(val) < platform2.IDLength {
		// This should not happen.
		return platform2.InvalidID(), fmt.Errorf("provided value is too short to contain an ID. Please report this error")
	}

	var id platform2.ID
	if err := id.Decode(val[:platform2.IDLength]); err != nil {
		return platform2.InvalidID(), err
	}
	return id, nil
}

func (c *Client) generateID(tx *bolt.Tx) error {
	id := c.IDGenerator.ID()
	encodedID, err := id.Encode()
	if err != nil {
		return err
	}

	return tx.Bucket(idsBucket).Put(idKey, encodedID)
}

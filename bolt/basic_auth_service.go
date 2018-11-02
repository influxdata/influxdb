package bolt

import (
	"context"

	bolt "github.com/coreos/bbolt"
	"golang.org/x/crypto/bcrypt"
)

// SetPassword stores the password hash associated with a user.
func (c *Client) SetPassword(ctx context.Context, name string, password string) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		return c.setPassword(ctx, tx, name, password)
	})
}

// HashCost currently using the default cost of bcrypt
var HashCost = bcrypt.DefaultCost

func (c *Client) setPassword(ctx context.Context, tx *bolt.Tx, name string, password string) error {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), HashCost)
	if err != nil {
		return err
	}

	u, err := c.findUserByName(ctx, tx, name)
	if err != nil {
		return err
	}

	encodedID, err := u.ID.Encode()
	if err != nil {
		return err
	}

	return tx.Bucket(userpasswordBucket).Put(encodedID, hash)
}

// ComparePassword compares a provided password with the stored password hash.
func (c *Client) ComparePassword(ctx context.Context, name string, password string) error {
	return c.db.View(func(tx *bolt.Tx) error {
		return c.comparePassword(ctx, tx, name, password)
	})
}
func (c *Client) comparePassword(ctx context.Context, tx *bolt.Tx, name string, password string) error {
	u, err := c.findUserByName(ctx, tx, name)
	if err != nil {
		return err
	}

	encodedID, err := u.ID.Encode()
	if err != nil {
		return err
	}

	hash := tx.Bucket(userpasswordBucket).Get(encodedID)

	return bcrypt.CompareHashAndPassword(hash, []byte(password))
}

// CompareAndSetPassword replaces the old password with the new password if thee old password is correct.
func (c *Client) CompareAndSetPassword(ctx context.Context, name string, old string, new string) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		if err := c.comparePassword(ctx, tx, name, old); err != nil {
			return err
		}
		return c.setPassword(ctx, tx, name, new)
	})
}

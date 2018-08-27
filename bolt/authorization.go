package bolt

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/coreos/bbolt"
	"github.com/influxdata/platform"
)

var (
	authorizationBucket = []byte("authorizationsv1")
	authorizationIndex  = []byte("authorizationindexv1")
)

var _ platform.AuthorizationService = (*Client)(nil)

func (c *Client) initializeAuthorizations(ctx context.Context, tx *bolt.Tx) error {
	if _, err := tx.CreateBucketIfNotExists([]byte(authorizationBucket)); err != nil {
		return err
	}
	if _, err := tx.CreateBucketIfNotExists([]byte(authorizationIndex)); err != nil {
		return err
	}
	return nil
}

func (c *Client) setUserOnAuthorization(ctx context.Context, tx *bolt.Tx, a *platform.Authorization) error {
	u, err := c.findUserByID(ctx, tx, a.UserID)
	if err != nil {
		return err
	}
	a.User = u.Name
	return nil
}

// FindAuthorizationByID retrieves a authorization by id.
func (c *Client) FindAuthorizationByID(ctx context.Context, id platform.ID) (*platform.Authorization, error) {
	var a *platform.Authorization

	err := c.db.View(func(tx *bolt.Tx) error {
		auth, err := c.findAuthorizationByID(ctx, tx, id)
		if err != nil {
			return err
		}
		a = auth
		return nil
	})

	if err != nil {
		return nil, err
	}

	return a, nil
}

func (c *Client) findAuthorizationByID(ctx context.Context, tx *bolt.Tx, id platform.ID) (*platform.Authorization, error) {
	var a platform.Authorization

	v := tx.Bucket(authorizationBucket).Get(id)

	if len(v) == 0 {
		// TODO: Make standard error
		return nil, fmt.Errorf("authorization not found")
	}

	if err := json.Unmarshal(v, &a); err != nil {
		return nil, err
	}

	if err := c.setUserOnAuthorization(ctx, tx, &a); err != nil {
		return nil, err
	}

	return &a, nil
}

// FindAuthorizationByToken returns a authorization by token for a particular authorization.
func (c *Client) FindAuthorizationByToken(ctx context.Context, n string) (*platform.Authorization, error) {
	var a *platform.Authorization

	err := c.db.View(func(tx *bolt.Tx) error {
		auth, err := c.findAuthorizationByToken(ctx, tx, n)
		if err != nil {
			return err
		}
		a = auth
		return nil
	})

	return a, err
}

func (c *Client) findAuthorizationByToken(ctx context.Context, tx *bolt.Tx, n string) (*platform.Authorization, error) {
	id := tx.Bucket(authorizationIndex).Get(authorizationIndexKey(n))
	return c.findAuthorizationByID(ctx, tx, platform.ID(id))
}

func filterAuthorizationsFn(filter platform.AuthorizationFilter) func(a *platform.Authorization) bool {
	if filter.ID != nil {
		return func(a *platform.Authorization) bool {
			return bytes.Equal(a.ID, *filter.ID)
		}
	}

	if filter.Token != nil {
		return func(a *platform.Authorization) bool {
			return a.Token == *filter.Token
		}
	}

	if filter.UserID != nil {
		return func(a *platform.Authorization) bool {
			return bytes.Equal(a.UserID, *filter.UserID)
		}
	}

	return func(a *platform.Authorization) bool { return true }
}

// FindAuthorizations retrives all authorizations that match an arbitrary authorization filter.
// Filters using ID, or Token should be efficient.
// Other filters will do a linear scan across all authorizations searching for a match.
func (c *Client) FindAuthorizations(ctx context.Context, filter platform.AuthorizationFilter, opt ...platform.FindOptions) ([]*platform.Authorization, int, error) {
	if filter.ID != nil {
		a, err := c.FindAuthorizationByID(ctx, *filter.ID)
		if err != nil {
			return nil, 0, err
		}

		return []*platform.Authorization{a}, 1, nil
	}

	if filter.Token != nil {
		a, err := c.FindAuthorizationByToken(ctx, *filter.Token)
		if err != nil {
			return nil, 0, err
		}

		return []*platform.Authorization{a}, 1, nil
	}

	as := []*platform.Authorization{}
	err := c.db.View(func(tx *bolt.Tx) error {
		auths, err := c.findAuthorizations(ctx, tx, filter)
		if err != nil {
			return err
		}
		as = auths
		return nil
	})

	if err != nil {
		return nil, 0, err
	}

	return as, len(as), nil
}

func (c *Client) findAuthorizations(ctx context.Context, tx *bolt.Tx, f platform.AuthorizationFilter) ([]*platform.Authorization, error) {
	// If the users name was provided, look up user by ID first
	if f.User != nil {
		u, err := c.findUserByName(ctx, tx, *f.User)
		if err != nil {
			return nil, err
		}
		f.UserID = &u.ID
	}

	as := []*platform.Authorization{}
	filterFn := filterAuthorizationsFn(f)
	err := c.forEachAuthorization(ctx, tx, func(a *platform.Authorization) bool {
		if filterFn(a) {
			as = append(as, a)
		}
		return true
	})
	if err != nil {
		return nil, err
	}

	return as, nil
}

// CreateAuthorization creates a platform authorization and sets b.ID, and b.UserID if not provided.
func (c *Client) CreateAuthorization(ctx context.Context, a *platform.Authorization) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		if len(a.UserID) == 0 {
			u, err := c.findUserByName(ctx, tx, a.User)
			if err != nil {
				return err
			}
			a.UserID = u.ID
		}

		unique := c.uniqueAuthorizationToken(ctx, tx, a)

		if !unique {
			// TODO: make standard error
			return fmt.Errorf("token already exists")
		}

		token, err := c.TokenGenerator.Token()
		if err != nil {
			return err
		}
		a.Token = token

		a.ID = c.IDGenerator.ID()

		return c.putAuthorization(ctx, tx, a)
	})
}

// PutAuthorization will put a authorization without setting an ID.
func (c *Client) PutAuthorization(ctx context.Context, a *platform.Authorization) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		return c.putAuthorization(ctx, tx, a)
	})
}

func (c *Client) putAuthorization(ctx context.Context, tx *bolt.Tx, a *platform.Authorization) error {
	a.User = ""
	v, err := json.Marshal(a)
	if err != nil {
		return err
	}

	if err := tx.Bucket(authorizationIndex).Put(authorizationIndexKey(a.Token), a.ID); err != nil {
		return err
	}
	if err := tx.Bucket(authorizationBucket).Put(a.ID, v); err != nil {
	}
	return c.setUserOnAuthorization(ctx, tx, a)
}

func authorizationIndexKey(n string) []byte {
	return []byte(n)
}

// forEachAuthorization will iterate through all authorizations while fn returns true.
func (c *Client) forEachAuthorization(ctx context.Context, tx *bolt.Tx, fn func(*platform.Authorization) bool) error {
	cur := tx.Bucket(authorizationBucket).Cursor()
	for k, v := cur.First(); k != nil; k, v = cur.Next() {
		a := &platform.Authorization{}
		if err := json.Unmarshal(v, a); err != nil {
			return err
		}
		if err := c.setUserOnAuthorization(ctx, tx, a); err != nil {
			return err
		}
		if !fn(a) {
			break
		}
	}

	return nil
}

func (c *Client) uniqueAuthorizationToken(ctx context.Context, tx *bolt.Tx, a *platform.Authorization) bool {
	v := tx.Bucket(authorizationIndex).Get(authorizationIndexKey(a.Token))
	return len(v) == 0
}

// DeleteAuthorization deletes a authorization and prunes it from the index.
func (c *Client) DeleteAuthorization(ctx context.Context, id platform.ID) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		return c.deleteAuthorization(ctx, tx, id)
	})
}

func (c *Client) deleteAuthorization(ctx context.Context, tx *bolt.Tx, id platform.ID) error {
	a, err := c.findAuthorizationByID(ctx, tx, id)
	if err != nil {
		return err
	}
	if err := tx.Bucket(authorizationIndex).Delete(authorizationIndexKey(a.Token)); err != nil {
		return err
	}
	return tx.Bucket(authorizationBucket).Delete(id)
}

// DisableAuthorization disables the authorization by setting the disabled field to true.
func (c *Client) DisableAuthorization(ctx context.Context, id platform.ID) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		isDisabled := true
		return c.updateAuthorization(ctx, tx, id, isDisabled)
	})
}

// EnableAuthorization enables the authorization by setting the disabled field to false.
func (c *Client) EnableAuthorization(ctx context.Context, id platform.ID) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		isDisabled := false
		return c.updateAuthorization(ctx, tx, id, isDisabled)
	})
}

func (c *Client) updateAuthorization(ctx context.Context, tx *bolt.Tx, id platform.ID, isDisabled bool) error {
	a, err := c.findAuthorizationByID(ctx, tx, id)
	if err != nil {
		return err
	}

	a.Disabled = isDisabled
	b, err := json.Marshal(a)
	if err != nil {
		return err
	}

	return tx.Bucket(authorizationBucket).Put(a.ID, b)
}

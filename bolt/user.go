package bolt

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/coreos/bbolt"
	"github.com/influxdata/platform"
	platformcontext "github.com/influxdata/platform/context"
)

var (
	userUser           = []byte("usersv1")
	userIndex          = []byte("userindexv1")
	userpasswordBucket = []byte("userspasswordv1")
)

var _ platform.UserService = (*Client)(nil)
var _ platform.UserOperationLogService = (*Client)(nil)
var _ platform.BasicAuthService = (*Client)(nil)

func (c *Client) initializeUsers(ctx context.Context, tx *bolt.Tx) error {
	if _, err := tx.CreateBucketIfNotExists([]byte(userUser)); err != nil {
		return err
	}
	if _, err := tx.CreateBucketIfNotExists([]byte(userIndex)); err != nil {
		return err
	}
	if _, err := tx.CreateBucketIfNotExists([]byte(userpasswordBucket)); err != nil {
		return err
	}
	return nil
}

// FindUserByID retrieves a user by id.
func (c *Client) FindUserByID(ctx context.Context, id platform.ID) (*platform.User, error) {
	var u *platform.User

	err := c.db.View(func(tx *bolt.Tx) error {
		usr, err := c.findUserByID(ctx, tx, id)
		if err != nil {
			return err
		}
		u = usr
		return nil
	})

	if err != nil {
		return nil, err
	}

	return u, nil
}

func (c *Client) findUserByID(ctx context.Context, tx *bolt.Tx, id platform.ID) (*platform.User, error) {
	encodedID, err := id.Encode()
	if err != nil {
		return nil, err
	}

	var u platform.User
	v := tx.Bucket(userUser).Get(encodedID)

	if len(v) == 0 {
		// TODO: Make standard error
		return nil, fmt.Errorf("user not found")
	}

	if err := json.Unmarshal(v, &u); err != nil {
		return nil, err
	}

	return &u, nil
}

// FindUserByName returns a user by name for a particular user.
func (c *Client) FindUserByName(ctx context.Context, n string) (*platform.User, error) {
	var u *platform.User

	err := c.db.View(func(tx *bolt.Tx) error {
		usr, err := c.findUserByName(ctx, tx, n)
		if err != nil {
			return err
		}
		u = usr
		return nil
	})

	return u, err
}

func (c *Client) findUserByName(ctx context.Context, tx *bolt.Tx, n string) (*platform.User, error) {
	u := tx.Bucket(userIndex).Get(userIndexKey(n))
	if u == nil {
		// TODO: Make standard error
		return nil, fmt.Errorf("user not found")
	}

	var id platform.ID
	if err := id.Decode(u); err != nil {
		return nil, err
	}
	return c.findUserByID(ctx, tx, id)
}

// FindUser retrives a user using an arbitrary user filter.
// Filters using ID, or Name should be efficient.
// Other filters will do a linear scan across users until it finds a match.
func (c *Client) FindUser(ctx context.Context, filter platform.UserFilter) (*platform.User, error) {
	if filter.ID != nil {
		return c.FindUserByID(ctx, *filter.ID)
	}

	if filter.Name != nil {
		return c.FindUserByName(ctx, *filter.Name)
	}

	filterFn := filterUsersFn(filter)

	var u *platform.User
	err := c.db.View(func(tx *bolt.Tx) error {
		return forEachUser(ctx, tx, func(usr *platform.User) bool {
			if filterFn(usr) {
				u = usr
				return false
			}
			return true
		})
	})

	if err != nil {
		return nil, err
	}

	if u == nil {
		return nil, fmt.Errorf("user not found")
	}

	return u, nil
}

func filterUsersFn(filter platform.UserFilter) func(u *platform.User) bool {
	if filter.ID != nil {
		return func(u *platform.User) bool {
			return u.ID.Valid() && u.ID == *filter.ID
		}
	}

	if filter.Name != nil {
		return func(u *platform.User) bool {
			return u.Name == *filter.Name
		}
	}

	return func(u *platform.User) bool { return true }
}

// FindUsers retrives all users that match an arbitrary user filter.
// Filters using ID, or Name should be efficient.
// Other filters will do a linear scan across all users searching for a match.
func (c *Client) FindUsers(ctx context.Context, filter platform.UserFilter, opt ...platform.FindOptions) ([]*platform.User, int, error) {
	if filter.ID != nil {
		u, err := c.FindUserByID(ctx, *filter.ID)
		if err != nil {
			return nil, 0, err
		}

		return []*platform.User{u}, 1, nil
	}

	if filter.Name != nil {
		u, err := c.FindUserByName(ctx, *filter.Name)
		if err != nil {
			return nil, 0, err
		}

		return []*platform.User{u}, 1, nil
	}

	us := []*platform.User{}
	filterFn := filterUsersFn(filter)
	err := c.db.View(func(tx *bolt.Tx) error {
		return forEachUser(ctx, tx, func(u *platform.User) bool {
			if filterFn(u) {
				us = append(us, u)
			}
			return true
		})
	})

	if err != nil {
		return nil, 0, err
	}

	return us, len(us), nil
}

// CreateUser creates a platform user and sets b.ID.
func (c *Client) CreateUser(ctx context.Context, u *platform.User) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		unique := c.uniqueUserName(ctx, tx, u)

		if !unique {
			// TODO: make standard error
			return fmt.Errorf("user with name %s already exists", u.Name)
		}

		u.ID = c.IDGenerator.ID()

		if err := c.appendUserEventToLog(ctx, tx, u.ID, userCreatedEvent); err != nil {
			return err
		}

		return c.putUser(ctx, tx, u)
	})
}

// PutUser will put a user without setting an ID.
func (c *Client) PutUser(ctx context.Context, u *platform.User) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		return c.putUser(ctx, tx, u)
	})
}

func (c *Client) putUser(ctx context.Context, tx *bolt.Tx, u *platform.User) error {
	v, err := json.Marshal(u)
	if err != nil {
		return err
	}
	encodedID, err := u.ID.Encode()
	if err != nil {
		return err
	}
	if err := tx.Bucket(userIndex).Put(userIndexKey(u.Name), encodedID); err != nil {
		return err
	}
	return tx.Bucket(userUser).Put(encodedID, v)
}

func userIndexKey(n string) []byte {
	return []byte(n)
}

// forEachUser will iterate through all users while fn returns true.
func forEachUser(ctx context.Context, tx *bolt.Tx, fn func(*platform.User) bool) error {
	cur := tx.Bucket(userUser).Cursor()
	for k, v := cur.First(); k != nil; k, v = cur.Next() {
		u := &platform.User{}
		if err := json.Unmarshal(v, u); err != nil {
			return err
		}
		if !fn(u) {
			break
		}
	}

	return nil
}

func (c *Client) uniqueUserName(ctx context.Context, tx *bolt.Tx, u *platform.User) bool {
	v := tx.Bucket(userIndex).Get(userIndexKey(u.Name))
	return len(v) == 0
}

// UpdateUser updates a user according the parameters set on upd.
func (c *Client) UpdateUser(ctx context.Context, id platform.ID, upd platform.UserUpdate) (*platform.User, error) {
	var u *platform.User
	err := c.db.Update(func(tx *bolt.Tx) error {
		usr, err := c.updateUser(ctx, tx, id, upd)
		if err != nil {
			return err
		}
		u = usr
		return nil
	})

	return u, err
}

func (c *Client) updateUser(ctx context.Context, tx *bolt.Tx, id platform.ID, upd platform.UserUpdate) (*platform.User, error) {
	u, err := c.findUserByID(ctx, tx, id)
	if err != nil {
		return nil, err
	}

	if upd.Name != nil {
		// Users are indexed by name and so the user index must be pruned
		// when name is modified.
		if err := tx.Bucket(userIndex).Delete(userIndexKey(u.Name)); err != nil {
			return nil, err
		}
		u.Name = *upd.Name
	}

	if err := c.appendUserEventToLog(ctx, tx, u.ID, userUpdatedEvent); err != nil {
		return nil, err
	}

	if err := c.putUser(ctx, tx, u); err != nil {
		return nil, err
	}

	return u, nil
}

// DeleteUser deletes a user and prunes it from the index.
func (c *Client) DeleteUser(ctx context.Context, id platform.ID) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		if err := c.deleteUsersAuthorizations(ctx, tx, id); err != nil {
			return err
		}
		return c.deleteUser(ctx, tx, id)
	})
}

func (c *Client) deleteUser(ctx context.Context, tx *bolt.Tx, id platform.ID) error {
	u, err := c.findUserByID(ctx, tx, id)
	if err != nil {
		return err
	}
	encodedID, err := id.Encode()
	if err != nil {
		return err
	}
	if err := tx.Bucket(userIndex).Delete(userIndexKey(u.Name)); err != nil {
		return err
	}
	if err := tx.Bucket(userUser).Delete(encodedID); err != nil {
		return err
	}
	return c.deleteUserResourceMappings(ctx, tx, platform.UserResourceMappingFilter{
		UserID: id,
	})
}

func (c *Client) deleteUsersAuthorizations(ctx context.Context, tx *bolt.Tx, id platform.ID) error {
	authFilter := platform.AuthorizationFilter{
		UserID: &id,
	}
	as, err := c.findAuthorizations(ctx, tx, authFilter)
	if err != nil {
		return err
	}
	for _, a := range as {
		if err := c.deleteAuthorization(ctx, tx, a.ID); err != nil {
			return err
		}
	}
	return nil
}

// GeUserOperationLog retrieves a user operation log.
func (c *Client) GetUserOperationLog(ctx context.Context, id platform.ID, opts platform.FindOptions) ([]*platform.OperationLogEntry, int, error) {
	// TODO(desa): might be worthwhile to allocate a slice of size opts.Limit
	log := []*platform.OperationLogEntry{}

	err := c.db.View(func(tx *bolt.Tx) error {
		key, err := encodeBucketOperationLogKey(id)
		if err != nil {
			return err
		}

		return c.forEachLogEntry(ctx, tx, key, opts, func(v []byte, t time.Time) error {
			e := &platform.OperationLogEntry{}
			if err := json.Unmarshal(v, e); err != nil {
				return err
			}
			e.Time = t

			log = append(log, e)

			return nil
		})
	})

	if err != nil {
		return nil, 0, err
	}

	return log, len(log), nil
}

// TODO(desa): what do we want these to be?
const (
	userCreatedEvent = "User Created"
	userUpdatedEvent = "User Updated"
)

func encodeUserOperationLogKey(id platform.ID) ([]byte, error) {
	buf, err := id.Encode()
	if err != nil {
		return nil, err
	}
	return append([]byte(bucketOperationLogKeyPrefix), buf...), nil
}

func (c *Client) appendUserEventToLog(ctx context.Context, tx *bolt.Tx, id platform.ID, s string) error {
	e := &platform.OperationLogEntry{
		Description: s,
	}
	// TODO(desa): this is fragile and non explicit since it requires an authorizer to be on context. It should be
	//             replaced with a higher level transaction so that adding to the log can take place in the http handler
	//             where the userID will exist explicitly.
	a, err := platformcontext.GetAuthorizer(ctx)
	if err == nil {
		// Add the user to the log if you can, but don't error if its not there.
		e.UserID = a.GetUserID()
	}

	v, err := json.Marshal(e)
	if err != nil {
		return err
	}

	k, err := encodeUserOperationLogKey(id)
	if err != nil {
		return err
	}

	return c.addLogEntry(ctx, tx, k, v, c.time())
}

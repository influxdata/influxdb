// Note: this file is used as a proof of concept for having a generic
// keyvalue store backed by specific implementations of kv.Store.
package kv

import (
	"context"
	"encoding/json"
	"fmt"

	platform "github.com/influxdata/influxdb"
)

var (
	exampleBucket = []byte("examplesv1")
	exampleIndex  = []byte("exampleindexv1")
)

// ExampleService is an example user like service built on a generic kv store.
type ExampleService struct {
	kv          Store
	idGenerator platform.IDGenerator
}

// NewExampleService creates an instance of an example service.
func NewExampleService(kv Store, idGen platform.IDGenerator) *ExampleService {
	return &ExampleService{
		kv:          kv,
		idGenerator: idGen,
	}
}

// Initialize creates the buckets for the example service
func (c *ExampleService) Initialize() error {
	return c.kv.Update(func(tx Tx) error {
		if _, err := tx.Bucket([]byte(exampleBucket)); err != nil {
			return err
		}
		if _, err := tx.Bucket([]byte(exampleIndex)); err != nil {
			return err
		}
		return nil
	})
}

// FindUserByID retrieves a example by id.
func (c *ExampleService) FindUserByID(ctx context.Context, id platform.ID) (*platform.User, error) {
	var u *platform.User

	err := c.kv.View(func(tx Tx) error {
		usr, err := c.findUserByID(ctx, tx, id)
		if err != nil {
			return err
		}
		u = usr
		return nil
	})

	if err != nil {
		return nil, &platform.Error{
			Op:  "kv/" + platform.OpFindUserByID,
			Err: err,
		}
	}

	return u, nil
}

func (c *ExampleService) findUserByID(ctx context.Context, tx Tx, id platform.ID) (*platform.User, error) {
	encodedID, err := id.Encode()
	if err != nil {
		return nil, err
	}

	b, err := tx.Bucket(exampleBucket)
	if err != nil {
		return nil, err
	}

	v, err := b.Get(encodedID)
	if err == ErrKeyNotFound {
		return nil, &platform.Error{
			Code: platform.ENotFound,
			Msg:  "user not found",
		}
	}
	if err != nil {
		return nil, err
	}

	var u platform.User
	if err := json.Unmarshal(v, &u); err != nil {
		return nil, err
	}

	return &u, nil
}

// FindUserByName returns a example by name for a particular example.
func (c *ExampleService) FindUserByName(ctx context.Context, n string) (*platform.User, error) {
	var u *platform.User

	err := c.kv.View(func(tx Tx) error {
		usr, err := c.findUserByName(ctx, tx, n)
		if err != nil {
			return err
		}
		u = usr
		return nil
	})

	return u, err
}

func (c *ExampleService) findUserByName(ctx context.Context, tx Tx, n string) (*platform.User, error) {
	b, err := tx.Bucket(exampleIndex)
	if err != nil {
		return nil, err
	}
	uid, err := b.Get(exampleIndexKey(n))
	if err == ErrKeyNotFound {
		return nil, &platform.Error{
			Code: platform.ENotFound,
			Msg:  "user not found",
			Op:   "kv/" + platform.OpFindUser,
		}
	}
	if err != nil {
		return nil, err
	}

	var id platform.ID
	if err := id.Decode(uid); err != nil {
		return nil, err
	}
	return c.findUserByID(ctx, tx, id)
}

// FindUser retrives a example using an arbitrary example filter.
// Filters using ID, or Name should be efficient.
// Other filters will do a linear scan across examples until it finds a match.
func (c *ExampleService) FindUser(ctx context.Context, filter platform.UserFilter) (*platform.User, error) {
	if filter.ID != nil {
		u, err := c.FindUserByID(ctx, *filter.ID)
		if err != nil {
			return nil, &platform.Error{
				Op:  "kv/" + platform.OpFindUser,
				Err: err,
			}
		}
		return u, nil
	}

	if filter.Name != nil {
		return c.FindUserByName(ctx, *filter.Name)
	}

	filterFn := filterExamplesFn(filter)

	var u *platform.User
	err := c.kv.View(func(tx Tx) error {
		return forEachExample(ctx, tx, func(usr *platform.User) bool {
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
		return nil, &platform.Error{
			Code: platform.ENotFound,
			Msg:  "user not found",
		}
	}

	return u, nil
}

func filterExamplesFn(filter platform.UserFilter) func(u *platform.User) bool {
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

// FindUsers retrives all examples that match an arbitrary example filter.
// Filters using ID, or Name should be efficient.
// Other filters will do a linear scan across all examples searching for a match.
func (c *ExampleService) FindUsers(ctx context.Context, filter platform.UserFilter, opt ...platform.FindOptions) ([]*platform.User, int, error) {
	op := platform.OpFindUsers
	if filter.ID != nil {
		u, err := c.FindUserByID(ctx, *filter.ID)
		if err != nil {
			return nil, 0, &platform.Error{
				Err: err,
				Op:  "kv/" + op,
			}
		}

		return []*platform.User{u}, 1, nil
	}

	if filter.Name != nil {
		u, err := c.FindUserByName(ctx, *filter.Name)
		if err != nil {
			return nil, 0, &platform.Error{
				Err: err,
				Op:  "kv/" + op,
			}
		}

		return []*platform.User{u}, 1, nil
	}

	us := []*platform.User{}
	filterFn := filterExamplesFn(filter)
	err := c.kv.View(func(tx Tx) error {
		return forEachExample(ctx, tx, func(u *platform.User) bool {
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

// CreateUser creates a platform example and sets b.ID.
func (c *ExampleService) CreateUser(ctx context.Context, u *platform.User) error {
	err := c.kv.Update(func(tx Tx) error {
		unique := c.uniqueExampleName(ctx, tx, u)

		if !unique {
			// TODO: make standard error
			return &platform.Error{
				Code: platform.EConflict,
				Msg:  fmt.Sprintf("user with name %s already exists", u.Name),
			}
		}

		u.ID = c.idGenerator.ID()

		return c.putUser(ctx, tx, u)
	})

	if err != nil {
		return &platform.Error{
			Err: err,
			Op:  "kv/" + platform.OpCreateUser,
		}
	}

	return nil
}

// PutUser will put a example without setting an ID.
func (c *ExampleService) PutUser(ctx context.Context, u *platform.User) error {
	return c.kv.Update(func(tx Tx) error {
		return c.putUser(ctx, tx, u)
	})
}

func (c *ExampleService) putUser(ctx context.Context, tx Tx, u *platform.User) error {
	v, err := json.Marshal(u)
	if err != nil {
		return err
	}
	encodedID, err := u.ID.Encode()
	if err != nil {
		return err
	}

	idx, err := tx.Bucket(exampleIndex)
	if err != nil {
		return err
	}

	if err := idx.Put(exampleIndexKey(u.Name), encodedID); err != nil {
		return err
	}

	b, err := tx.Bucket(exampleBucket)
	if err != nil {
		return err
	}

	return b.Put(encodedID, v)
}

func exampleIndexKey(n string) []byte {
	return []byte(n)
}

// forEachExample will iterate through all examples while fn returns true.
func forEachExample(ctx context.Context, tx Tx, fn func(*platform.User) bool) error {
	b, err := tx.Bucket(exampleBucket)
	if err != nil {
		return err
	}

	cur, err := b.Cursor()
	if err != nil {
		return err
	}

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

func (c *ExampleService) uniqueExampleName(ctx context.Context, tx Tx, u *platform.User) bool {
	idx, err := tx.Bucket(exampleIndex)
	if err != nil {
		return false
	}

	if _, err := idx.Get(exampleIndexKey(u.Name)); err == ErrKeyNotFound {
		return true
	}
	return false
}

// UpdateUser updates a example according the parameters set on upd.
func (c *ExampleService) UpdateUser(ctx context.Context, id platform.ID, upd platform.UserUpdate) (*platform.User, error) {
	var u *platform.User
	err := c.kv.Update(func(tx Tx) error {
		usr, err := c.updateUser(ctx, tx, id, upd)
		if err != nil {
			return err
		}
		u = usr
		return nil
	})

	if err != nil {
		return nil, &platform.Error{
			Err: err,
			Op:  "kv/" + platform.OpUpdateUser,
		}
	}

	return u, nil
}

func (c *ExampleService) updateUser(ctx context.Context, tx Tx, id platform.ID, upd platform.UserUpdate) (*platform.User, error) {
	u, err := c.findUserByID(ctx, tx, id)
	if err != nil {
		return nil, err
	}

	if upd.Name != nil {
		// Examples are indexed by name and so the example index must be pruned
		// when name is modified.
		idx, err := tx.Bucket(exampleIndex)
		if err != nil {
			return nil, err
		}

		if err := idx.Delete(exampleIndexKey(u.Name)); err != nil {
			return nil, err
		}
		u.Name = *upd.Name
	}

	if err := c.putUser(ctx, tx, u); err != nil {
		return nil, err
	}

	return u, nil
}

// DeleteUser deletes a example and prunes it from the index.
func (c *ExampleService) DeleteUser(ctx context.Context, id platform.ID) error {
	err := c.kv.Update(func(tx Tx) error {
		return c.deleteUser(ctx, tx, id)
	})

	if err != nil {
		return &platform.Error{
			Op:  "kv/" + platform.OpDeleteUser,
			Err: err,
		}
	}

	return nil
}

func (c *ExampleService) deleteUser(ctx context.Context, tx Tx, id platform.ID) error {
	u, err := c.findUserByID(ctx, tx, id)
	if err != nil {
		return err
	}
	encodedID, err := id.Encode()
	if err != nil {
		return err
	}

	idx, err := tx.Bucket(exampleIndex)
	if err != nil {
		return err
	}

	if err := idx.Delete(exampleIndexKey(u.Name)); err != nil {
		return err
	}

	b, err := tx.Bucket(exampleBucket)
	if err != nil {
		return err
	}
	if err := b.Delete(encodedID); err != nil {
		return err
	}

	return nil
}

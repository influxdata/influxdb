package kv

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/influxdata/influxdb"
)

var (
	userBucket = []byte("usersv1")
	userIndex  = []byte("userindexv1")
)

var _ influxdb.UserService = (*Service)(nil)

// Initialize creates the buckets for the user service
func (s *Service) initializeUsers(ctx context.Context, tx Tx) error {
	if _, err := tx.Bucket([]byte(userBucket)); err != nil {
		return err
	}
	if _, err := tx.Bucket([]byte(userIndex)); err != nil {
		return err
	}
	if _, err := tx.Bucket([]byte(userIndex)); err != nil {
		return err
	}
	return nil
}

// FindUserByID retrieves a user by id.
func (s *Service) FindUserByID(ctx context.Context, id influxdb.ID) (*influxdb.User, error) {
	var u *influxdb.User

	err := s.kv.View(func(tx Tx) error {
		usr, err := s.findUserByID(ctx, tx, id)
		if err != nil {
			return err
		}
		u = usr
		return nil
	})

	if err != nil {
		return nil, &influxdb.Error{
			Op:  "kv/" + influxdb.OpFindUserByID,
			Err: err,
		}
	}

	return u, nil
}

func (s *Service) findUserByID(ctx context.Context, tx Tx, id influxdb.ID) (*influxdb.User, error) {
	encodedID, err := id.Encode()
	if err != nil {
		return nil, err
	}

	b, err := tx.Bucket(userBucket)
	if err != nil {
		return nil, err
	}

	v, err := b.Get(encodedID)
	if err == ErrKeyNotFound {
		return nil, &influxdb.Error{
			Code: influxdb.ENotFound,
			Msg:  "user not found",
		}
	}
	if err != nil {
		return nil, err
	}

	var u influxdb.User
	if err := json.Unmarshal(v, &u); err != nil {
		return nil, err
	}

	return &u, nil
}

// FindUserByName returns a user by name for a particular user.
func (s *Service) FindUserByName(ctx context.Context, n string) (*influxdb.User, error) {
	var u *influxdb.User

	err := s.kv.View(func(tx Tx) error {
		usr, err := s.findUserByName(ctx, tx, n)
		if err != nil {
			return err
		}
		u = usr
		return nil
	})

	return u, err
}

func (s *Service) findUserByName(ctx context.Context, tx Tx, n string) (*influxdb.User, error) {
	b, err := tx.Bucket(userIndex)
	if err != nil {
		return nil, err
	}
	uid, err := b.Get(userIndexKey(n))
	if err == ErrKeyNotFound {
		return nil, &influxdb.Error{
			Code: influxdb.ENotFound,
			Msg:  "user not found",
			Op:   "kv/" + influxdb.OpFindUser,
		}
	}
	if err != nil {
		return nil, err
	}

	var id influxdb.ID
	if err := id.Decode(uid); err != nil {
		return nil, err
	}
	return s.findUserByID(ctx, tx, id)
}

// FindUser retrives a user using an arbitrary user filter.
// Filters using ID, or Name should be efficient.
// Other filters will do a linear scan across users until it finds a match.
func (s *Service) FindUser(ctx context.Context, filter influxdb.UserFilter) (*influxdb.User, error) {
	if filter.ID != nil {
		u, err := s.FindUserByID(ctx, *filter.ID)
		if err != nil {
			return nil, &influxdb.Error{
				Op:  "kv/" + influxdb.OpFindUser,
				Err: err,
			}
		}
		return u, nil
	}

	if filter.Name != nil {
		return s.FindUserByName(ctx, *filter.Name)
	}

	return nil, &influxdb.Error{
		Code: influxdb.ENotFound,
		Msg:  "user not found",
	}
}

func filterUsersFn(filter influxdb.UserFilter) func(u *influxdb.User) bool {
	if filter.ID != nil {
		return func(u *influxdb.User) bool {
			return u.ID.Valid() && u.ID == *filter.ID
		}
	}

	if filter.Name != nil {
		return func(u *influxdb.User) bool {
			return u.Name == *filter.Name
		}
	}

	return func(u *influxdb.User) bool { return true }
}

// FindUsers retrives all users that match an arbitrary user filter.
// Filters using ID, or Name should be efficient.
// Other filters will do a linear scan across all users searching for a match.
func (s *Service) FindUsers(ctx context.Context, filter influxdb.UserFilter, opt ...influxdb.FindOptions) ([]*influxdb.User, int, error) {
	op := influxdb.OpFindUsers
	if filter.ID != nil {
		u, err := s.FindUserByID(ctx, *filter.ID)
		if err != nil {
			return nil, 0, &influxdb.Error{
				Err: err,
				Op:  "kv/" + op,
			}
		}

		return []*influxdb.User{u}, 1, nil
	}

	if filter.Name != nil {
		u, err := s.FindUserByName(ctx, *filter.Name)
		if err != nil {
			return nil, 0, &influxdb.Error{
				Err: err,
				Op:  "kv/" + op,
			}
		}

		return []*influxdb.User{u}, 1, nil
	}

	us := []*influxdb.User{}
	filterFn := filterUsersFn(filter)
	err := s.kv.View(func(tx Tx) error {
		return forEachUser(ctx, tx, func(u *influxdb.User) bool {
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

// CreateUser creates a influxdb user and sets b.ID.
func (s *Service) CreateUser(ctx context.Context, u *influxdb.User) error {
	err := s.kv.Update(func(tx Tx) error {
		unique := s.uniqueUserName(ctx, tx, u)

		if !unique {
			// TODO: make standard error
			return &influxdb.Error{
				Code: influxdb.EConflict,
				Msg:  fmt.Sprintf("user with name %s already exists", u.Name),
			}
		}

		u.ID = s.IDGenerator.ID()

		return s.putUser(ctx, tx, u)
	})

	if err != nil {
		return &influxdb.Error{
			Err: err,
			Op:  "kv/" + influxdb.OpCreateUser,
		}
	}

	return nil
}

// PutUser will put a user without setting an ID.
func (s *Service) PutUser(ctx context.Context, u *influxdb.User) error {
	return s.kv.Update(func(tx Tx) error {
		return s.putUser(ctx, tx, u)
	})
}

func (s *Service) putUser(ctx context.Context, tx Tx, u *influxdb.User) error {
	v, err := json.Marshal(u)
	if err != nil {
		return err
	}
	encodedID, err := u.ID.Encode()
	if err != nil {
		return err
	}

	idx, err := tx.Bucket(userIndex)
	if err != nil {
		return err
	}

	if err := idx.Put(userIndexKey(u.Name), encodedID); err != nil {
		return err
	}

	b, err := tx.Bucket(userBucket)
	if err != nil {
		return err
	}

	return b.Put(encodedID, v)
}

func userIndexKey(n string) []byte {
	return []byte(n)
}

// forEachUser will iterate through all users while fn returns true.
func forEachUser(ctx context.Context, tx Tx, fn func(*influxdb.User) bool) error {
	b, err := tx.Bucket(userBucket)
	if err != nil {
		return err
	}

	cur, err := b.Cursor()
	if err != nil {
		return err
	}

	for k, v := cur.First(); k != nil; k, v = cur.Next() {
		u := &influxdb.User{}
		if err := json.Unmarshal(v, u); err != nil {
			return err
		}
		if !fn(u) {
			break
		}
	}

	return nil
}

func (s *Service) uniqueUserName(ctx context.Context, tx Tx, u *influxdb.User) bool {
	idx, err := tx.Bucket(userIndex)
	if err != nil {
		return false
	}

	if _, err := idx.Get(userIndexKey(u.Name)); err == ErrKeyNotFound {
		return true
	}
	return false
}

// UpdateUser updates a user according the parameters set on upd.
func (s *Service) UpdateUser(ctx context.Context, id influxdb.ID, upd influxdb.UserUpdate) (*influxdb.User, error) {
	var u *influxdb.User
	err := s.kv.Update(func(tx Tx) error {
		usr, err := s.updateUser(ctx, tx, id, upd)
		if err != nil {
			return err
		}
		u = usr
		return nil
	})

	if err != nil {
		return nil, &influxdb.Error{
			Err: err,
			Op:  "kv/" + influxdb.OpUpdateUser,
		}
	}

	return u, nil
}

func (s *Service) updateUser(ctx context.Context, tx Tx, id influxdb.ID, upd influxdb.UserUpdate) (*influxdb.User, error) {
	u, err := s.findUserByID(ctx, tx, id)
	if err != nil {
		return nil, err
	}

	if upd.Name != nil {
		// Users are indexed by name and so the user index must be pruned
		// when name is modified.
		idx, err := tx.Bucket(userIndex)
		if err != nil {
			return nil, err
		}

		if err := idx.Delete(userIndexKey(u.Name)); err != nil {
			return nil, err
		}
		u.Name = *upd.Name
	}

	if err := s.putUser(ctx, tx, u); err != nil {
		return nil, err
	}

	return u, nil
}

// DeleteUser deletes a user and prunes it from the index.
func (s *Service) DeleteUser(ctx context.Context, id influxdb.ID) error {
	err := s.kv.Update(func(tx Tx) error {
		return s.deleteUser(ctx, tx, id)
	})

	if err != nil {
		return &influxdb.Error{
			Op:  "kv/" + influxdb.OpDeleteUser,
			Err: err,
		}
	}

	return nil
}

func (s *Service) deleteUser(ctx context.Context, tx Tx, id influxdb.ID) error {
	u, err := s.findUserByID(ctx, tx, id)
	if err != nil {
		return err
	}
	encodedID, err := id.Encode()
	if err != nil {
		return err
	}

	idx, err := tx.Bucket(userIndex)
	if err != nil {
		return err
	}

	if err := idx.Delete(userIndexKey(u.Name)); err != nil {
		return err
	}

	b, err := tx.Bucket(userBucket)
	if err != nil {
		return err
	}
	if err := b.Delete(encodedID); err != nil {
		return err
	}

	return nil
}

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

// Initialize creates the buckets for the user service.
func (s *Service) initializeUsers(ctx context.Context, tx Tx) error {
	if _, err := s.userBucket(tx); err != nil {
		return err
	}

	if _, err := s.userIndex(tx); err != nil {
		return err
	}
	return nil
}

func (s *Service) userBucket(tx Tx) (Bucket, error) {
	b, err := tx.Bucket([]byte(userBucket))
	if err != nil {
		return nil, UnexpectedUserBucketError(err)
	}

	return b, nil
}
func (s *Service) userIndex(tx Tx) (Bucket, error) {
	b, err := tx.Bucket([]byte(userIndex))
	if err != nil {
		return nil, UnexpectedUserIndexError(err)
	}

	return b, nil
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
		return nil, err
	}

	return u, nil
}

func (s *Service) findUserByID(ctx context.Context, tx Tx, id influxdb.ID) (*influxdb.User, error) {
	encodedID, err := id.Encode()
	if err != nil {
		return nil, InvalidUserIDError(err)
	}

	b, err := s.userBucket(tx)
	if err != nil {
		return nil, err
	}

	v, err := b.Get(encodedID)
	if IsNotFound(err) {
		return nil, ErrUserNotFound
	}

	if err != nil {
		return nil, ErrInternalUserServiceError(err)
	}

	return UnmarshalUser(v)
}

// UnmarshalUser turns the stored byte slice in the kv into a *influxdb.User.
func UnmarshalUser(v []byte) (*influxdb.User, error) {
	u := &influxdb.User{}
	if err := json.Unmarshal(v, u); err != nil {
		return nil, ErrCorruptUser(err)
	}

	return u, nil
}

// MarshalUser turns an *influxdb.User into a byte slice.
func MarshalUser(u *influxdb.User) ([]byte, error) {
	v, err := json.Marshal(u)
	if err != nil {
		return nil, ErrUnprocessableUser(err)
	}

	return v, nil
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
	b, err := s.userIndex(tx)
	if err != nil {
		return nil, err
	}

	uid, err := b.Get(userIndexKey(n))
	if err == ErrKeyNotFound {
		return nil, ErrUserNotFound
	}
	if err != nil {
		return nil, ErrInternalUserServiceError(err)
	}

	var id influxdb.ID
	if err := id.Decode(uid); err != nil {
		return nil, ErrCorruptUserID(err)
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
			return nil, err
		}
		return u, nil
	}

	if filter.Name != nil {
		return s.FindUserByName(ctx, *filter.Name)
	}

	return nil, ErrUserNotFound
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
	if filter.ID != nil {
		u, err := s.FindUserByID(ctx, *filter.ID)
		if err != nil {
			return nil, 0, err
		}

		return []*influxdb.User{u}, 1, nil
	}

	if filter.Name != nil {
		u, err := s.FindUserByName(ctx, *filter.Name)
		if err != nil {
			return nil, 0, err
		}

		return []*influxdb.User{u}, 1, nil
	}

	us := []*influxdb.User{}
	filterFn := filterUsersFn(filter)
	err := s.kv.View(func(tx Tx) error {
		return s.forEachUser(ctx, tx, func(u *influxdb.User) bool {
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
	return s.kv.Update(func(tx Tx) error {
		unique := s.uniqueUserName(ctx, tx, u)

		if !unique {
			return ErrUserWithNameAlreadyExists(u.Name)
		}

		u.ID = s.IDGenerator.ID()

		return s.putUser(ctx, tx, u)
	})
}

// PutUser will put a user without setting an ID.
func (s *Service) PutUser(ctx context.Context, u *influxdb.User) error {
	return s.kv.Update(func(tx Tx) error {
		return s.putUser(ctx, tx, u)
	})
}

func (s *Service) putUser(ctx context.Context, tx Tx, u *influxdb.User) error {
	v, err := MarshalUser(u)
	if err != nil {
		return err
	}
	encodedID, err := u.ID.Encode()
	if err != nil {
		return InvalidUserIDError(err)
	}

	idx, err := s.userIndex(tx)
	if err != nil {
		return err
	}

	if err := idx.Put(userIndexKey(u.Name), encodedID); err != nil {
		return ErrInternalUserServiceError(err)
	}

	b, err := s.userBucket(tx)
	if err != nil {
		return err
	}

	if err := b.Put(encodedID, v); err != nil {
		return ErrInternalUserServiceError(err)
	}

	return nil
}

func userIndexKey(n string) []byte {
	return []byte(n)
}

// forEachUser will iterate through all users while fn returns true.
func (s *Service) forEachUser(ctx context.Context, tx Tx, fn func(*influxdb.User) bool) error {
	b, err := s.userBucket(tx)
	if err != nil {
		return err
	}

	cur, err := b.Cursor()
	if err != nil {
		return ErrInternalUserServiceError(err)
	}

	for k, v := cur.First(); k != nil; k, v = cur.Next() {
		u, err := UnmarshalUser(v)
		if err != nil {
			return err
		}
		if !fn(u) {
			break
		}
	}

	return nil
}

func (s *Service) uniqueUserName(ctx context.Context, tx Tx, u *influxdb.User) bool {
	idx, err := s.userIndex(tx)
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
		return nil, err
	}

	return u, nil
}

func (s *Service) updateUser(ctx context.Context, tx Tx, id influxdb.ID, upd influxdb.UserUpdate) (*influxdb.User, error) {
	u, err := s.findUserByID(ctx, tx, id)
	if err != nil {
		return nil, err
	}

	if upd.Name != nil {
		if err := s.removeUserFromIndex(ctx, tx, id, *upd.Name); err != nil {
			return nil, err
		}

		u.Name = *upd.Name
	}

	if err := s.putUser(ctx, tx, u); err != nil {
		return nil, err
	}

	return u, nil
}

func (s *Service) removeUserFromIndex(ctx context.Context, tx Tx, id influxdb.ID, name string) error {
	// Users are indexed by name and so the user index must be pruned
	// when name is modified.
	idx, err := s.userIndex(tx)
	if err != nil {
		return err
	}

	if err := idx.Delete(userIndexKey(name)); err != nil {
		return ErrInternalUserServiceError(err)
	}

	return nil
}

// DeleteUser deletes a user and prunes it from the index.
func (s *Service) DeleteUser(ctx context.Context, id influxdb.ID) error {
	return s.kv.Update(func(tx Tx) error {
		return s.deleteUser(ctx, tx, id)
	})
}

func (s *Service) deleteUser(ctx context.Context, tx Tx, id influxdb.ID) error {
	u, err := s.findUserByID(ctx, tx, id)
	if err != nil {
		return err
	}
	encodedID, err := id.Encode()
	if err != nil {
		return InvalidUserIDError(err)
	}

	idx, err := s.userIndex(tx)
	if err != nil {
		return err
	}

	if err := idx.Delete(userIndexKey(u.Name)); err != nil {
		return ErrInternalUserServiceError(err)
	}

	b, err := s.userBucket(tx)
	if err != nil {
		return err
	}
	if err := b.Delete(encodedID); err != nil {
		return ErrInternalUserServiceError(err)
	}

	return nil
}

var (
	// ErrUserNotFound is used when the user is not found.
	ErrUserNotFound = &influxdb.Error{
		Msg:  "user not found",
		Code: influxdb.ENotFound,
	}
)

// ErrInternalUserServiceError is used when the error comes from an internal system.
func ErrInternalUserServiceError(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInternal,
		Err:  err,
	}
}

// ErrUserWithNameAlreadyExists is used when attempting to create a user with a name
// that already exists.
func ErrUserWithNameAlreadyExists(n string) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EConflict,
		Msg:  fmt.Sprintf("user with name %s already exists", n),
	}
}

// UnexpectedUserBucketError is used when the error comes from an internal system.
func UnexpectedUserBucketError(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInternal,
		Msg:  "unexpected error retrieving user bucket",
		Err:  err,
		Op:   "kv/userBucket",
	}
}

// UnexpectedUserIndexError is used when the error comes from an internal system.
func UnexpectedUserIndexError(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInternal,
		Msg:  "unexpected error retrieving user index",
		Err:  err,
		Op:   "kv/userIndex",
	}
}

// InvalidUserIDError is used when a service was provided an invalid ID.
// This is some sort of internal server error.
func InvalidUserIDError(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInvalid,
		Msg:  "user id provided is invalid",
		Err:  err,
	}
}

// ErrCorruptUserID the ID stored in the Store is corrupt.
func ErrCorruptUserID(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInvalid,
		Msg:  "corrupt ID provided",
		Err:  err,
	}
}

// ErrCorruptUser is used when the user cannot be unmarshalled from the bytes
// stored in the kv.
func ErrCorruptUser(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInternal,
		Msg:  "user could not be unmarshalled",
		Err:  err,
		Op:   "kv/UnmarshalUser",
	}
}

// UnprocessableUserError is used when a user is not able to be processed.
func ErrUnprocessableUser(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EUnprocessableEntity,
		Msg:  "user could not be marshalled",
		Err:  err,
		Op:   "kv/MarshalUser",
	}
}

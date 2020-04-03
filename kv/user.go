package kv

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/influxdata/influxdb/v2"
	icontext "github.com/influxdata/influxdb/v2/context"
)

var (
	userBucket = []byte("usersv1")
	userIndex  = []byte("userindexv1")
)

var _ influxdb.UserService = (*Service)(nil)
var _ influxdb.UserOperationLogService = (*Service)(nil)

// Initialize creates the buckets for the user service.
func (s *Service) initializeUsers(ctx context.Context, tx Tx) error {
	if _, err := s.userBucket(tx); err != nil {
		return err
	}

	if _, err := s.userIndexBucket(tx); err != nil {
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

func (s *Service) userIndexBucket(tx Tx) (Bucket, error) {
	b, err := tx.Bucket([]byte(userIndex))
	if err != nil {
		return nil, UnexpectedUserIndexError(err)
	}

	return b, nil
}

// FindUserByID retrieves a user by id.
func (s *Service) FindUserByID(ctx context.Context, id influxdb.ID) (*influxdb.User, error) {
	var u *influxdb.User

	err := s.kv.View(ctx, func(tx Tx) error {
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

	err := s.kv.View(ctx, func(tx Tx) error {
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
	b, err := s.userIndexBucket(tx)
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
	err := s.kv.View(ctx, func(tx Tx) error {
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
	return s.kv.Update(ctx, func(tx Tx) error {
		return s.createUser(ctx, tx, u)
	})
}

// CreateUserTx is used when importing kv as a library
func (s *Service) CreateUserTx(ctx context.Context, tx Tx, u *influxdb.User) error {
	return s.createUser(ctx, tx, u)
}

func (s *Service) createUser(ctx context.Context, tx Tx, u *influxdb.User) error {
	if err := s.uniqueUserName(ctx, tx, u); err != nil {
		return err
	}

	u.ID = s.IDGenerator.ID()
	u.Status = influxdb.Active
	if err := s.appendUserEventToLog(ctx, tx, u.ID, userCreatedEvent); err != nil {
		return err
	}

	return s.putUser(ctx, tx, u)
}

// PutUser will put a user without setting an ID.
func (s *Service) PutUser(ctx context.Context, u *influxdb.User) error {
	return s.kv.Update(ctx, func(tx Tx) error {
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

	idx, err := s.userIndexBucket(tx)
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

	cur, err := b.ForwardCursor(nil)
	if err != nil {
		return ErrInternalUserServiceError(err)
	}

	for k, v := cur.Next(); k != nil; k, v = cur.Next() {
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

func (s *Service) uniqueUserName(ctx context.Context, tx Tx, u *influxdb.User) error {
	key := userIndexKey(u.Name)

	// if the name is not unique across all users in all organizations, then,
	// do not allow creation.
	err := s.unique(ctx, tx, userIndex, key)
	if err == NotUniqueError {
		return UserAlreadyExistsError(u.Name)
	}
	return err
}

// UpdateUser updates a user according the parameters set on upd.
func (s *Service) UpdateUser(ctx context.Context, id influxdb.ID, upd influxdb.UserUpdate) (*influxdb.User, error) {
	var u *influxdb.User
	err := s.kv.Update(ctx, func(tx Tx) error {
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

	if upd.Status != nil {
		if *upd.Status != u.Status && *upd.Status == "inactive" {
			// Disable tasks
			tasks, _, err := s.findTasksByUser(ctx, tx, influxdb.TaskFilter{User: &id})
			if err != nil {
				return nil, err
			}
			status := influxdb.TaskStatusInactive

			for _, task := range tasks {
				_, err := s.UpdateTask(ctx, task.ID, influxdb.TaskUpdate{Status: &status})
				if err != nil {
					return nil, err
				}
			}
		}
		u.Status = *upd.Status
	}

	if err := s.appendUserEventToLog(ctx, tx, u.ID, userUpdatedEvent); err != nil {
		return nil, err
	}

	if err := s.putUser(ctx, tx, u); err != nil {
		return nil, err
	}

	return u, nil
}

func (s *Service) removeUserFromIndex(ctx context.Context, tx Tx, id influxdb.ID, name string) error {
	// Users are indexed by name and so the user index must be pruned
	// when name is modified.
	idx, err := s.userIndexBucket(tx)
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
	return s.kv.Update(ctx, func(tx Tx) error {
		return s.deleteUser(ctx, tx, id)
	})
}

func (s *Service) deleteUser(ctx context.Context, tx Tx, id influxdb.ID) error {
	u, err := s.findUserByID(ctx, tx, id)
	if err != nil {
		return err
	}

	if err := s.deleteUsersAuthorizations(ctx, tx, id); err != nil {
		return err
	}

	encodedID, err := id.Encode()
	if err != nil {
		return InvalidUserIDError(err)
	}

	idx, err := s.userIndexBucket(tx)
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

	if err := s.deleteUserResourceMappings(ctx, tx, influxdb.UserResourceMappingFilter{
		UserID: id,
	}); err != nil {
		return err
	}

	return nil
}

func (s *Service) deleteUsersAuthorizations(ctx context.Context, tx Tx, id influxdb.ID) error {
	authFilter := influxdb.AuthorizationFilter{
		UserID: &id,
	}
	as, err := s.findAuthorizations(ctx, tx, authFilter)
	if err != nil {
		return err
	}
	for _, a := range as {
		if err := s.deleteAuthorization(ctx, tx, a.ID); err != nil {
			return err
		}
	}
	return nil
}

// GetUserOperationLog retrieves a user operation log.
func (s *Service) GetUserOperationLog(ctx context.Context, id influxdb.ID, opts influxdb.FindOptions) ([]*influxdb.OperationLogEntry, int, error) {
	// TODO(desa): might be worthwhile to allocate a slice of size opts.Limit
	log := []*influxdb.OperationLogEntry{}

	err := s.kv.View(ctx, func(tx Tx) error {
		key, err := encodeUserOperationLogKey(id)
		if err != nil {
			return err
		}

		return s.forEachLogEntry(ctx, tx, key, opts, func(v []byte, t time.Time) error {
			e := &influxdb.OperationLogEntry{}
			if err := json.Unmarshal(v, e); err != nil {
				return err
			}
			e.Time = t

			log = append(log, e)

			return nil
		})
	})

	if err != nil && err != errKeyValueLogBoundsNotFound {
		return nil, 0, err
	}

	return log, len(log), nil
}

const userOperationLogKeyPrefix = "user"

// TODO(desa): what do we want these to be?
const (
	userCreatedEvent = "User Created"
	userUpdatedEvent = "User Updated"
)

func encodeUserOperationLogKey(id influxdb.ID) ([]byte, error) {
	buf, err := id.Encode()
	if err != nil {
		return nil, err
	}
	return append([]byte(userOperationLogKeyPrefix), buf...), nil
}

func (s *Service) appendUserEventToLog(ctx context.Context, tx Tx, id influxdb.ID, st string) error {
	e := &influxdb.OperationLogEntry{
		Description: st,
	}
	// TODO(desa): this is fragile and non explicit since it requires an authorizer to be on context. It should be
	//             replaced with a higher level transaction so that adding to the log can take place in the http handler
	//             where the userID will exist explicitly.
	a, err := icontext.GetAuthorizer(ctx)
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

	return s.addLogEntry(ctx, tx, k, v, s.Now())
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

// UserAlreadyExistsError is used when attempting to create a user with a name
// that already exists.
func UserAlreadyExistsError(n string) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EConflict,
		Msg:  fmt.Sprintf("user with name %s already exists", n),
	}
}

// UnexpectedUserBucketError is used when the error comes from an internal system.
func UnexpectedUserBucketError(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInternal,
		Msg:  fmt.Sprintf("unexpected error retrieving user bucket; Err: %v", err),
		Op:   "kv/userBucket",
	}
}

// UnexpectedUserIndexError is used when the error comes from an internal system.
func UnexpectedUserIndexError(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInternal,
		Msg:  fmt.Sprintf("unexpected error retrieving user index; Err: %v", err),
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

// ErrUnprocessableUser is used when a user is not able to be processed.
func ErrUnprocessableUser(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EUnprocessableEntity,
		Msg:  "user could not be marshalled",
		Err:  err,
		Op:   "kv/MarshalUser",
	}
}

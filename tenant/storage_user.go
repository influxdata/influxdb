package tenant

import (
	"context"
	"encoding/json"

	"github.com/influxdata/influxdb/v2/kit/platform"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
)

var (
	userBucket = []byte("usersv1")
	userIndex  = []byte("userindexv1")

	userpasswordBucket = []byte("userspasswordv1")
)

func unmarshalUser(v []byte) (*influxdb.User, error) {
	u := &influxdb.User{}
	if err := json.Unmarshal(v, u); err != nil {
		return nil, ErrCorruptUser(err)
	}

	return u, nil
}

func marshalUser(u *influxdb.User) ([]byte, error) {
	v, err := json.Marshal(u)
	if err != nil {
		return nil, ErrUnprocessableUser(err)
	}

	return v, nil
}

func (s *Store) uniqueUserName(ctx context.Context, tx kv.Tx, uname string) error {

	idx, err := tx.Bucket(userIndex)
	if err != nil {
		return err
	}

	_, err = idx.Get([]byte(uname))
	// if not found then this is  _unique_.
	if kv.IsNotFound(err) {
		return nil
	}

	// no error means this is not unique
	if err == nil {
		return UserAlreadyExistsError(uname)
	}

	// any other error is some sort of internal server error
	return ErrUnprocessableUser(err)
}

func (s *Store) GetUser(ctx context.Context, tx kv.Tx, id platform.ID) (*influxdb.User, error) {
	encodedID, err := id.Encode()
	if err != nil {
		return nil, InvalidUserIDError(err)
	}

	b, err := tx.Bucket(userBucket)
	if err != nil {
		return nil, err
	}

	v, err := b.Get(encodedID)
	if kv.IsNotFound(err) {
		return nil, ErrUserNotFound
	}

	if err != nil {
		return nil, ErrInternalServiceError(err)
	}

	return unmarshalUser(v)
}

func (s *Store) GetUserByName(ctx context.Context, tx kv.Tx, n string) (*influxdb.User, error) {
	b, err := tx.Bucket(userIndex)
	if err != nil {
		return nil, err
	}

	uid, err := b.Get([]byte(n))
	if err == kv.ErrKeyNotFound {
		return nil, ErrUserNotFound
	}

	if err != nil {
		return nil, ErrInternalServiceError(err)
	}

	var id platform.ID
	if err := id.Decode(uid); err != nil {
		return nil, platform.ErrCorruptID(err)
	}
	return s.GetUser(ctx, tx, id)
}

func (s *Store) ListUsers(ctx context.Context, tx kv.Tx, opt ...influxdb.FindOptions) ([]*influxdb.User, error) {
	// if we dont have any options it would be irresponsible to just give back all users in the system
	if len(opt) == 0 {
		opt = append(opt, influxdb.FindOptions{
			Limit: influxdb.DefaultPageSize,
		})
	}
	o := opt[0]
	if o.Limit > influxdb.MaxPageSize || o.Limit == 0 {
		o.Limit = influxdb.MaxPageSize
	}

	b, err := tx.Bucket(userBucket)
	if err != nil {
		return nil, err
	}

	var opts []kv.CursorOption
	if o.Descending {
		opts = append(opts, kv.WithCursorDirection(kv.CursorDescending))
	}

	var seek []byte
	if o.After != nil {
		after := (*o.After) + 1
		seek, err = after.Encode()
		if err != nil {
			return nil, err
		}
	}

	cursor, err := b.ForwardCursor(seek, opts...)
	if err != nil {
		return nil, err
	}
	defer cursor.Close()

	count := 0
	us := []*influxdb.User{}
	for k, v := cursor.Next(); k != nil; k, v = cursor.Next() {
		if o.Offset != 0 && count < o.Offset {
			count++
			continue
		}
		u, err := unmarshalUser(v)
		if err != nil {
			continue
		}

		us = append(us, u)

		if len(us) >= o.Limit {
			break
		}
	}

	return us, cursor.Err()
}

func (s *Store) CreateUser(ctx context.Context, tx kv.Tx, u *influxdb.User) error {
	if !u.ID.Valid() {
		u.ID = s.IDGen.ID()
	}

	encodedID, err := u.ID.Encode()
	if err != nil {
		return InvalidUserIDError(err)
	}

	if err := s.uniqueUserName(ctx, tx, u.Name); err != nil {
		return err
	}

	idx, err := tx.Bucket(userIndex)
	if err != nil {
		return err
	}

	b, err := tx.Bucket(userBucket)
	if err != nil {
		return err
	}

	v, err := marshalUser(u)
	if err != nil {
		return err
	}

	if err := idx.Put([]byte(u.Name), encodedID); err != nil {
		return ErrInternalServiceError(err)
	}

	if err := b.Put(encodedID, v); err != nil {
		return ErrInternalServiceError(err)
	}

	return nil
}

func (s *Store) UpdateUser(ctx context.Context, tx kv.Tx, id platform.ID, upd influxdb.UserUpdate) (*influxdb.User, error) {
	encodedID, err := id.Encode()
	if err != nil {
		return nil, err
	}

	u, err := s.GetUser(ctx, tx, id)
	if err != nil {
		return nil, err
	}

	if upd.Name != nil && *upd.Name != u.Name {
		if err := s.uniqueUserName(ctx, tx, *upd.Name); err != nil {
			return nil, err
		}

		idx, err := tx.Bucket(userIndex)
		if err != nil {
			return nil, err
		}

		if err := idx.Delete([]byte(u.Name)); err != nil {
			return nil, ErrInternalServiceError(err)
		}

		u.Name = *upd.Name

		if err := idx.Put([]byte(u.Name), encodedID); err != nil {
			return nil, ErrInternalServiceError(err)
		}
	}

	if upd.Status != nil {
		u.Status = *upd.Status
	}

	v, err := marshalUser(u)
	if err != nil {
		return nil, err
	}

	b, err := tx.Bucket(userBucket)
	if err != nil {
		return nil, err
	}
	if err := b.Put(encodedID, v); err != nil {
		return nil, ErrInternalServiceError(err)
	}

	return u, nil
}

func (s *Store) DeleteUser(ctx context.Context, tx kv.Tx, id platform.ID) error {
	u, err := s.GetUser(ctx, tx, id)
	if err != nil {
		return err
	}

	encodedID, err := id.Encode()
	if err != nil {
		return InvalidUserIDError(err)
	}

	idx, err := tx.Bucket(userIndex)
	if err != nil {
		return err
	}

	if err := idx.Delete([]byte(u.Name)); err != nil {
		return ErrInternalServiceError(err)
	}

	b, err := tx.Bucket(userBucket)
	if err != nil {
		return err
	}

	if err := b.Delete(encodedID); err != nil {
		return ErrInternalServiceError(err)
	}

	// Clean up users password.
	ub, err := tx.Bucket(userpasswordBucket)
	if err != nil {
		return UnavailablePasswordServiceError(err)
	}
	if err := ub.Delete(encodedID); err != nil {
		return err
	}

	// Clean up user URMs.
	urms, err := s.ListURMs(ctx, tx, influxdb.UserResourceMappingFilter{UserID: id})
	if err != nil {
		return err
	}
	// Do not fail fast on error.
	// Try to avoid as much as possible the effects of partial deletion.
	aggErr := NewAggregateError()
	for _, urm := range urms {
		if err := s.DeleteURM(ctx, tx, urm.ResourceID, urm.UserID); err != nil {
			aggErr.Add(err)
		}
	}
	return aggErr.Err()
}

func (s *Store) GetPassword(ctx context.Context, tx kv.Tx, id platform.ID) (string, error) {
	encodedID, err := id.Encode()
	if err != nil {
		return "", InvalidUserIDError(err)
	}

	b, err := tx.Bucket(userpasswordBucket)
	if err != nil {
		return "", UnavailablePasswordServiceError(err)
	}

	passwd, err := b.Get(encodedID)

	return string(passwd), err
}

func (s *Store) SetPassword(ctx context.Context, tx kv.Tx, id platform.ID, password string) error {
	encodedID, err := id.Encode()
	if err != nil {
		return InvalidUserIDError(err)
	}

	b, err := tx.Bucket(userpasswordBucket)
	if err != nil {
		return UnavailablePasswordServiceError(err)
	}

	return b.Put(encodedID, []byte(password))
}

func (s *Store) DeletePassword(ctx context.Context, tx kv.Tx, id platform.ID) error {
	encodedID, err := id.Encode()
	if err != nil {
		return InvalidUserIDError(err)
	}

	b, err := tx.Bucket(userpasswordBucket)
	if err != nil {
		return UnavailablePasswordServiceError(err)
	}

	return b.Delete(encodedID)

}

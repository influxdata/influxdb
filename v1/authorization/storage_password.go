package authorization

import (
	"context"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"

	"github.com/influxdata/influxdb/v2/kv"
)

var (
	passwordBucket = []byte("legacy/authorizationPasswordv1")
)

// UnavailablePasswordServiceError is used if we aren't able to add the
// password to the store, it means the store is not available at the moment
// (e.g. network).
func UnavailablePasswordServiceError(err error) *errors.Error {
	return &errors.Error{
		Code: errors.EInternal,
		Msg:  "unable to access password bucket",
		Err:  err,
	}
}

func (s *Store) GetPassword(ctx context.Context, tx kv.Tx, id platform.ID) (string, error) {
	encodedID, err := id.Encode()
	if err != nil {
		return "", ErrInvalidAuthIDError(err)
	}

	b, err := tx.Bucket(passwordBucket)
	if err != nil {
		return "", UnavailablePasswordServiceError(err)
	}

	passwd, err := b.Get(encodedID)

	return string(passwd), err
}

func (s *Store) SetPassword(ctx context.Context, tx kv.Tx, id platform.ID, password string) error {
	encodedID, err := id.Encode()
	if err != nil {
		return ErrInvalidAuthIDError(err)
	}

	b, err := tx.Bucket(passwordBucket)
	if err != nil {
		return UnavailablePasswordServiceError(err)
	}

	return b.Put(encodedID, []byte(password))
}

func (s *Store) DeletePassword(ctx context.Context, tx kv.Tx, id platform.ID) error {
	encodedID, err := id.Encode()
	if err != nil {
		return ErrInvalidAuthIDError(err)
	}

	b, err := tx.Bucket(passwordBucket)
	if err != nil {
		return UnavailablePasswordServiceError(err)
	}

	return b.Delete(encodedID)

}

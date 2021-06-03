package secret

import (
	"context"
	"encoding/base64"
	"errors"

	"github.com/influxdata/influxdb/v2/kit/platform"
	errors2 "github.com/influxdata/influxdb/v2/kit/platform/errors"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
)

var secretBucket = []byte("secretsv1")

// Storage is a store translation layer between the data storage unit and the
// service layer.
type Storage struct {
	store kv.Store
}

// NewStore creates a new storage system
func NewStore(s kv.Store) (*Storage, error) {
	return &Storage{s}, nil
}

func (s *Storage) View(ctx context.Context, fn func(kv.Tx) error) error {
	return s.store.View(ctx, fn)
}

func (s *Storage) Update(ctx context.Context, fn func(kv.Tx) error) error {
	return s.store.Update(ctx, fn)
}

// GetSecret Returns the value of a secret
func (s *Storage) GetSecret(ctx context.Context, tx kv.Tx, orgID platform.ID, k string) (string, error) {
	key, err := encodeSecretKey(orgID, k)
	if err != nil {
		return "", err
	}

	b, err := tx.Bucket(secretBucket)
	if err != nil {
		return "", err
	}

	val, err := b.Get(key)
	if kv.IsNotFound(err) {
		return "", &errors2.Error{
			Code: errors2.ENotFound,
			Msg:  influxdb.ErrSecretNotFound,
		}
	}

	if err != nil {
		return "", err
	}

	v, err := decodeSecretValue(val)
	if err != nil {
		return "", err
	}

	return v, nil
}

// ListSecrets returns a list of secret keys
func (s *Storage) ListSecret(ctx context.Context, tx kv.Tx, orgID platform.ID) ([]string, error) {
	b, err := tx.Bucket(secretBucket)
	if err != nil {
		return nil, err
	}

	prefix, err := orgID.Encode()
	if err != nil {
		return nil, err
	}

	cur, err := b.ForwardCursor(prefix, kv.WithCursorPrefix(prefix))
	if err != nil {
		return nil, err
	}

	keys := []string{}

	err = kv.WalkCursor(ctx, cur, func(k, v []byte) (bool, error) {
		id, key, err := decodeSecretKey(k)
		if err != nil {
			return false, err
		}

		if id != orgID {
			// We've reached the end of the keyspace for the provided orgID
			return false, nil
		}

		keys = append(keys, key)

		return true, nil
	})
	if err != nil {
		return nil, err
	}

	return keys, nil
}

// PutSecret sets a secret in the db.
func (s *Storage) PutSecret(ctx context.Context, tx kv.Tx, orgID platform.ID, k, v string) error {
	key, err := encodeSecretKey(orgID, k)
	if err != nil {
		return err
	}

	val := encodeSecretValue(v)

	b, err := tx.Bucket(secretBucket)
	if err != nil {
		return err
	}

	if err := b.Put(key, val); err != nil {
		return err
	}

	return nil
}

// DeleteSecret removes a secret for the db
func (s *Storage) DeleteSecret(ctx context.Context, tx kv.Tx, orgID platform.ID, k string) error {
	key, err := encodeSecretKey(orgID, k)
	if err != nil {
		return err
	}

	b, err := tx.Bucket(secretBucket)
	if err != nil {
		return err
	}

	return b.Delete(key)
}

func encodeSecretKey(orgID platform.ID, k string) ([]byte, error) {
	buf, err := orgID.Encode()
	if err != nil {
		return nil, err
	}

	key := make([]byte, 0, platform.IDLength+len(k))
	key = append(key, buf...)
	key = append(key, k...)

	return key, nil
}

func decodeSecretKey(key []byte) (platform.ID, string, error) {
	if len(key) < platform.IDLength {
		// This should not happen.
		return platform.InvalidID(), "", errors.New("provided key is too short to contain an ID (please report this error)")
	}

	var id platform.ID
	if err := id.Decode(key[:platform.IDLength]); err != nil {
		return platform.InvalidID(), "", err
	}

	k := string(key[platform.IDLength:])

	return id, k, nil
}

func decodeSecretValue(val []byte) (string, error) {
	// store the secret value base64 encoded so that it's marginally better than plaintext
	v, err := base64.StdEncoding.DecodeString(string(val))
	if err != nil {
		return "", err
	}

	return string(v), nil
}

func encodeSecretValue(v string) []byte {
	val := make([]byte, base64.StdEncoding.EncodedLen(len(v)))
	base64.StdEncoding.Encode(val, []byte(v))
	return val
}

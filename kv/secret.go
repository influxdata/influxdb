package kv

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/influxdata/influxdb"
)

var (
	secretBucket = []byte("secretsv1")
)

var _ influxdb.SecretService = (*Service)(nil)

func (s *Service) initializeSecrets(ctx context.Context, tx Tx) error {
	if _, err := tx.Bucket(secretBucket); err != nil {
		return err
	}
	return nil
}

// LoadSecret retrieves the secret value v found at key k for organization orgID.
func (s *Service) LoadSecret(ctx context.Context, orgID influxdb.ID, k string) (string, error) {
	var v string
	err := s.kv.View(ctx, func(tx Tx) error {
		val, err := s.loadSecret(ctx, tx, orgID, k)
		if err != nil {
			return err
		}

		v = val
		return nil
	})

	if err != nil {
		return "", err
	}

	return v, nil
}

func (s *Service) loadSecret(ctx context.Context, tx Tx, orgID influxdb.ID, k string) (string, error) {
	key, err := encodeSecretKey(orgID, k)
	if err != nil {
		return "", err
	}

	b, err := tx.Bucket(secretBucket)
	if err != nil {
		return "", err
	}

	val, err := b.Get(key)
	if IsNotFound(err) {
		return "", &influxdb.Error{
			Code: influxdb.ENotFound,
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

// GetSecretKeys retrieves all secret keys that are stored for the organization orgID.
func (s *Service) GetSecretKeys(ctx context.Context, orgID influxdb.ID) ([]string, error) {
	var vs []string
	err := s.kv.View(ctx, func(tx Tx) error {
		vals, err := s.getSecretKeys(ctx, tx, orgID)
		if err != nil {
			return err
		}

		vs = vals
		return nil
	})

	if err != nil {
		return nil, err
	}

	return vs, nil
}

func (s *Service) getSecretKeys(ctx context.Context, tx Tx, orgID influxdb.ID) ([]string, error) {
	b, err := tx.Bucket(secretBucket)
	if err != nil {
		return nil, err
	}

	cur, err := b.Cursor()
	if err != nil {
		return nil, err
	}

	prefix, err := orgID.Encode()
	if err != nil {
		return nil, err
	}
	k, _ := cur.Seek(prefix)

	if len(k) == 0 {
		return []string{}, nil
	}

	id, key, err := decodeSecretKey(k)
	if err != nil {
		return nil, err
	}

	if id != orgID {
		return nil, fmt.Errorf("organization has no secret keys")
	}

	keys := []string{key}

	for {
		k, _ = cur.Next()

		if len(k) == 0 {
			// We've reached the end of the keys so we're done
			break
		}

		id, key, err = decodeSecretKey(k)
		if err != nil {
			return nil, err
		}

		if id != orgID {
			// We've reached the end of the keyspace for the provided orgID
			break
		}

		keys = append(keys, key)
	}

	return keys, nil
}

// PutSecret stores the secret pair (k,v) for the organization orgID.
func (s *Service) PutSecret(ctx context.Context, orgID influxdb.ID, k, v string) error {
	return s.kv.Update(ctx, func(tx Tx) error {
		return s.putSecret(ctx, tx, orgID, k, v)
	})
}

func (s *Service) putSecret(ctx context.Context, tx Tx, orgID influxdb.ID, k, v string) error {
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

func encodeSecretKey(orgID influxdb.ID, k string) ([]byte, error) {
	buf, err := orgID.Encode()
	if err != nil {
		return nil, err
	}

	key := make([]byte, 0, influxdb.IDLength+len(k))
	key = append(key, buf...)
	key = append(key, k...)

	return key, nil
}

func decodeSecretKey(key []byte) (influxdb.ID, string, error) {
	if len(key) < influxdb.IDLength {
		// This should not happen.
		return influxdb.InvalidID(), "", errors.New("provided key is too short to contain an ID (please report this error)")
	}

	var id influxdb.ID
	if err := id.Decode(key[:influxdb.IDLength]); err != nil {
		return influxdb.InvalidID(), "", err
	}

	k := string(key[influxdb.IDLength:])

	return id, k, nil
}

func decodeSecretValue(val []byte) (string, error) {
	// store the secret value base64 encoded so that it's marginally better than plaintext
	v := make([]byte, base64.StdEncoding.DecodedLen(len(val)))
	if _, err := base64.StdEncoding.Decode(v, val); err != nil {
		return "", err
	}

	return string(v), nil
}

func encodeSecretValue(v string) []byte {
	val := make([]byte, base64.StdEncoding.EncodedLen(len(v)))
	base64.StdEncoding.Encode(val, []byte(v))
	return val
}

// PutSecrets puts all provided secrets and overwrites any previous values.
func (s *Service) PutSecrets(ctx context.Context, orgID influxdb.ID, m map[string]string) error {
	return s.kv.Update(ctx, func(tx Tx) error {
		keys, err := s.getSecretKeys(ctx, tx, orgID)
		if err != nil {
			return err
		}
		for k, v := range m {
			if err := s.putSecret(ctx, tx, orgID, k, v); err != nil {
				return err
			}
		}
		for _, k := range keys {
			if _, ok := m[k]; !ok {
				if err := s.deleteSecret(ctx, tx, orgID, k); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// PatchSecrets patches all provided secrets and updates any previous values.
func (s *Service) PatchSecrets(ctx context.Context, orgID influxdb.ID, m map[string]string) error {
	return s.kv.Update(ctx, func(tx Tx) error {
		for k, v := range m {
			if err := s.putSecret(ctx, tx, orgID, k, v); err != nil {
				return err
			}
		}
		return nil
	})
}

// DeleteSecret removes secrets from the secret store.
func (s *Service) DeleteSecret(ctx context.Context, orgID influxdb.ID, ks ...string) error {
	return s.kv.Update(ctx, func(tx Tx) error {
		for _, k := range ks {
			if err := s.deleteSecret(ctx, tx, orgID, k); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Service) deleteSecret(ctx context.Context, tx Tx, orgID influxdb.ID, k string) error {
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

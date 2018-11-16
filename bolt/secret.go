package bolt

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"

	bolt "github.com/coreos/bbolt"
	"github.com/influxdata/platform"
)

var (
	secretBucket = []byte("secretsv1")
)

var _ platform.SecretService = (*Client)(nil)

func (c *Client) initializeSecretService(ctx context.Context, tx *bolt.Tx) error {
	if _, err := tx.CreateBucketIfNotExists([]byte(secretBucket)); err != nil {
		return err
	}
	return nil
}

// LoadSecret retrieves the secret value v found at key k for organization orgID.
func (c *Client) LoadSecret(ctx context.Context, orgID platform.ID, k string) (string, error) {
	var v string
	err := c.db.View(func(tx *bolt.Tx) error {
		val, err := c.loadSecret(ctx, tx, orgID, k)
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

func (c *Client) loadSecret(ctx context.Context, tx *bolt.Tx, orgID platform.ID, k string) (string, error) {
	key, err := encodeSecretKey(orgID, k)
	if err != nil {
		return "", err
	}

	val := tx.Bucket(secretBucket).Get(key)
	if len(val) == 0 {
		return "", fmt.Errorf("secret not found")
	}

	v, err := decodeSecretValue(val)
	if err != nil {
		return "", err
	}

	return v, nil
}

// GetSecretKeys retrieves all secret keys that are stored for the organization orgID.
func (c *Client) GetSecretKeys(ctx context.Context, orgID platform.ID) ([]string, error) {
	var vs []string
	err := c.db.View(func(tx *bolt.Tx) error {
		vals, err := c.getSecretKeys(ctx, tx, orgID)
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

func (c *Client) getSecretKeys(ctx context.Context, tx *bolt.Tx, orgID platform.ID) ([]string, error) {
	cur := tx.Bucket(secretBucket).Cursor()
	prefix, err := orgID.Encode()
	if err != nil {
		return nil, err
	}
	k, _ := cur.Seek(prefix)

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
func (c *Client) PutSecret(ctx context.Context, orgID platform.ID, k, v string) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		return c.putSecret(ctx, tx, orgID, k, v)
	})
}

func (c *Client) putSecret(ctx context.Context, tx *bolt.Tx, orgID platform.ID, k, v string) error {
	key, err := encodeSecretKey(orgID, k)
	if err != nil {
		return err
	}

	val := encodeSecretValue(v)

	if err := tx.Bucket(secretBucket).Put(key, val); err != nil {
		return err
	}
	return nil
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
		return platform.InvalidID(), "", errors.New("provided key is too short to contain an ID. Please report this error")
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
func (c *Client) PutSecrets(ctx context.Context, orgID platform.ID, m map[string]string) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		keys, err := c.getSecretKeys(ctx, tx, orgID)
		if err != nil {
			return err
		}
		for k, v := range m {
			if err := c.putSecret(ctx, tx, orgID, k, v); err != nil {
				return err
			}
		}
		for _, k := range keys {
			if _, ok := m[k]; !ok {
				if err := c.deleteSecret(ctx, tx, orgID, k); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// PatchSecrets patches all provided secrets and updates any previous values.
func (c *Client) PatchSecrets(ctx context.Context, orgID platform.ID, m map[string]string) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		for k, v := range m {
			if err := c.putSecret(ctx, tx, orgID, k, v); err != nil {
				return err
			}
		}
		return nil
	})
}

// DeleteSecret removes secrets from the secret store.
func (c *Client) DeleteSecret(ctx context.Context, orgID platform.ID, ks ...string) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		for _, k := range ks {
			if err := c.deleteSecret(ctx, tx, orgID, k); err != nil {
				return err
			}
		}
		return nil
	})
}

func (c *Client) deleteSecret(ctx context.Context, tx *bolt.Tx, orgID platform.ID, k string) error {
	key, err := encodeSecretKey(orgID, k)
	if err != nil {
		return err
	}
	return tx.Bucket(secretBucket).Delete(key)
}

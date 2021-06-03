package authorization

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"crypto/sha256"
	"io"
	"sync"

	"github.com/influxdata/influxdb/v2/kit/platform"

	"github.com/influxdata/influxdb/v2"
)

// An implementation of influxdb.PasswordsService that will perform
// ComparePassword requests at a reduced cost under certain
// conditions. See ComparePassword for further information.
//
// The cache is only valid for the duration of the process.
type CachingPasswordsService struct {
	inner influxdb.PasswordsService

	mu        sync.RWMutex // protects concurrent access to authCache
	authCache map[platform.ID]authUser
}

func NewCachingPasswordsService(inner influxdb.PasswordsService) *CachingPasswordsService {
	return &CachingPasswordsService{inner: inner, authCache: make(map[platform.ID]authUser)}
}

var _ influxdb.PasswordsService = (*CachingPasswordsService)(nil)

func (c *CachingPasswordsService) SetPassword(ctx context.Context, id platform.ID, password string) error {
	err := c.inner.SetPassword(ctx, id, password)
	if err == nil {
		c.mu.Lock()
		delete(c.authCache, id)
		c.mu.Unlock()
	}
	return err
}

// ComparePassword will attempt to perform the comparison using a lower cost hashing function
// if influxdb.ContextHasPasswordCacheOption returns true for ctx.
func (c *CachingPasswordsService) ComparePassword(ctx context.Context, id platform.ID, password string) error {
	c.mu.RLock()
	au, ok := c.authCache[id]
	c.mu.RUnlock()
	if ok {
		// verify the password using the cached salt and hash
		if bytes.Equal(c.hashWithSalt(au.salt, password), au.hash) {
			return nil
		}

		// fall through to requiring a full bcrypt hash for invalid passwords
	}

	err := c.inner.ComparePassword(ctx, id, password)
	if err != nil {
		return err
	}

	if salt, hashed, err := c.saltedHash(password); err == nil {
		c.mu.Lock()
		c.authCache[id] = authUser{salt: salt, hash: hashed}
		c.mu.Unlock()
	}

	return nil
}

func (c *CachingPasswordsService) CompareAndSetPassword(ctx context.Context, id platform.ID, old, new string) error {
	err := c.inner.CompareAndSetPassword(ctx, id, old, new)
	if err == nil {
		c.mu.Lock()
		delete(c.authCache, id)
		c.mu.Unlock()
	}
	return err
}

// NOTE(sgc): This caching implementation was lifted from the 1.x source
//   https://github.com/influxdata/influxdb/blob/c1e11e732e145fc1a356535ddf3dcb9fb732a22b/services/meta/client.go#L390-L406

const (
	// SaltBytes is the number of bytes used for salts.
	SaltBytes = 32
)

type authUser struct {
	salt []byte
	hash []byte
}

// hashWithSalt returns a salted hash of password using salt.
func (c *CachingPasswordsService) hashWithSalt(salt []byte, password string) []byte {
	hasher := sha256.New()
	hasher.Write(salt)
	hasher.Write([]byte(password))
	return hasher.Sum(nil)
}

// saltedHash returns a salt and salted hash of password.
func (c *CachingPasswordsService) saltedHash(password string) (salt, hash []byte, err error) {
	salt = make([]byte, SaltBytes)
	if _, err := io.ReadFull(crand.Reader, salt); err != nil {
		return nil, nil, err
	}

	return salt, c.hashWithSalt(salt, password), nil
}

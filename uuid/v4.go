package uuid

import (
	"context"
	"time"

	"github.com/influxdata/chronograf"
	uuid "github.com/satori/go.uuid"
)

// V4 implements chronograf.ID
type V4 struct{}

// Generate creates a UUID v4 string
func (i *V4) Generate() (string, error) {
	return uuid.NewV4().String(), nil
}

// APIKey implements chronograf.Authenticator using V4
type APIKey struct {
	Key string
}

// NewAPIKey creates an APIKey with a UUID v4 Key
func NewAPIKey() chronograf.Authenticator {
	v4 := V4{}
	key, _ := v4.Generate()
	return &APIKey{
		Key: key,
	}
}

// Authenticate checks the key against the UUID v4 key
func (k *APIKey) Authenticate(ctx context.Context, key string) (chronograf.Principal, error) {
	if key != k.Key {
		return "", chronograf.ErrAuthentication
	}
	return "admin", nil
}

// Token returns the UUID v4 key
func (k *APIKey) Token(context.Context, chronograf.Principal, time.Duration) (string, error) {
	return k.Key, nil
}

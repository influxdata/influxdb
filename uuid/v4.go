package uuid

import (
	"time"

	"golang.org/x/net/context"

	"github.com/influxdata/mrfusion"
	uuid "github.com/satori/go.uuid"
)

// V4 implements mrfusion.ID
type V4 struct{}

// Generate creates a UUID v4 string
func (i *V4) Generate() (string, error) {
	return uuid.NewV4().String(), nil
}

// APIKey implements mrfusion.Authenticator using V4
type APIKey struct {
	Key string
}

// NewAPIKey creates an APIKey with a UUID v4 Key
func NewAPIKey() mrfusion.Authenticator {
	v4 := V4{}
	key, _ := v4.Generate()
	return &APIKey{
		Key: key,
	}
}

// Authenticate checks the key against the UUID v4 key
func (k *APIKey) Authenticate(ctx context.Context, key string) (mrfusion.Principal, error) {
	if key != k.Key {
		return "", mrfusion.ErrAuthentication
	}
	return "admin", nil
}

// Token returns the UUID v4 key
func (k *APIKey) Token(context.Context, mrfusion.Principal, time.Duration) (string, error) {
	return k.Key, nil
}

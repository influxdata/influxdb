package influxdb

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/influxdata/influxdb/v2/kit/platform"
)

// ErrSecretNotFound is the error msg for a missing secret.
const ErrSecretNotFound = "secret not found"

// SecretService a service for storing and retrieving secrets.
type SecretService interface {
	// LoadSecret retrieves the secret value v found at key k for organization orgID.
	LoadSecret(ctx context.Context, orgID platform.ID, k string) (string, error)

	// GetSecretKeys retrieves all secret keys that are stored for the organization orgID.
	GetSecretKeys(ctx context.Context, orgID platform.ID) ([]string, error)

	// PutSecret stores the secret pair (k,v) for the organization orgID.
	PutSecret(ctx context.Context, orgID platform.ID, k string, v string) error

	// PutSecrets puts all provided secrets and overwrites any previous values.
	PutSecrets(ctx context.Context, orgID platform.ID, m map[string]string) error

	// PatchSecrets patches all provided secrets and updates any previous values.
	PatchSecrets(ctx context.Context, orgID platform.ID, m map[string]string) error

	// DeleteSecret removes a single secret from the secret store.
	DeleteSecret(ctx context.Context, orgID platform.ID, ks ...string) error
}

// SecretField contains a key string, and value pointer.
type SecretField struct {
	Key   string  `json:"key"`
	Value *string `json:"value,omitempty"`
}

// String returns the key of the secret.
func (s SecretField) String() string {
	if s.Key == "" {
		return ""
	}
	return "secret: " + s.Key
}

// MarshalJSON implement the json marshaler interface.
func (s SecretField) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

// UnmarshalJSON implement the json unmarshaler interface.
func (s *SecretField) UnmarshalJSON(b []byte) error {
	var ss string
	if err := json.Unmarshal(b, &ss); err != nil {
		return err
	}
	if ss == "" {
		s.Key = ""
		return nil
	}
	if strings.HasPrefix(ss, "secret: ") {
		s.Key = ss[len("secret: "):]
	} else {
		s.Value = strPtr(ss)
	}
	return nil
}

func strPtr(s string) *string {
	ss := new(string)
	*ss = s
	return ss
}

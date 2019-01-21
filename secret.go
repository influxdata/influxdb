package influxdb

import "context"

// ErrSecretNotFound is the error msg for a missing secret.
const ErrSecretNotFound = "secret not found"

// SecretService a service for storing and retrieving secrets.
type SecretService interface {
	// LoadSecret retrieves the secret value v found at key k for organization orgID.
	LoadSecret(ctx context.Context, orgID ID, k string) (string, error)

	// GetSecretKeys retrieves all secret keys that are stored for the organization orgID.
	GetSecretKeys(ctx context.Context, orgID ID) ([]string, error)

	// PutSecret stores the secret pair (k,v) for the organization orgID.
	PutSecret(ctx context.Context, orgID ID, k string, v string) error

	// PutSecrets puts all provided secrets and overwrites any previous values.
	PutSecrets(ctx context.Context, orgID ID, m map[string]string) error

	// PatchSecrets patches all provided secrets and updates any previous values.
	PatchSecrets(ctx context.Context, orgID ID, m map[string]string) error

	// DeleteSecret removes a single secret from the secret store.
	DeleteSecret(ctx context.Context, orgID ID, ks ...string) error
}

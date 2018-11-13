package platform

import "context"

// SecretService a service for storing and retrieving secrets.
type SecretService interface {
	// LoadSecret retrieves the secret value v found at key k for organization orgID.
	LoadSecret(ctx context.Context, orgID ID, k string) (string, error)

	// GetSecretKeys retrieves all secret keys that are stored for the organization orgID.
	GetSecretKeys(ctx context.Context, orgID ID) ([]string, error)

	// PutSecret stores the secret pair (k,v) for the organization orgID.
	PutSecret(ctx context.Context, orgID ID, k string, v string) error
}

package mock

import (
	"context"
	"fmt"

	platform "github.com/influxdata/influxdb"
)

// SecretService is a mock implementation of a retention.SecretService, which
// also makes it a suitable mock to use wherever an platform.SecretService is required.
type SecretService struct {
	LoadSecretFn    func(ctx context.Context, orgID platform.ID, k string) (string, error)
	GetSecretKeysFn func(ctx context.Context, orgID platform.ID) ([]string, error)
	PutSecretFn     func(ctx context.Context, orgID platform.ID, k string, v string) error
	PutSecretsFn    func(ctx context.Context, orgID platform.ID, m map[string]string) error
	PatchSecretsFn  func(ctx context.Context, orgID platform.ID, m map[string]string) error
	DeleteSecretFn  func(ctx context.Context, orgID platform.ID, ks ...string) error
}

// NewSecretService returns a mock SecretService where its methods will return
// zero values.
func NewSecretService() *SecretService {
	return &SecretService{
		LoadSecretFn: func(ctx context.Context, orgID platform.ID, k string) (string, error) {
			return "", fmt.Errorf("not implmemented")
		},
		GetSecretKeysFn: func(ctx context.Context, orgID platform.ID) ([]string, error) {
			return nil, fmt.Errorf("not implmemented")
		},
		PutSecretFn: func(ctx context.Context, orgID platform.ID, k string, v string) error {
			return fmt.Errorf("not implmemented")
		},
		PutSecretsFn: func(ctx context.Context, orgID platform.ID, m map[string]string) error {
			return fmt.Errorf("not implmemented")
		},
		PatchSecretsFn: func(ctx context.Context, orgID platform.ID, m map[string]string) error {
			return fmt.Errorf("not implmemented")
		},
		DeleteSecretFn: func(ctx context.Context, orgID platform.ID, ks ...string) error {
			return fmt.Errorf("not implmemented")
		},
	}
}

// LoadSecret retrieves the secret value v found at key k for organization orgID.
func (s *SecretService) LoadSecret(ctx context.Context, orgID platform.ID, k string) (string, error) {
	return s.LoadSecretFn(ctx, orgID, k)
}

// GetSecretKeys retrieves all secret keys that are stored for the organization orgID.
func (s *SecretService) GetSecretKeys(ctx context.Context, orgID platform.ID) ([]string, error) {
	return s.GetSecretKeysFn(ctx, orgID)
}

// PutSecret stores the secret pair (k,v) for the organization orgID.
func (s *SecretService) PutSecret(ctx context.Context, orgID platform.ID, k string, v string) error {
	return s.PutSecretFn(ctx, orgID, k, v)
}

// PutSecrets puts all provided secrets and overwrites any previous values.
func (s *SecretService) PutSecrets(ctx context.Context, orgID platform.ID, m map[string]string) error {
	return s.PutSecretsFn(ctx, orgID, m)
}

// PatchSecrets patches all provided secrets and updates any previous values.
func (s *SecretService) PatchSecrets(ctx context.Context, orgID platform.ID, m map[string]string) error {
	return s.PatchSecretsFn(ctx, orgID, m)
}

// DeleteSecret removes a single secret from the secret store.
func (s *SecretService) DeleteSecret(ctx context.Context, orgID platform.ID, ks ...string) error {
	return s.DeleteSecretFn(ctx, orgID, ks...)
}

package authorizer

import (
	"context"

	"github.com/influxdata/influxdb"
)

var _ influxdb.SecretService = (*SecretService)(nil)

// SecretService wraps a influxdb.SecretService and authorizes actions
// against it appropriately.
type SecretService struct {
	s influxdb.SecretService
}

// NewSecretService constructs an instance of an authorizing secret serivce.
func NewSecretService(s influxdb.SecretService) *SecretService {
	return &SecretService{
		s: s,
	}
}

func newSecretPermission(a influxdb.Action, orgID influxdb.ID) (*influxdb.Permission, error) {
	return influxdb.NewPermission(a, influxdb.SecretsResourceType, orgID)
}

func authorizeReadSecret(ctx context.Context, orgID influxdb.ID) error {
	p, err := newSecretPermission(influxdb.ReadAction, orgID)
	if err != nil {
		return err
	}

	if err := IsAllowed(ctx, *p); err != nil {
		return err
	}

	return nil
}

func authorizeWriteSecret(ctx context.Context, orgID influxdb.ID) error {
	p, err := newSecretPermission(influxdb.WriteAction, orgID)
	if err != nil {
		return err
	}

	if err := IsAllowed(ctx, *p); err != nil {
		return err
	}

	return nil
}

// LoadSecret checks to see if the authorizer on context has read access to the secret key provided.
func (s *SecretService) LoadSecret(ctx context.Context, orgID influxdb.ID, key string) (string, error) {
	if err := authorizeReadSecret(ctx, orgID); err != nil {
		return "", err
	}

	secret, err := s.s.LoadSecret(ctx, orgID, key)
	if err != nil {
		return "", err
	}

	return secret, nil
}

// GetSecretKeys checks to see if the authorizer on context has read access to all the secrets belonging to orgID.
func (s *SecretService) GetSecretKeys(ctx context.Context, orgID influxdb.ID) ([]string, error) {
	if err := authorizeReadSecret(ctx, orgID); err != nil {
		return []string{}, err
	}

	secrets, err := s.s.GetSecretKeys(ctx, orgID)
	if err != nil {
		return []string{}, err
	}

	return secrets, nil
}

// PutSecret checks to see if the authorizer on context has write access to the secret key provided.
func (s *SecretService) PutSecret(ctx context.Context, orgID influxdb.ID, key string, val string) error {
	if err := authorizeWriteSecret(ctx, orgID); err != nil {
		return err
	}

	err := s.s.PutSecret(ctx, orgID, key, val)
	if err != nil {
		return err
	}

	return nil
}

// PutSecrets checks to see if the authorizer on context has read and write access to the secret keys provided.
func (s *SecretService) PutSecrets(ctx context.Context, orgID influxdb.ID, m map[string]string) error {
	// PutSecrets operates on intersection betwen m and keys beloging to orgID.
	// We need to have read access to those secrets since it deletes the secrets (within the intersection) that have not be overridden.
	if err := authorizeReadSecret(ctx, orgID); err != nil {
		return err
	}

	if err := authorizeWriteSecret(ctx, orgID); err != nil {
		return err
	}

	err := s.s.PutSecrets(ctx, orgID, m)
	if err != nil {
		return err
	}

	return nil
}

// PatchSecrets checks to see if the authorizer on context has write access to the secret keys provided.
func (s *SecretService) PatchSecrets(ctx context.Context, orgID influxdb.ID, m map[string]string) error {
	if err := authorizeWriteSecret(ctx, orgID); err != nil {
		return err
	}

	err := s.s.PatchSecrets(ctx, orgID, m)
	if err != nil {
		return err
	}

	return nil
}

// DeleteSecret checks to see if the authorizer on context has write access to the secret keys provided.
func (s *SecretService) DeleteSecret(ctx context.Context, orgID influxdb.ID, keys ...string) error {
	if err := authorizeWriteSecret(ctx, orgID); err != nil {
		return err
	}

	err := s.s.DeleteSecret(ctx, orgID, keys...)
	if err != nil {
		return err
	}

	return nil
}

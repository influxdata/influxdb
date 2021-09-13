package secret

import (
	"context"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorizer"
	"github.com/influxdata/influxdb/v2/kit/platform"
)

var _ influxdb.SecretService = (*AuthedSvc)(nil)

// AuthedSvc wraps a influxdb.AuthedSvc and authorizes actions
// against it appropriately.
type AuthedSvc struct {
	s influxdb.SecretService
}

// NewAuthedService constructs an instance of an authorizing secret service.
func NewAuthedService(s influxdb.SecretService) *AuthedSvc {
	return &AuthedSvc{
		s: s,
	}
}

// LoadSecret checks to see if the authorizer on context has read access to the secret key provided.
func (s *AuthedSvc) LoadSecret(ctx context.Context, orgID platform.ID, key string) (string, error) {
	if _, _, err := authorizer.AuthorizeOrgReadResource(ctx, influxdb.SecretsResourceType, orgID); err != nil {
		return "", err
	}
	secret, err := s.s.LoadSecret(ctx, orgID, key)
	if err != nil {
		return "", err
	}
	return secret, nil
}

// GetSecretKeys checks to see if the authorizer on context has read access to all the secrets belonging to orgID.
func (s *AuthedSvc) GetSecretKeys(ctx context.Context, orgID platform.ID) ([]string, error) {
	if _, _, err := authorizer.AuthorizeOrgReadResource(ctx, influxdb.SecretsResourceType, orgID); err != nil {
		return []string{}, err
	}
	secrets, err := s.s.GetSecretKeys(ctx, orgID)
	if err != nil {
		return []string{}, err
	}
	return secrets, nil
}

// PutSecret checks to see if the authorizer on context has write access to the secret key provided.
func (s *AuthedSvc) PutSecret(ctx context.Context, orgID platform.ID, key string, val string) error {
	if _, _, err := authorizer.AuthorizeCreate(ctx, influxdb.SecretsResourceType, orgID); err != nil {
		return err
	}
	err := s.s.PutSecret(ctx, orgID, key, val)
	if err != nil {
		return err
	}
	return nil
}

// PutSecrets checks to see if the authorizer on context has read and write access to the secret keys provided.
func (s *AuthedSvc) PutSecrets(ctx context.Context, orgID platform.ID, m map[string]string) error {
	// PutSecrets operates on intersection between m and keys beloging to orgID.
	// We need to have read access to those secrets since it deletes the secrets (within the intersection) that have not be overridden.
	if _, _, err := authorizer.AuthorizeOrgReadResource(ctx, influxdb.SecretsResourceType, orgID); err != nil {
		return err
	}
	if _, _, err := authorizer.AuthorizeOrgWriteResource(ctx, influxdb.SecretsResourceType, orgID); err != nil {
		return err
	}
	err := s.s.PutSecrets(ctx, orgID, m)
	if err != nil {
		return err
	}
	return nil
}

// PatchSecrets checks to see if the authorizer on context has write access to the secret keys provided.
func (s *AuthedSvc) PatchSecrets(ctx context.Context, orgID platform.ID, m map[string]string) error {
	if _, _, err := authorizer.AuthorizeOrgWriteResource(ctx, influxdb.SecretsResourceType, orgID); err != nil {
		return err
	}
	err := s.s.PatchSecrets(ctx, orgID, m)
	if err != nil {
		return err
	}
	return nil
}

// DeleteSecret checks to see if the authorizer on context has write access to the secret keys provided.
func (s *AuthedSvc) DeleteSecret(ctx context.Context, orgID platform.ID, keys ...string) error {
	if _, _, err := authorizer.AuthorizeOrgWriteResource(ctx, influxdb.SecretsResourceType, orgID); err != nil {
		return err
	}
	err := s.s.DeleteSecret(ctx, orgID, keys...)
	if err != nil {
		return err
	}
	return nil
}

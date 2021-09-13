package secret

import (
	"context"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kv"
)

type Service struct {
	s *Storage
}

// NewService creates a new service implementation for secrets
func NewService(s *Storage) *Service {
	return &Service{s}
}

// LoadSecret retrieves the secret value v found at key k for organization orgID.
func (s *Service) LoadSecret(ctx context.Context, orgID platform.ID, k string) (string, error) {
	var v string
	err := s.s.View(ctx, func(tx kv.Tx) error {
		var err error
		v, err = s.s.GetSecret(ctx, tx, orgID, k)
		return err
	})
	return v, err
}

// GetSecretKeys retrieves all secret keys that are stored for the organization orgID.
func (s *Service) GetSecretKeys(ctx context.Context, orgID platform.ID) ([]string, error) {
	var v []string
	err := s.s.View(ctx, func(tx kv.Tx) error {
		var err error
		v, err = s.s.ListSecret(ctx, tx, orgID)
		return err
	})
	return v, err
}

// PutSecret stores the secret pair (k,v) for the organization orgID.
func (s *Service) PutSecret(ctx context.Context, orgID platform.ID, k, v string) error {
	err := s.s.Update(ctx, func(tx kv.Tx) error {
		return s.s.PutSecret(ctx, tx, orgID, k, v)
	})
	return err
}

// PutSecrets puts all provided secrets and overwrites any previous values.
func (s *Service) PutSecrets(ctx context.Context, orgID platform.ID, m map[string]string) error {
	// put secretes expects to replace all existing secretes
	keys, err := s.GetSecretKeys(ctx, orgID)
	if err != nil {
		return err
	}
	if err := s.DeleteSecret(ctx, orgID, keys...); err != nil {
		return err
	}

	return s.PatchSecrets(ctx, orgID, m)
}

// PatchSecrets patches all provided secrets and updates any previous values.
func (s *Service) PatchSecrets(ctx context.Context, orgID platform.ID, m map[string]string) error {
	err := s.s.Update(ctx, func(tx kv.Tx) error {
		for k, v := range m {
			err := s.s.PutSecret(ctx, tx, orgID, k, v)
			if err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

// DeleteSecret removes a single secret from the secret store.
func (s *Service) DeleteSecret(ctx context.Context, orgID platform.ID, ks ...string) error {
	err := s.s.Update(ctx, func(tx kv.Tx) error {
		for _, k := range ks {
			err := s.s.DeleteSecret(ctx, tx, orgID, k)
			if err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

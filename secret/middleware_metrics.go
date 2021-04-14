package secret

import (
	"context"

	"github.com/influxdata/influxdb/v2/kit/platform"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/metric"
	"github.com/prometheus/client_golang/prometheus"
)

// SecreteService is a metrics middleware system for the secret service
type SecreteService struct {
	// RED metrics
	rec *metric.REDClient

	secretSvc influxdb.SecretService
}

var _ influxdb.SecretService = (*SecreteService)(nil)

// NewMetricService creates a new secret metrics middleware
func NewMetricService(reg prometheus.Registerer, s influxdb.SecretService) *SecreteService {
	return &SecreteService{
		rec:       metric.New(reg, "secret"),
		secretSvc: s,
	}
}

// LoadSecret retrieves the secret value v found at key k for organization orgID.
func (ms *SecreteService) LoadSecret(ctx context.Context, orgID platform.ID, key string) (string, error) {
	rec := ms.rec.Record("load_secret")
	secret, err := ms.secretSvc.LoadSecret(ctx, orgID, key)
	return secret, rec(err)
}

// GetSecretKeys retrieves all secret keys that are stored for the organization orgID.
func (ms *SecreteService) GetSecretKeys(ctx context.Context, orgID platform.ID) ([]string, error) {
	rec := ms.rec.Record("get_secret_keys")
	secrets, err := ms.secretSvc.GetSecretKeys(ctx, orgID)
	return secrets, rec(err)
}

// PutSecret stores the secret pair (k,v) for the organization orgID.
func (ms *SecreteService) PutSecret(ctx context.Context, orgID platform.ID, key string, val string) error {
	rec := ms.rec.Record("put_secret")
	err := ms.secretSvc.PutSecret(ctx, orgID, key, val)
	return rec(err)
}

// PutSecrets puts all provided secrets and overwrites any previous values.
func (ms *SecreteService) PutSecrets(ctx context.Context, orgID platform.ID, m map[string]string) error {
	rec := ms.rec.Record("put_secrets")
	err := ms.secretSvc.PutSecrets(ctx, orgID, m)
	return rec(err)
}

// PatchSecrets patches all provided secrets and updates any previous values.
func (ms *SecreteService) PatchSecrets(ctx context.Context, orgID platform.ID, m map[string]string) error {
	rec := ms.rec.Record("patch_secrets")
	err := ms.secretSvc.PatchSecrets(ctx, orgID, m)
	return rec(err)
}

// DeleteSecret removes a single secret from the secret store.
func (ms *SecreteService) DeleteSecret(ctx context.Context, orgID platform.ID, keys ...string) error {
	rec := ms.rec.Record("delete_secret")
	err := ms.secretSvc.DeleteSecret(ctx, orgID, keys...)
	return rec(err)
}

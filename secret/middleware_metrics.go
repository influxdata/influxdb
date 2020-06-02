package secret

import (
	"context"

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

// FindSession calls the underlying secret service and tracks RED metrics for the call
func (ms *SecreteService) LoadSecret(ctx context.Context, orgID influxdb.ID, key string) (string, error) {
	rec := ms.rec.Record("load_secret")
	secret, err := ms.secretSvc.LoadSecret(ctx, orgID, key)
	return secret, rec(err)
}

// ExpireSession calls the underlying secret service and tracks RED metrics for the call
func (ms *SecreteService) GetSecretKeys(ctx context.Context, orgID influxdb.ID) ([]string, error) {
	rec := ms.rec.Record("get_secret_keys")
	secrets, err := ms.secretSvc.GetSecretKeys(ctx, orgID)
	return secrets, rec(err)
}

// CreateSession calls the underlying secret service and tracks RED metrics for the call
func (ms *SecreteService) PutSecret(ctx context.Context, orgID influxdb.ID, key string, val string) error {
	rec := ms.rec.Record("put_secret")
	err := ms.secretSvc.PutSecret(ctx, orgID, key, val)
	return rec(err)
}

// RenewSession calls the underlying secret service and tracks RED metrics for the call
func (ms *SecreteService) PutSecrets(ctx context.Context, orgID influxdb.ID, m map[string]string) error {
	rec := ms.rec.Record("put_secrets")
	err := ms.secretSvc.PutSecrets(ctx, orgID, m)
	return rec(err)
}

// CreateSession calls the underlying secret service and tracks RED metrics for the call
func (ms *SecreteService) PatchSecrets(ctx context.Context, orgID influxdb.ID, m map[string]string) error {
	rec := ms.rec.Record("patch_secrets")
	err := ms.secretSvc.PatchSecrets(ctx, orgID, m)
	return rec(err)
}

// RenewSession calls the underlying secret service and tracks RED metrics for the call
func (ms *SecreteService) DeleteSecret(ctx context.Context, orgID influxdb.ID, keys ...string) error {
	rec := ms.rec.Record("delete_secret")
	err := ms.secretSvc.DeleteSecret(ctx, orgID, keys...)
	return rec(err)
}

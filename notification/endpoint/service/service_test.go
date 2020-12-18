package service_test

import (
	"context"
	"testing"
	"time"

	influxdb "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/kv/migration/all"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/notification/endpoint"
	"github.com/influxdata/influxdb/v2/notification/endpoint/service"
	"github.com/influxdata/influxdb/v2/pkg/pointer"
	"github.com/influxdata/influxdb/v2/secret"
	"github.com/influxdata/influxdb/v2/tenant"
	influxTesting "github.com/influxdata/influxdb/v2/testing"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

var (
	id1    = influxTesting.MustIDBase16Ptr("020f755c3c082000")
	id2    = influxTesting.MustIDBase16Ptr("020f755c3c082001")
	orgID  = influxTesting.MustIDBase16Ptr("a10f755c3c082001")
	userID = influxTesting.MustIDBase16Ptr("b10f755c3c082001")

	timeGen1 = mock.TimeGenerator{FakeValue: time.Date(2006, time.July, 13, 4, 19, 10, 0, time.UTC)}
	timeGen2 = mock.TimeGenerator{FakeValue: time.Date(2006, time.July, 14, 5, 23, 53, 10, time.UTC)}

	testCrudLog = influxdb.CRUDLog{
		CreatedAt: timeGen1.Now(),
		UpdatedAt: timeGen2.Now(),
	}
)

func newSecretService(t *testing.T, ctx context.Context, logger *zap.Logger, s kv.Store) influxdb.SecretService {
	t.Helper()

	tenantSvc := tenant.NewService(tenant.NewStore(s))

	// initialize organization
	org := influxdb.Organization{
		ID:      *orgID,
		Name:    "Test Organization",
		CRUDLog: testCrudLog,
	}

	if err := tenantSvc.CreateOrganization(ctx, &org); err != nil {
		t.Fatal(err)
	}
	orgID = &org.ID // orgID is generated

	secretStore, err := secret.NewStore(s)
	require.NoError(t, err)
	return secret.NewService(secretStore)
}

// TestEndpointService_cumulativeSecrets tests that secrets are cumulatively added/updated and removed upon delete
// see https://github.com/influxdata/influxdb/pull/19082 for details
func TestEndpointService_cumulativeSecrets(t *testing.T) {
	ctx := context.Background()
	store := inmem.NewKVStore()
	logger := zaptest.NewLogger(t)
	if err := all.Up(ctx, logger, store); err != nil {
		t.Fatal(err)
	}

	secretService := newSecretService(t, ctx, logger, store)
	endpointService := service.New(service.NewStore(store), secretService)

	var endpoint1 = endpoint.HTTP{
		Base: endpoint.Base{
			ID:     id1,
			Name:   "name1",
			OrgID:  orgID,
			Status: influxdb.Active,
			CRUDLog: influxdb.CRUDLog{
				CreatedAt: timeGen1.Now(),
				UpdatedAt: timeGen2.Now(),
			},
		},
		Headers:    map[string]string{},
		AuthMethod: "basic",
		Method:     "POST",
		URL:        "http://example.com",
		Username:   influxdb.SecretField{Key: id1.String() + "username-key", Value: pointer.String("val1")},
		Password:   influxdb.SecretField{Key: id1.String() + "password-key", Value: pointer.String("val2")},
	}
	var endpoint2 = endpoint.HTTP{
		Base: endpoint.Base{
			ID:     id2,
			Name:   "name2",
			OrgID:  orgID,
			Status: influxdb.Active,
			CRUDLog: influxdb.CRUDLog{
				CreatedAt: timeGen1.Now(),
				UpdatedAt: timeGen2.Now(),
			},
		},
		Headers:    map[string]string{},
		AuthMethod: "basic",
		Method:     "POST",
		URL:        "http://example2.com",
		Username:   influxdb.SecretField{Key: id2.String() + "username-key", Value: pointer.String("val3")},
		Password:   influxdb.SecretField{Key: id2.String() + "password-key", Value: pointer.String("val4")},
	}
	var err error
	var secretKeys []string

	// create 1st endpoint and validate secrets
	if err = endpointService.CreateNotificationEndpoint(ctx, &endpoint1, *userID); err != nil {
		t.Fatal(err)
	}
	if secretKeys, err = secretService.GetSecretKeys(ctx, *orgID); err != nil {
		t.Fatal(err)
	}
	if len(secretKeys) != 2 {
		t.Errorf("secrets after creating 1st endpoint = %v, want %v", len(secretKeys), 2)
	}

	// create 2nd endpoint and validate secrets
	if err = endpointService.CreateNotificationEndpoint(ctx, &endpoint2, *userID); err != nil {
		t.Fatal(err)
	}
	if secretKeys, err = secretService.GetSecretKeys(ctx, *orgID); err != nil {
		t.Fatal(err)
	}
	if len(secretKeys) != 4 {
		t.Errorf("secrets after creating 2nd endpoint = %v, want %v", len(secretKeys), 4)
	}

	// update 1st endpoint and validate secrets
	const updatedSecretValue = "updatedSecVal"
	endpoint1.Username.Value = pointer.String(updatedSecretValue)
	if _, err = endpointService.UpdateNotificationEndpoint(ctx, *endpoint1.ID, &endpoint1, *userID); err != nil {
		t.Fatal(err)
	}
	if secretKeys, err = secretService.GetSecretKeys(ctx, *orgID); err != nil {
		t.Fatal(err)
	}
	if len(secretKeys) != 4 {
		t.Errorf("secrets after updating 1st endpoint = %v, want %v", len(secretKeys), 4)
	}
	var secretValue string
	if secretValue, err = secretService.LoadSecret(ctx, *orgID, endpoint1.Username.Key); err != nil {
		t.Fatal(err)
	}
	if secretValue != updatedSecretValue {
		t.Errorf("secret after updating 1st endpoint is not updated = %v, want %v", secretValue, updatedSecretValue)
	}

	// delete 1st endpoints and secrets, validate secrets
	var secretsToDelete []influxdb.SecretField
	if secretsToDelete, _, err = endpointService.DeleteNotificationEndpoint(ctx, *endpoint1.ID); err != nil {
		t.Fatal(err)
	}
	if len(secretsToDelete) != 2 {
		t.Errorf("2 secrets expected as a result of deleting the 1st endpoint")
	}
	secretService.DeleteSecret(ctx, *orgID, secretsToDelete[0].Key, secretsToDelete[1].Key)
	if secretKeys, err = secretService.GetSecretKeys(ctx, *orgID); err != nil {
		t.Fatal(err)
	}
	if len(secretKeys) != 2 {
		t.Errorf("secrets after deleting 1st endpoint = %v, want %v", len(secretKeys), 2)
	}

	if secretsToDelete, _, err = endpointService.DeleteNotificationEndpoint(ctx, *endpoint2.ID); err != nil {
		t.Fatal(err)
	}
	if len(secretsToDelete) != 2 {
		t.Errorf("2 secrets expected as a result of deleting the 2nd endpoint")
	}
	secretService.DeleteSecret(ctx, *orgID, secretsToDelete[0].Key, secretsToDelete[1].Key)
	if secretKeys, err = secretService.GetSecretKeys(ctx, *orgID); err != nil {
		t.Fatal(err)
	}
	if len(secretKeys) != 0 {
		t.Errorf("secrets after deleting the 2nd endpoint = %v, want %v", len(secretKeys), 2)
	}
}

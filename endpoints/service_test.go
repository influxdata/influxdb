package endpoints_test

import (
	"context"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/endpoints"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/kv/migration/all"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/notification/endpoint"
	influxTesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

var id1 = influxTesting.MustIDBase16Ptr("020f755c3c082000")
var id2 = influxTesting.MustIDBase16Ptr("020f755c3c082001")
var orgID = influxTesting.MustIDBase16Ptr("a10f755c3c082001")
var userID = influxTesting.MustIDBase16Ptr("b10f755c3c082001")

var timeGen1 = mock.TimeGenerator{FakeValue: time.Date(2006, time.July, 13, 4, 19, 10, 0, time.UTC)}
var timeGen2 = mock.TimeGenerator{FakeValue: time.Date(2006, time.July, 14, 5, 23, 53, 10, time.UTC)}
var testCrudLog = influxdb.CRUDLog{
	CreatedAt: timeGen1.Now(),
	UpdatedAt: timeGen2.Now(),
}

// newInmemService creates a new in-memory secret service
func newInmemService(t *testing.T) *kv.Service {
	t.Helper()

	store := inmem.NewKVStore()
	logger := zaptest.NewLogger(t)
	ctx := context.Background()
	// initialize the store
	if err := all.Up(ctx, logger, store); err != nil {
		t.Fatal(err)
	}
	svc := kv.NewService(logger, store)

	// initialize organization
	org := influxdb.Organization{
		ID:      *orgID,
		Name:    "Test Organization",
		CRUDLog: testCrudLog,
	}

	if err := svc.CreateOrganization(ctx, &org); err != nil {
		t.Fatal(err)
	}
	orgID = &org.ID // orgID is generated

	return svc
}

// TestEndpointService_cummulativeSecrets tests that secrets are cummulatively added/updated and removed upon delete
// see https://github.com/influxdata/influxdb/pull/19082 for details
func TestEndpointService_cummulativeSecrets(t *testing.T) {
	inMemService := newInmemService(t)
	endpointService := endpoints.NewService(inMemService, inMemService, inMemService, inMemService)
	secretService := inMemService
	ctx := context.Background()

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
		Username:   influxdb.SecretField{Key: id1.String() + "username-key", Value: strPtr("val1")},
		Password:   influxdb.SecretField{Key: id1.String() + "password-key", Value: strPtr("val2")},
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
		Username:   influxdb.SecretField{Key: id2.String() + "username-key", Value: strPtr("val3")},
		Password:   influxdb.SecretField{Key: id2.String() + "password-key", Value: strPtr("val4")},
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
	endpoint1.Username.Value = strPtr(updatedSecretValue)
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

// strPtr returns string pointer
func strPtr(s string) *string {
	return &s
}

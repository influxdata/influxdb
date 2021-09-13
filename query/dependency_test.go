package query_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/query"
)

func TestSecretLookup(t *testing.T) {
	req := &query.Request{OrganizationID: orgID}
	ctx := query.ContextWithRequest(context.Background(), req)
	svc := &mock.SecretService{
		LoadSecretFn: func(ctx context.Context, orgID platform.ID, k string) (string, error) {
			if want, got := req.OrganizationID, orgID; want != got {
				t.Errorf("unexpected organization id -want/+got:\n\t- %v\n\t+ %v", want, got)
			}
			if want, got := "mysecret", k; want != got {
				t.Errorf("unexpected secret key -want/+got:\n\t- %v\n\t+ %v", want, got)
			}
			return "mypassword", nil
		},
	}

	dep := query.FromSecretService(svc)
	if val, err := dep.LoadSecret(ctx, "mysecret"); err != nil {
		t.Errorf("unexpected error: %s", err)
	} else if want, got := "mypassword", val; want != got {
		t.Errorf("unexpected secret value -want/+got:\n\t- %v\n\t+ %v", want, got)
	}
}

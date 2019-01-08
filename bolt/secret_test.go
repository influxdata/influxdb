package bolt_test

import (
	"context"
	"testing"

	platform "github.com/influxdata/influxdb"
	platformtesting "github.com/influxdata/influxdb/testing"
)

func initSecretService(f platformtesting.SecretServiceFields, t *testing.T) (platform.SecretService, func()) {
	c, closeFn, err := NewTestClient()
	if err != nil {
		t.Fatalf("failed to create new bolt client: %v", err)
	}
	ctx := context.TODO()
	for _, s := range f.Secrets {
		for k, v := range s.Env {
			if err := c.PutSecret(ctx, s.OrganizationID, k, v); err != nil {
				t.Fatalf("failed to populate secrets")
			}
		}
	}
	return c, func() {
		defer closeFn()
	}
}

func TestSecretService(t *testing.T) {
	platformtesting.SecretService(initSecretService, t)
}

package kv_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

func TestBoltSecretService(t *testing.T) {
	influxdbtesting.SecretService(initBoltSecretService, t)
}

func initBoltSecretService(f influxdbtesting.SecretServiceFields, t *testing.T) (influxdb.SecretService, func()) {
	s, closeBolt, err := NewTestBoltStore(t)
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, closeSvc := initSecretService(s, f, t)
	return svc, func() {
		closeSvc()
		closeBolt()
	}
}

func initSecretService(s kv.Store, f influxdbtesting.SecretServiceFields, t *testing.T) (influxdb.SecretService, func()) {
	svc := kv.NewService(zaptest.NewLogger(t), s)
	ctx := context.Background()
	if err := svc.Initialize(ctx); err != nil {
		t.Fatalf("error initializing secret service: %v", err)
	}

	for _, s := range f.Secrets {
		for k, v := range s.Env {
			if err := svc.PutSecret(ctx, s.OrganizationID, k, v); err != nil {
				t.Fatalf("failed to populate secrets")
			}
		}
	}

	return svc, func() {}
}

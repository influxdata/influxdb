package kv_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kv"
	influxdbtesting "github.com/influxdata/influxdb/testing"
)

func TestBoltSecretService(t *testing.T) {
	influxdbtesting.SecretService(initBoltSecretService, t)
}

func TestInmemSecretService(t *testing.T) {
	influxdbtesting.SecretService(initInmemSecretService, t)
}

func initBoltSecretService(f influxdbtesting.SecretServiceFields, t *testing.T) (influxdb.SecretService, func()) {
	s, closeBolt, err := NewTestBoltStore()
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, closeSvc := initSecretService(s, f, t)
	return svc, func() {
		closeSvc()
		closeBolt()
	}
}

func initInmemSecretService(f influxdbtesting.SecretServiceFields, t *testing.T) (influxdb.SecretService, func()) {
	s, closeBolt, err := NewTestInmemStore()
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
	svc := kv.NewService(s)
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

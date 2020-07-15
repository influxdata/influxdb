package secret_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/kv/migration/all"
	"github.com/influxdata/influxdb/v2/secret"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

func TestBoltSecretService(t *testing.T) {
	influxdbtesting.SecretService(initSvc, t)
}

func initSvc(f influxdbtesting.SecretServiceFields, t *testing.T) (influxdb.SecretService, func()) {
	t.Helper()

	s := inmem.NewKVStore()

	ctx := context.Background()
	if err := all.Up(ctx, zaptest.NewLogger(t), s); err != nil {
		t.Fatal(err)
	}

	storage, err := secret.NewStore(s)
	if err != nil {
		t.Fatal(err)
	}

	svc := secret.NewService(storage)

	for _, s := range f.Secrets {
		if err := svc.PutSecrets(ctx, s.OrganizationID, s.Env); err != nil {
			t.Fatalf("failed to populate users: %q", err)
		}
	}

	return svc, func() {}
}

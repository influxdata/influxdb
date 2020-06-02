package secret_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/secret"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
)

func TestBoltSecretService(t *testing.T) {
	influxdbtesting.SecretService(initSvc, t)
}

func initSvc(f influxdbtesting.SecretServiceFields, t *testing.T) (influxdb.SecretService, func()) {
	s := inmem.NewKVStore()

	storage, err := secret.NewStore(s)
	if err != nil {
		t.Fatal(err)
	}
	svc := secret.NewService(storage)

	for _, s := range f.Secrets {
		if err := svc.PutSecrets(context.Background(), s.OrganizationID, s.Env); err != nil {
			t.Fatalf("failed to populate users")
		}
	}

	return svc, func() {}
}

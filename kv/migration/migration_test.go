package migration_test

import (
	"context"
	"errors"
	"io/ioutil"
	"os"
	"testing"

	"github.com/influxdata/influxdb/v2/bolt"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/kv/migration"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

func newMigrator(t *testing.T, logger *zap.Logger, store kv.SchemaStore, now influxdbtesting.NowFunc) *migration.Migrator {
	migrator, err := migration.NewMigrator(logger, store)
	if err != nil {
		t.Fatal(err)
	}

	migration.MigratorSetNow(migrator, now)
	return migrator
}

func Test_Inmem_Migrator(t *testing.T) {
	influxdbtesting.Migrator(t, inmem.NewKVStore(), newMigrator)
}

func Test_Bolt_Migrator(t *testing.T) {
	store, closeBolt, err := newTestBoltStoreWithoutMigrations(t)
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}
	defer closeBolt()

	influxdbtesting.Migrator(t, store, newMigrator)
}

func newTestBoltStoreWithoutMigrations(t *testing.T) (kv.SchemaStore, func(), error) {
	f, err := ioutil.TempFile("", "influxdata-bolt-")
	if err != nil {
		return nil, nil, errors.New("unable to open temporary boltdb file")
	}
	f.Close()

	path := f.Name()
	s := bolt.NewKVStore(zaptest.NewLogger(t), path, bolt.WithNoSync)
	if err := s.Open(context.Background()); err != nil {
		return nil, nil, err
	}

	close := func() {
		s.Close()
		os.Remove(path)
	}

	return s, close, nil
}

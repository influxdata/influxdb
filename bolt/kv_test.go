package bolt_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/influxdata/influxdb/v2/bolt"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/kv/migration"
	platformtesting "github.com/influxdata/influxdb/v2/testing"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func initKVStore(f platformtesting.KVStoreFields, t *testing.T) (kv.Store, func()) {
	s, closeFn, err := NewTestKVStore(t)
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	mustCreateBucket(t, s, f.Bucket)

	err = s.Update(context.Background(), func(tx kv.Tx) error {
		b, err := tx.Bucket(f.Bucket)
		if err != nil {
			return err
		}

		for _, p := range f.Pairs {
			if err := b.Put(p.Key, p.Value); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		t.Fatalf("failed to put keys: %v", err)
	}
	return s, func() {
		closeFn()
	}
}

func TestKVStore(t *testing.T) {
	platformtesting.KVStore(initKVStore, t)
}

func mustCreateBucket(t testing.TB, store kv.SchemaStore, bucket []byte) {
	t.Helper()

	migrationName := fmt.Sprintf("create bucket %q", string(bucket))

	if err := migration.CreateBuckets(migrationName, bucket).Up(context.Background(), store); err != nil {
		t.Fatal(err)
	}
}

func TestCreateBucketManifests(t *testing.T) {
	ctx := context.Background()

	kvStore := bolt.NewKVStore(zap.NewNop(), "/Users/wbaker/.influxdbv2/influxd.bolt")
	err := kvStore.Open(ctx)
	require.NoError(t, err)
	defer kvStore.Close()

	w := bytes.NewBuffer(nil)
	err = kvStore.CreateBucketManifests(ctx, w)
	require.NoError(t, err)

	t.Fatal()
}

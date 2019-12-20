package inmem_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/snowflake"
	"go.uber.org/zap/zaptest"
)

var (
	taskBucket                              = []byte("tasksv1")
	organizationBucket                      = []byte("organizationsv1")
	authBucket                              = []byte("authorizationsv1")
	idgen              influxdb.IDGenerator = snowflake.NewIDGenerator()
)

func BenchmarkFindTaskByID_CursorHints(b *testing.B) {
	kvs := inmem.NewKVStore()
	ctx := context.Background()
	_ = kvs.Update(ctx, func(tx kv.Tx) error {
		createData(b, tx)
		createTasks(b, tx)

		return nil
	})

	s := kv.NewService(zaptest.NewLogger(b), kvs)
	_ = s.Initialize(ctx)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = s.FindTaskByID(ctx, 1)
	}
}

func createData(tb testing.TB, tx kv.Tx) {
	tb.Helper()

	authB, err := tx.Bucket(authBucket)
	if err != nil {
		tb.Fatal("authBucket:", err)
	}
	orgB, err := tx.Bucket(organizationBucket)
	if err != nil {
		tb.Fatal("organizationBucket:", err)
	}

	a := influxdb.Authorization{
		Permissions: influxdb.OperPermissions(),
	}
	o := influxdb.Organization{}

	var orgID = influxdb.ID(1e4)
	var userID = influxdb.ID(1e7)
	for i := 1; i <= 1000; i++ {
		o.ID = orgID
		val := mustMarshal(tb, &o)
		key, _ := a.OrgID.Encode()
		_ = orgB.Put(key, val)

		a.OrgID = o.ID
		orgID++

		for j := 1; j <= 5; j++ {
			a.ID = idgen.ID()
			a.UserID = userID
			userID++

			val = mustMarshal(tb, &a)
			key, _ = a.ID.Encode()
			_ = authB.Put(key, val)
		}
	}
}

func createTasks(tb testing.TB, tx kv.Tx) {
	tb.Helper()

	taskB, err := tx.Bucket(taskBucket)
	if err != nil {
		tb.Fatal("taskBucket:", err)
	}

	t := influxdb.Task{
		ID:             1,
		OrganizationID: 1e4,
		OwnerID:        1e7,
	}

	val := mustMarshal(tb, &t)
	key, _ := t.ID.Encode()
	_ = taskB.Put(key, val)
}

func mustMarshal(t testing.TB, v interface{}) []byte {
	t.Helper()
	d, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

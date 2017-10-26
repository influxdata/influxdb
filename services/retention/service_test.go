package retention_test

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/influxdata/influxdb/internal"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/services/retention"
	"github.com/influxdata/influxdb/toml"
	"github.com/uber-go/zap"
)

func TestService_OpenDisabled(t *testing.T) {
	// Opening a disabled service should be a no-op.
	c := retention.NewConfig()
	c.Enabled = false
	s := NewService(c)

	if err := s.Open(); err != nil {
		t.Fatal(err)
	}

	if s.LogBuf.String() != "" {
		t.Fatalf("service logged %q, didn't expect any logging", s.LogBuf.String())
	}
}

func TestService_OpenClose(t *testing.T) {
	// Opening a disabled service should be a no-op.
	s := NewService(retention.NewConfig())

	if err := s.Open(); err != nil {
		t.Fatal(err)
	}

	if s.LogBuf.String() == "" {
		t.Fatal("service didn't log anything on open")
	}

	// Reopening is a no-op
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Re-closing is a no-op
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

// This reproduces https://github.com/influxdata/influxdb/issues/8819
func TestService_8819_repro(t *testing.T) {
	for i := 0; i < 1000; i++ {
		s, errC := testService_8819_repro(t)

		if err := s.Open(); err != nil {
			t.Fatal(err)
		}

		// Wait for service to run.
		if err := <-errC; err != nil {
			t.Fatalf("%dth iteration: %v", i, err)
		}
	}
}

func testService_8819_repro(t *testing.T) (*Service, chan error) {
	c := retention.NewConfig()
	c.CheckInterval = toml.Duration(time.Millisecond)
	s := NewService(c)
	errC := make(chan error)

	// A database and a bunch of shards
	var mu sync.Mutex
	shards := []uint64{3, 5, 8, 9, 11}
	localShards := []uint64{3, 5, 8, 9, 11}
	databases := []meta.DatabaseInfo{
		{
			Name: "db0",
			RetentionPolicies: []meta.RetentionPolicyInfo{
				{
					Name:               "autogen",
					Duration:           time.Millisecond,
					ShardGroupDuration: time.Millisecond,
					ShardGroups: []meta.ShardGroupInfo{
						{
							ID:        1,
							StartTime: time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC),
							EndTime:   time.Date(1981, 1, 1, 0, 0, 0, 0, time.UTC),
							Shards: []meta.ShardInfo{
								{ID: 3}, {ID: 9},
							},
						},
					},
				},
			},
			// TODO - add expired stuff
		},
	}

	s.MetaClient.DatabasesFn = func() []meta.DatabaseInfo {
		mu.Lock()
		defer mu.Unlock()
		return databases
	}

	s.MetaClient.DeleteShardGroupFn = func(database string, policy string, id uint64) error {
		if database != "db0" {
			errC <- fmt.Errorf("wrong db name: %s", database)
		} else if policy != "autogen" {
			errC <- fmt.Errorf("wrong rp name: %s", policy)
		} else if id != 1 {
			errC <- fmt.Errorf("wrong shard group id: %d", id)
		}

		// remove the associated shards (3 and 9) from the shards slice...
		mu.Lock()
		newShards := make([]uint64, 0, len(shards))
		for _, sid := range shards {
			if sid != 3 && sid != 9 {
				newShards = append(newShards, sid)
			}
		}
		shards = newShards
		databases[0].RetentionPolicies[0].ShardGroups[0].DeletedAt = time.Now().UTC()
		mu.Unlock()
		return nil
	}

	s.MetaClient.PruneShardGroupsFn = func() error {
		// When this is called all shards that have been deleted from the meta
		// store (expired) should also have been deleted from disk.
		// If they haven't then that indicates that shards can be removed from
		// the meta store and there can be a race where they haven't yet been
		// removed from the local disk and indexes. This has an impact on, for
		// example, the max series per database limit.

		mu.Lock()
		defer mu.Unlock()
		for _, lid := range localShards {
			var found bool
			for _, mid := range shards {
				if lid == mid {
					found = true
					break
				}
			}

			if !found {
				errC <- fmt.Errorf("local shard %d present, yet it's missing from meta store. %v -- %v ", lid, shards, localShards)
			}
		}
		errC <- nil
		return nil
	}

	s.TSDBStore.ShardIDsFn = func() []uint64 {
		mu.Lock()
		defer mu.Unlock()
		return localShards
	}

	s.TSDBStore.DeleteShardFn = func(id uint64) error {
		var found bool
		mu.Lock()
		newShards := make([]uint64, 0, len(localShards))
		for _, sid := range localShards {
			if sid != id {
				newShards = append(newShards, sid)
			} else {
				found = true
			}
		}
		localShards = newShards
		mu.Unlock()

		if !found {
			return fmt.Errorf("shard %d not found locally", id)
		}
		return nil
	}

	return s, errC
}

type Service struct {
	MetaClient *internal.MetaClientMock
	TSDBStore  *internal.TSDBStoreMock

	LogBuf bytes.Buffer
	*retention.Service
}

func NewService(c retention.Config) *Service {
	s := &Service{
		MetaClient: &internal.MetaClientMock{},
		TSDBStore:  &internal.TSDBStoreMock{},
		Service:    retention.NewService(c),
	}

	mls := zap.MultiWriteSyncer(zap.AddSync(&s.LogBuf))

	l := zap.New(
		zap.NewTextEncoder(),
		zap.Output(mls),
	)
	s.WithLogger(l)

	s.Service.MetaClient = s.MetaClient
	s.Service.TSDBStore = s.TSDBStore
	return s
}

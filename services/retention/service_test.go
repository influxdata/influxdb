package retention_test

import (
	"bytes"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/influxdata/influxdb/internal"
	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/services/retention"
	"github.com/influxdata/influxdb/toml"
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
		s, errC, done := testService_8819_repro(t)

		if err := s.Open(); err != nil {
			t.Fatal(err)
		}

		// Wait for service to run one sweep of all dbs/rps/shards.
		if err := <-errC; err != nil {
			t.Fatalf("%dth iteration: %v", i, err)
		}
		// Mark that we do not expect more errors in case it runs one more time.
		close(done)

		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func testService_8819_repro(t *testing.T) (*Service, chan error, chan struct{}) {
	c := retention.NewConfig()
	c.CheckInterval = toml.Duration(time.Millisecond)
	s := NewService(c)
	errC := make(chan error, 1) // Buffer Important to prevent deadlock.
	done := make(chan struct{})

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
		},
	}

	sendError := func(err error) {
		select {
		case errC <- err:
		case <-done:
		}
	}

	s.MetaClient.DatabasesFn = func() []meta.DatabaseInfo {
		mu.Lock()
		defer mu.Unlock()
		return databases
	}

	s.MetaClient.DeleteShardGroupFn = func(database string, policy string, id uint64) error {
		if database != "db0" {
			sendError(fmt.Errorf("wrong db name: %s", database))
			return nil
		} else if policy != "autogen" {
			sendError(fmt.Errorf("wrong rp name: %s", policy))
			return nil
		} else if id != 1 {
			sendError(fmt.Errorf("wrong shard group id: %d", id))
			return nil
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
				sendError(fmt.Errorf("local shard %d present, yet it's missing from meta store. %v -- %v ", lid, shards, localShards))
				return nil
			}
		}

		// We should have removed shards 3 and 9
		if !reflect.DeepEqual(localShards, []uint64{5, 8, 11}) {
			sendError(fmt.Errorf("removed shards still present locally: %v", localShards))
			return nil
		}
		sendError(nil)
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

	return s, errC, done
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

	l := logger.New(&s.LogBuf)
	s.WithLogger(l)

	s.Service.MetaClient = s.MetaClient
	s.Service.TSDBStore = s.TSDBStore
	return s
}

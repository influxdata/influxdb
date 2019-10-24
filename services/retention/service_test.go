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

func TestService_CheckShards(t *testing.T) {
	now := time.Now()
	// Account for any time difference that could cause some of the logic in
	// this test to fail due to a race condition. If we are at the very end of
	// the hour, we can choose a time interval based on one "now" time and then
	// run the retention service in the next hour. If we're in one of those
	// situations, wait 100 milliseconds until we're in the next hour.
	if got, want := now.Add(100*time.Millisecond).Truncate(time.Hour), now.Truncate(time.Hour); !got.Equal(want) {
		time.Sleep(100 * time.Millisecond)
	}

	data := []meta.DatabaseInfo{
		{
			Name: "db0",

			DefaultRetentionPolicy: "rp0",
			RetentionPolicies: []meta.RetentionPolicyInfo{
				{
					Name:               "rp0",
					ReplicaN:           1,
					Duration:           time.Hour,
					ShardGroupDuration: time.Hour,
					ShardGroups: []meta.ShardGroupInfo{
						{
							ID:        1,
							StartTime: now.Truncate(time.Hour).Add(-2 * time.Hour),
							EndTime:   now.Truncate(time.Hour).Add(-1 * time.Hour),
							Shards: []meta.ShardInfo{
								{ID: 2},
								{ID: 3},
							},
						},
						{
							ID:        4,
							StartTime: now.Truncate(time.Hour).Add(-1 * time.Hour),
							EndTime:   now.Truncate(time.Hour),
							Shards: []meta.ShardInfo{
								{ID: 5},
								{ID: 6},
							},
						},
						{
							ID:        7,
							StartTime: now.Truncate(time.Hour),
							EndTime:   now.Truncate(time.Hour).Add(time.Hour),
							Shards: []meta.ShardInfo{
								{ID: 8},
								{ID: 9},
							},
						},
					},
				},
			},
		},
	}

	config := retention.NewConfig()
	config.CheckInterval = toml.Duration(10 * time.Millisecond)
	s := NewService(config)
	s.MetaClient.DatabasesFn = func() []meta.DatabaseInfo {
		return data
	}

	done := make(chan struct{})
	deletedShardGroups := make(map[string]struct{})
	s.MetaClient.DeleteShardGroupFn = func(database, policy string, id uint64) error {
		for _, dbi := range data {
			if dbi.Name == database {
				for _, rpi := range dbi.RetentionPolicies {
					if rpi.Name == policy {
						for i, sg := range rpi.ShardGroups {
							if sg.ID == id {
								rpi.ShardGroups[i].DeletedAt = time.Now().UTC()
							}
						}
					}
				}
			}
		}

		deletedShardGroups[fmt.Sprintf("%s.%s.%d", database, policy, id)] = struct{}{}
		if got, want := deletedShardGroups, map[string]struct{}{
			"db0.rp0.1": struct{}{},
		}; reflect.DeepEqual(got, want) {
			close(done)
		} else if len(got) > 1 {
			t.Errorf("deleted too many shard groups")
		}
		return nil
	}

	pruned := false
	closing := make(chan struct{})
	s.MetaClient.PruneShardGroupsFn = func() error {
		select {
		case <-done:
			if !pruned {
				close(closing)
				pruned = true
			}
		default:
		}
		return nil
	}

	deletedShards := make(map[uint64]struct{})
	s.TSDBStore.ShardIDsFn = func() []uint64 {
		return []uint64{2, 3, 5, 6}
	}
	s.TSDBStore.DeleteShardFn = func(shardID uint64) error {
		deletedShards[shardID] = struct{}{}
		return nil
	}

	if err := s.Open(); err != nil {
		t.Fatalf("unexpected open error: %s", err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Fatalf("unexpected close error: %s", err)
		}
	}()

	timer := time.NewTimer(100 * time.Millisecond)
	select {
	case <-done:
		timer.Stop()
	case <-timer.C:
		t.Errorf("timeout waiting for shard groups to be deleted")
		return
	}

	timer = time.NewTimer(100 * time.Millisecond)
	select {
	case <-closing:
		timer.Stop()
	case <-timer.C:
		t.Errorf("timeout waiting for shards to be deleted")
		return
	}

	if got, want := deletedShards, map[uint64]struct{}{
		2: struct{}{},
		3: struct{}{},
	}; !reflect.DeepEqual(got, want) {
		t.Errorf("unexpected deleted shards: got=%#v want=%#v", got, want)
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
	shards := []uint64{3, 5, 8, 9, 11, 12}
	localShards := []uint64{3, 5, 8, 9, 11, 12}
	databases := []meta.DatabaseInfo{
		{
			Name: "db0",
			RetentionPolicies: []meta.RetentionPolicyInfo{
				{
					Name:               "autogen",
					Duration:           24 * time.Hour,
					ShardGroupDuration: 24 * time.Hour,
					ShardGroups: []meta.ShardGroupInfo{
						{
							ID:        1,
							StartTime: time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC),
							EndTime:   time.Date(1981, 1, 1, 0, 0, 0, 0, time.UTC),
							Shards: []meta.ShardInfo{
								{ID: 3}, {ID: 9},
							},
						},
						{
							ID:        2,
							StartTime: time.Now().Add(-1 * time.Hour),
							EndTime:   time.Now(),
							DeletedAt: time.Now(),
							Shards: []meta.ShardInfo{
								{ID: 11}, {ID: 12},
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
		if !reflect.DeepEqual(localShards, []uint64{5, 8}) {
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

package retention_test

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"sync"
	"testing"
	"time"

	"golang.org/x/exp/maps"

	"github.com/influxdata/influxdb/internal"
	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/services/retention"
	"github.com/influxdata/influxdb/services/retention/helpers"
	"github.com/influxdata/influxdb/toml"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/stretchr/testify/require"
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

func TestRetention_DeletionCheck(t *testing.T) {
	cfg := retention.Config{
		Enabled: true,

		// This test runs DeletionCheck manually for the test cases. It is about checking
		// the results of DeletionCheck, not if it is run properly on the timer.
		// Set a long check interval so the deletion check won't run on its own during the test.
		CheckInterval: toml.Duration(time.Hour * 24),
	}

	now := time.Now().UTC()
	shardDuration := time.Hour * 24 * 14
	shardGroupDuration := time.Hour * 24
	foreverShard := uint64(1003) // a shard that can't be deleted
	phantomShard := uint64(1006) // a shard that exists in meta data but not TSDB store
	activeShard := uint64(1007)  // a shard that is active
	dataUT := &meta.Data{
		Users: []meta.UserInfo{},
		Databases: []meta.DatabaseInfo{
			{
				Name:                   "servers",
				DefaultRetentionPolicy: "autogen",
				RetentionPolicies: []meta.RetentionPolicyInfo{
					{
						Name:               "autogen",
						ReplicaN:           2,
						Duration:           shardDuration,
						ShardGroupDuration: shardGroupDuration,
						ShardGroups: []meta.ShardGroupInfo{
							// Shard group 1 is deleted and expired group with a single shard.
							{
								ID:        1,
								StartTime: now.Truncate(time.Hour * 24).Add(-2*shardDuration + 0*shardGroupDuration).Add(meta.ShardGroupDeletedExpiration),
								EndTime:   now.Truncate(time.Hour * 24).Add(-2*shardDuration + 1*shardGroupDuration).Add(meta.ShardGroupDeletedExpiration),
								DeletedAt: now.Truncate(time.Hour * 24).Add(-1 * shardDuration).Add(meta.ShardGroupDeletedExpiration),
								Shards: []meta.ShardInfo{
									{
										ID: 101,
									},
								},
							},
							// Shard group 2 is deleted and expired with no shards.
							// Note a shard group with no shards should not exist anyway.
							{
								ID:        2,
								StartTime: now.Truncate(time.Hour * 24).Add(-2*shardDuration + 2*shardGroupDuration).Add(meta.ShardGroupDeletedExpiration),
								EndTime:   now.Truncate(time.Hour * 24).Add(-2*shardDuration + 1*shardGroupDuration).Add(meta.ShardGroupDeletedExpiration),
								DeletedAt: now.Truncate(time.Hour * 24).Add(-1 * shardDuration).Add(meta.ShardGroupDeletedExpiration),
							},
							// Shard group 3 is deleted and expired, but its shard can not be deleted.
							{
								ID:        3,
								StartTime: now.Truncate(time.Hour * 24).Add(-2*shardDuration + 2*shardGroupDuration).Add(meta.ShardGroupDeletedExpiration),
								EndTime:   now.Truncate(time.Hour * 24).Add(-2*shardDuration + 1*shardGroupDuration).Add(meta.ShardGroupDeletedExpiration),
								DeletedAt: now.Truncate(time.Hour * 24).Add(-1 * shardDuration).Add(meta.ShardGroupDeletedExpiration),
								Shards: []meta.ShardInfo{
									{
										ID: foreverShard,
									},
								},
							},
							// Shard group 4 is deleted, but not expired with a single shard.
							{
								ID:        4,
								StartTime: now.Truncate(time.Hour * 24).Add(-2*shardDuration + 0*shardGroupDuration),
								EndTime:   now.Truncate(time.Hour * 24).Add(-2*shardDuration + 1*shardGroupDuration),
								DeletedAt: now.Truncate(time.Hour * 24),
								Shards: []meta.ShardInfo{
									{
										ID: 104,
									},
								},
							},
							// Shard group 5 is active and should not be touched.
							{
								ID:        5,
								StartTime: now.Truncate(time.Hour * 24).Add(0 * shardGroupDuration),
								EndTime:   now.Truncate(time.Hour * 24).Add(1 * shardGroupDuration),
								Shards: []meta.ShardInfo{
									{
										ID: 105,
									},
								},
							},
							// Shard group 6 is a deleted and expired shard group with a phantom shard that doesn't exist in the store.
							{
								ID:        6,
								StartTime: now.Truncate(time.Hour * 24).Add(-2*shardDuration + 0*shardGroupDuration).Add(meta.ShardGroupDeletedExpiration),
								EndTime:   now.Truncate(time.Hour * 24).Add(-2*shardDuration + 1*shardGroupDuration).Add(meta.ShardGroupDeletedExpiration),
								DeletedAt: now.Truncate(time.Hour * 24).Add(-1 * shardDuration).Add(meta.ShardGroupDeletedExpiration),
								Shards: []meta.ShardInfo{
									{
										ID: phantomShard,
									},
								},
							},
							// Shard group 7 is deleted and expired, but its shard is in-use and should not be deleted.
							{
								ID:        7,
								StartTime: now.Truncate(time.Hour * 24).Add(-2*shardDuration + 2*shardGroupDuration).Add(meta.ShardGroupDeletedExpiration),
								EndTime:   now.Truncate(time.Hour * 24).Add(-2*shardDuration + 1*shardGroupDuration).Add(meta.ShardGroupDeletedExpiration),
								DeletedAt: now.Truncate(time.Hour * 24).Add(-1 * shardDuration).Add(meta.ShardGroupDeletedExpiration),
								Shards: []meta.ShardInfo{
									{
										ID: activeShard,
									},
								},
							},
						},
					},
				},
			},
		},
	}
	expData := dataUT.Clone()

	databasesFn := func() []meta.DatabaseInfo {
		return dataUT.Databases
	}
	deleteShardGroupFn := func(database, policy string, id uint64) error {
		return helpers.DataDeleteShardGroup(dataUT, now, database, policy, id)
	}
	dropShardFn := func(id uint64) error {
		dataUT.DropShard(id)
		return nil
	}
	pruneShardGroupsFn := func() error {
		// PruneShardGroups is the core functionality we are testing. We must use meta.Data's version.
		dataUT.PruneShardGroups(now.Add(meta.ShardGroupDeletedExpiration))
		return nil
	}
	mc := &internal.MetaClientMock{
		DatabasesFn:        databasesFn,
		DeleteShardGroupFn: deleteShardGroupFn,
		DropShardFn:        dropShardFn,
		PruneShardGroupsFn: pruneShardGroupsFn,
	}

	collectShards := func(d *meta.Data) map[uint64]struct{} {
		s := map[uint64]struct{}{}
		for _, db := range d.Databases {
			for _, rp := range db.RetentionPolicies {
				for _, sg := range rp.ShardGroups {
					for _, sh := range sg.Shards {
						s[sh.ID] = struct{}{}
					}
				}
			}
		}
		return s
	}

	// All these shards are yours except phantomShard. Attempt no deletion there.
	shards := collectShards(dataUT)
	delete(shards, phantomShard)

	shardIDs := func() []uint64 {
		return maps.Keys(shards)
	}
	inUseShards := map[uint64]struct{}{
		activeShard: struct{}{},
	}
	newReaderBlocks := make(map[uint64]int) // ShsrdID to number of active blocks
	setShardNewReadersBlocked := func(shardID uint64, blocked bool) error {
		t.Helper()
		require.Contains(t, shards, shardID, "SetShardNewReadersBlocked for non-existant shard %d", shardID)
		if blocked {
			newReaderBlocks[shardID]++
		} else {
			require.Greater(t, newReaderBlocks[shardID], 0)
			newReaderBlocks[shardID]--
		}
		return nil
	}
	deleteShard := func(shardID uint64) error {
		t.Helper()
		if _, ok := shards[shardID]; !ok {
			return tsdb.ErrShardNotFound
		}
		require.Greater(t, newReaderBlocks[shardID], 0, "DeleteShard called on shard without a reader block (%d)", shardID)
		require.NotContains(t, inUseShards, shardID, "DeleteShard called on an active shard (%d)", shardID)
		if shardID == foreverShard {
			return fmt.Errorf("unknown error deleting shard files for shard %d", shardID)
		}
		delete(shards, shardID)
		delete(newReaderBlocks, shardID)
		return nil
	}
	shardInUse := func(shardID uint64) (bool, error) {
		if _, valid := shards[shardID]; !valid {
			return false, tsdb.ErrShardNotFound
		}
		_, inUse := inUseShards[shardID]
		return inUse, nil
	}
	store := &internal.TSDBStoreMock{
		DeleteShardFn:               deleteShard,
		ShardIDsFn:                  shardIDs,
		SetShardNewReadersBlockedFn: setShardNewReadersBlocked,
		ShardInUseFn:                shardInUse,
	}

	s := retention.NewService(cfg)
	s.MetaClient = mc
	s.TSDBStore = store
	s.DropShardMetaRef = retention.OSSDropShardMetaRef(s.MetaClient)
	require.NoError(t, s.Open())
	deletionCheck := func() {
		t.Helper()
		s.DeletionCheck()
		for k, v := range newReaderBlocks {
			require.Zero(t, v, "shard %d has %d active blocks, should be zero", k, v)
		}
	}
	deletionCheck()

	// Adjust expData to make it look like we expect.
	require.NoError(t, helpers.DataNukeShardGroup(expData, "servers", "autogen", 1))
	require.NoError(t, helpers.DataNukeShardGroup(expData, "servers", "autogen", 2))
	expData.DropShard(104)
	require.NoError(t, helpers.DataNukeShardGroup(expData, "servers", "autogen", 6))

	require.Equal(t, expData, dataUT)
	require.Equal(t, collectShards(expData), shards)

	// Check that multiple duplicate calls to DeletionCheck don't make further changes.
	// This is mostly for our friend foreverShard.
	for i := 0; i < 10; i++ {
		deletionCheck()
		require.Equal(t, expData, dataUT)
		require.Equal(t, collectShards(expData), shards)
	}

	// Our heroic support team hos fixed the issue with foreverShard.
	foreverShard = math.MaxUint64
	deletionCheck()
	require.NoError(t, helpers.DataNukeShardGroup(expData, "servers", "autogen", 3))
	require.Equal(t, expData, dataUT)
	require.Equal(t, collectShards(expData), shards)

	require.NoError(t, s.Close())
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

	data := meta.Data{
		Databases: []meta.DatabaseInfo{
			{
				Name:                   "db0",
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
		},
	}

	config := retention.NewConfig()
	config.CheckInterval = toml.Duration(10 * time.Millisecond)
	s := NewService(config)
	s.MetaClient.DatabasesFn = func() []meta.DatabaseInfo {
		return data.Databases
	}

	done := make(chan struct{})
	deletedShardGroups := make(map[string]struct{})
	s.MetaClient.DeleteShardGroupFn = func(database, policy string, id uint64) error {
		for _, dbi := range data.Databases {
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

	dropShardDone := make(chan struct{})
	droppedShards := make(map[uint64]struct{})
	s.MetaClient.DropShardFn = func(id uint64) error {
		data.DropShard(id)
		if _, ok := droppedShards[id]; ok {
			t.Errorf("duplicate DropShard")
		}
		droppedShards[id] = struct{}{}
		if got, want := droppedShards, map[uint64]struct{}{
			2: struct{}{},
			3: struct{}{},
		}; reflect.DeepEqual(got, want) {
			close(dropShardDone)
		} else if len(got) > len(want) {
			t.Errorf("dropped too many shards")
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

	activeShards := map[uint64]struct{}{
		2: struct{}{},
		3: struct{}{},
		5: struct{}{},
		6: struct{}{},
	}
	deletedShards := make(map[uint64]struct{})
	s.TSDBStore.ShardIDsFn = func() []uint64 {
		return maps.Keys(activeShards)
	}
	s.TSDBStore.DeleteShardFn = func(shardID uint64) error {
		if _, ok := activeShards[shardID]; !ok {
			return tsdb.ErrShardNotFound
		}
		delete(activeShards, shardID)
		deletedShards[shardID] = struct{}{}
		return nil
	}

	shardBlockCount := map[uint64]int{}
	s.TSDBStore.SetShardNewReadersBlockedFn = func(shardID uint64, blocked bool) error {
		c := shardBlockCount[shardID]
		if blocked {
			c++
		} else {
			c--
			if c < 0 {
				return fmt.Errorf("too many unblocks: %d", shardID)
			}
		}
		shardBlockCount[shardID] = c
		return nil
	}

	s.TSDBStore.ShardInUseFn = func(shardID uint64) (bool, error) {
		c := shardBlockCount[shardID]
		if c <= 0 {
			return false, fmt.Errorf("ShardInUse called on unblocked shard: %d", shardID)
		}
		// TestService_DeletionCheck has tests for active shards. We're just checking for proper use.
		return false, nil
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
	case <-dropShardDone:
		timer.Stop()
	case <-timer.C:
		t.Errorf("timeout waiting for shard to be dropped")
	}

	timer = time.NewTimer(100 * time.Millisecond)
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
	data := meta.Data{
		Databases: []meta.DatabaseInfo{
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
		return data.Databases
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
		data.Databases[0].RetentionPolicies[0].ShardGroups[0].DeletedAt = time.Now().UTC()
		mu.Unlock()
		return nil
	}

	s.MetaClient.DropShardFn = func(shardID uint64) error {
		mu.Lock()
		defer mu.Unlock()
		data.DropShard(shardID)
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

	s.TSDBStore.SetShardNewReadersBlockedFn = func(shardID uint64, blocked bool) error {
		// This test does not simulate active / in-use shards. This can just be a stub.
		return nil
	}

	s.TSDBStore.ShardInUseFn = func(shardID uint64) (bool, error) {
		// This does not simulate active / in-use shards. This can just be a stub.
		return false, nil
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
	s.Service.DropShardMetaRef = retention.OSSDropShardMetaRef(s.Service.MetaClient)
	return s
}

package cluster_test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/influxdb/influxdb/cluster"
	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/meta"
	"github.com/influxdb/influxdb/tsdb"
)

// TestCoordinatorEnsureShardMappingOne tests that a single point maps to
// a single shard
func TestCoordinatorEnsureShardMappingOne(t *testing.T) {
	ms := MetaStore{}
	rp := NewRetentionPolicy("myp", time.Hour, 3)

	ms.RetentionPolicyFn = func(db, retentionPolicy string) (*meta.RetentionPolicyInfo, error) {
		return rp, nil
	}

	ms.CreateShardGroupIfNotExistsFn = func(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error) {
		return &rp.ShardGroups[0], nil
	}

	c := cluster.Coordinator{MetaStore: ms}
	pr := &cluster.WritePointsRequest{
		Database:         "mydb",
		RetentionPolicy:  "myrp",
		ConsistencyLevel: cluster.ConsistencyLevelOne,
	}
	pr.AddPoint("cpu", 1.0, time.Now(), nil)

	var (
		shardMappings *cluster.ShardMapping
		err           error
	)
	if shardMappings, err = c.MapShards(pr); err != nil {
		t.Fatalf("unexpected an error: %v", err)
	}

	if exp := 1; len(shardMappings.Points) != exp {
		t.Errorf("MapShards() len mismatch. got %v, exp %v", len(shardMappings.Points), exp)
	}
}

// TestCoordinatorEnsureShardMappingMultiple tests that MapShards maps multiple points
// across shard group boundaries to multiple shards
func TestCoordinatorEnsureShardMappingMultiple(t *testing.T) {
	ms := MetaStore{}
	rp := NewRetentionPolicy("myp", time.Hour, 3)
	AttachShardGroupInfo(rp, []uint64{1, 2, 3})
	AttachShardGroupInfo(rp, []uint64{1, 2, 3})

	ms.RetentionPolicyFn = func(db, retentionPolicy string) (*meta.RetentionPolicyInfo, error) {
		return rp, nil
	}

	ms.CreateShardGroupIfNotExistsFn = func(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error) {
		for i, sg := range rp.ShardGroups {
			if timestamp.Equal(sg.StartTime) || timestamp.After(sg.StartTime) && timestamp.Before(sg.EndTime) {
				return &rp.ShardGroups[i], nil
			}
		}
		panic("should not get here")
	}

	c := cluster.Coordinator{MetaStore: ms}
	pr := &cluster.WritePointsRequest{
		Database:         "mydb",
		RetentionPolicy:  "myrp",
		ConsistencyLevel: cluster.ConsistencyLevelOne,
	}

	// Three points that range over the shardGroup duration (1h) and should map to two
	// distinct shards
	pr.AddPoint("cpu", 1.0, time.Unix(0, 0), nil)
	pr.AddPoint("cpu", 2.0, time.Unix(0, 0).Add(time.Hour), nil)
	pr.AddPoint("cpu", 3.0, time.Unix(0, 0).Add(time.Hour+time.Second), nil)

	var (
		shardMappings *cluster.ShardMapping
		err           error
	)
	if shardMappings, err = c.MapShards(pr); err != nil {
		t.Fatalf("unexpected an error: %v", err)
	}

	if exp := 2; len(shardMappings.Points) != exp {
		t.Errorf("MapShards() len mismatch. got %v, exp %v", len(shardMappings.Points), exp)
	}

	for _, points := range shardMappings.Points {
		// First shard shoud have 1 point w/ first point added
		if len(points) == 1 && points[0].Time() != pr.Points[0].Time() {
			t.Fatalf("MapShards() value mismatch. got %v, exp %v", points[0].Time(), pr.Points[0].Time())
		}

		// Second shard shoud have the last two points added
		if len(points) == 2 && points[0].Time() != pr.Points[1].Time() {
			t.Fatalf("MapShards() value mismatch. got %v, exp %v", points[0].Time(), pr.Points[1].Time())
		}

		if len(points) == 2 && points[1].Time() != pr.Points[2].Time() {
			t.Fatalf("MapShards() value mismatch. got %v, exp %v", points[1].Time(), pr.Points[2].Time())
		}
	}
}

func TestCoordinatorWrite(t *testing.T) {

	tests := []struct {
		name        string
		consistency cluster.ConsistencyLevel

		// the responses returned by each shard write call.  node ID 1 = pos 0
		err    []error
		expErr error
	}{
		// Consistency one
		{
			name:        "write one success",
			consistency: cluster.ConsistencyLevelOne,
			err:         []error{nil, nil, nil},
			expErr:      nil,
		},
		{
			name:        "write one fail",
			consistency: cluster.ConsistencyLevelOne,
			err:         []error{fmt.Errorf("a failure"), fmt.Errorf("a failure"), fmt.Errorf("a failure")},
			expErr:      cluster.ErrWriteFailed,
		},

		// Consistency any
		{
			name:        "write any success",
			consistency: cluster.ConsistencyLevelAny,
			err:         []error{nil, nil, nil},
			expErr:      nil,
		},
		{
			name:        "write any failure",
			consistency: cluster.ConsistencyLevelAny,
			err:         []error{fmt.Errorf("a failure"), fmt.Errorf("a failure"), fmt.Errorf("a failure")},
			expErr:      cluster.ErrWriteFailed,
		},

		// Consistency all
		{
			name:        "write all success",
			consistency: cluster.ConsistencyLevelAll,
			err:         []error{nil, nil, nil},
			expErr:      nil,
		},
		{
			name:        "write all, 2/3, partial write",
			consistency: cluster.ConsistencyLevelAll,
			err:         []error{nil, fmt.Errorf("a failure"), nil},
			expErr:      cluster.ErrPartialWrite,
		},
		{
			name:        "write all, 1/3 (failure)",
			consistency: cluster.ConsistencyLevelAll,
			err:         []error{nil, fmt.Errorf("a failure"), fmt.Errorf("a failure")},
			expErr:      cluster.ErrPartialWrite,
		},

		// Consistency quorum
		{
			name:        "write quorum, 1/3 failure",
			consistency: cluster.ConsistencyLevelQuorum,
			err:         []error{fmt.Errorf("a failure"), fmt.Errorf("a failure"), nil},
			expErr:      cluster.ErrPartialWrite,
		},
		{
			name:        "write quorum, 2/3 success",
			consistency: cluster.ConsistencyLevelQuorum,
			err:         []error{nil, nil, fmt.Errorf("a failure")},
			expErr:      nil,
		},
		{
			name:        "write quorum, 3/3 success",
			consistency: cluster.ConsistencyLevelQuorum,
			err:         []error{nil, nil, nil},
			expErr:      nil,
		},

		// Error write failed
		{
			name:        "no writes succeed",
			consistency: cluster.ConsistencyLevelOne,
			err:         []error{fmt.Errorf("a failure"), fmt.Errorf("a failure"), fmt.Errorf("a failure")},
			expErr:      cluster.ErrWriteFailed,
		},
	}

	for _, test := range tests {

		pr := &cluster.WritePointsRequest{
			Database:         "mydb",
			RetentionPolicy:  "myrp",
			ConsistencyLevel: test.consistency,
		}

		// Three points that range over the shardGroup duration (1h) and should map to two
		// distinct shards
		pr.AddPoint("cpu", 1.0, time.Unix(0, 0), nil)
		pr.AddPoint("cpu", 2.0, time.Unix(0, 0).Add(time.Hour), nil)
		pr.AddPoint("cpu", 3.0, time.Unix(0, 0).Add(time.Hour+time.Second), nil)

		// copy to prevent data race
		theTest := test
		sm := cluster.NewShardMapping()
		sm.MapPoint(
			&meta.ShardInfo{ID: uint64(1), OwnerIDs: []uint64{uint64(1), uint64(2), uint64(3)}},
			pr.Points[0])
		sm.MapPoint(
			&meta.ShardInfo{ID: uint64(2), OwnerIDs: []uint64{uint64(1), uint64(2), uint64(3)}},
			pr.Points[1])
		sm.MapPoint(
			&meta.ShardInfo{ID: uint64(2), OwnerIDs: []uint64{uint64(1), uint64(2), uint64(3)}},
			pr.Points[2])

		// Local cluster.Node ShardWriter
		// lock on the write increment since these functions get called in parallel
		var mu sync.Mutex
		dn := &fakeShardWriter{
			ShardWriteFn: func(shardID, nodeID uint64, points []tsdb.Point) error {
				mu.Lock()
				defer mu.Unlock()
				return theTest.err[int(nodeID)-1]
			},
		}

		store := &fakeStore{
			WriteFn: func(shardID uint64, points []tsdb.Point) error {
				mu.Lock()
				defer mu.Unlock()
				return theTest.err[0]
			},
		}

		ms := newTestMetaStore()
		c := cluster.Coordinator{
			MetaStore:     ms,
			ClusterWriter: dn,
			Store:         store,
		}

		if err := c.Write(pr); err != test.expErr {
			t.Errorf("Coordinator.Write(): '%s' failed: got %v, exp %v", test.name, err, test.expErr)
		}
	}
}

var (
	shardID uint64
)

type fakeShardWriter struct {
	ShardWriteFn func(shardID, nodeID uint64, points []tsdb.Point) error
}

func (f *fakeShardWriter) Write(shardID, nodeID uint64, points []tsdb.Point) error {
	return f.ShardWriteFn(shardID, nodeID, points)
}

type fakeStore struct {
	WriteFn       func(shardID uint64, points []tsdb.Point) error
	CreateShardfn func(database, retentionPolicy string, shardID uint64) error
}

func (f *fakeStore) WriteToShard(shardID uint64, points []tsdb.Point) error {
	return f.WriteFn(shardID, points)
}

func (f *fakeStore) CreateShard(database, retentionPolicy string, shardID uint64) error {
	return f.CreateShardfn(database, retentionPolicy, shardID)
}

func newTestMetaStore() *MetaStore {
	ms := &MetaStore{}
	rp := NewRetentionPolicy("myp", time.Hour, 3)
	AttachShardGroupInfo(rp, []uint64{1, 2, 3})
	AttachShardGroupInfo(rp, []uint64{1, 2, 3})

	ms.RetentionPolicyFn = func(db, retentionPolicy string) (*meta.RetentionPolicyInfo, error) {
		return rp, nil
	}

	ms.CreateShardGroupIfNotExistsFn = func(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error) {
		for i, sg := range rp.ShardGroups {
			if timestamp.Equal(sg.StartTime) || timestamp.After(sg.StartTime) && timestamp.Before(sg.EndTime) {
				return &rp.ShardGroups[i], nil
			}
		}
		panic("should not get here")
	}
	return ms
}

type MetaStore struct {
	OpenFn  func(path string) error
	CloseFn func() error

	CreateContinuousQueryFn func(query string) (*meta.ContinuousQueryInfo, error)
	DropContinuousQueryFn   func(query string) error

	NodeFn       func(id uint64) (*meta.NodeInfo, error)
	NodeByHostFn func(host string) (*meta.NodeInfo, error)
	CreateNodeFn func(host string) (*meta.NodeInfo, error)
	DeleteNodeFn func(id uint64) error

	DatabaseFn                  func(name string) (*meta.DatabaseInfo, error)
	CreateDatabaseFn            func(name string) (*meta.DatabaseInfo, error)
	CreateDatabaseIfNotExistsFn func(name string) (*meta.DatabaseInfo, error)
	DropDatabaseFn              func(name string) error

	RetentionPolicyFn                  func(database, name string) (*meta.RetentionPolicyInfo, error)
	CreateRetentionPolicyFn            func(database string, rp *meta.RetentionPolicyInfo) (*meta.RetentionPolicyInfo, error)
	CreateRetentionPolicyIfNotExistsFn func(database string, rp *meta.RetentionPolicyInfo) (*meta.RetentionPolicyInfo, error)
	SetDefaultRetentionPolicyFn        func(database, name string) error
	UpdateRetentionPolicyFn            func(database, name string, rpu *meta.RetentionPolicyUpdate) (*meta.RetentionPolicyInfo, error)
	DeleteRetentionPolicyFn            func(database, name string) error

	ShardGroupFn                  func(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error)
	CreateShardGroupIfNotExistsFn func(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error)
	DeleteShardGroupFn            func(database, policy string, shardID uint64) error

	UserFn         func(username string) (*meta.UserInfo, error)
	CreateUserFn   func(username, password string, admin bool) (*meta.UserInfo, error)
	UpdateUserFn   func(username, password string) (*meta.UserInfo, error)
	DeleteUserFn   func(username string) error
	SetPrivilegeFn func(p influxql.Privilege, username string, dbname string) error
}

func (m MetaStore) Open(path string) error {
	return m.OpenFn(path)
}

func (m MetaStore) Close() error {
	return m.CloseFn()
}

func (m MetaStore) CreateContinuousQuery(query string) (*meta.ContinuousQueryInfo, error) {
	return m.CreateContinuousQueryFn(query)
}

func (m MetaStore) DropContinuousQuery(query string) error {
	return m.DropContinuousQueryFn(query)
}

func (m MetaStore) Node(id uint64) (*meta.NodeInfo, error) {
	return m.NodeFn(id)
}

func (m MetaStore) NodeByHost(host string) (*meta.NodeInfo, error) {
	return m.NodeByHostFn(host)
}

func (m MetaStore) CreateNode(host string) (*meta.NodeInfo, error) {
	return m.CreateNodeFn(host)
}

func (m MetaStore) DeleteNode(id uint64) error {
	return m.DeleteNodeFn(id)
}

func (m MetaStore) Database(name string) (*meta.DatabaseInfo, error) {
	return m.DatabaseFn(name)
}

func (m MetaStore) CreateDatabase(name string) (*meta.DatabaseInfo, error) {
	return m.CreateDatabaseFn(name)
}

func (m MetaStore) CreateDatabaseIfNotExists(name string) (*meta.DatabaseInfo, error) {
	return m.CreateDatabaseIfNotExistsFn(name)
}

func (m MetaStore) DropDatabase(name string) error {
	return m.DropDatabaseFn(name)
}

func (m MetaStore) RetentionPolicy(database, name string) (*meta.RetentionPolicyInfo, error) {
	return m.RetentionPolicyFn(database, name)
}

func (m MetaStore) CreateRetentionPolicy(database string, rp *meta.RetentionPolicyInfo) (*meta.RetentionPolicyInfo, error) {
	return m.CreateRetentionPolicyFn(database, rp)
}

func (m MetaStore) CreateRetentionPolicyIfNotExists(database string, rp *meta.RetentionPolicyInfo) (*meta.RetentionPolicyInfo, error) {
	return m.CreateRetentionPolicyIfNotExistsFn(database, rp)
}

func (m MetaStore) SetDefaultRetentionPolicy(database, name string) error {
	return m.SetDefaultRetentionPolicyFn(database, name)
}

func (m MetaStore) UpdateRetentionPolicy(database, name string, rpu *meta.RetentionPolicyUpdate) (*meta.RetentionPolicyInfo, error) {
	return m.UpdateRetentionPolicyFn(database, name, rpu)
}

func (m MetaStore) DeleteRetentionPolicy(database, name string) error {
	return m.DeleteRetentionPolicyFn(database, name)
}

func (m MetaStore) ShardGroup(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error) {
	return m.ShardGroupFn(database, policy, timestamp)
}

func (m MetaStore) CreateShardGroupIfNotExists(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error) {
	return m.CreateShardGroupIfNotExistsFn(database, policy, timestamp)
}

func (m MetaStore) DeleteShardGroup(database, policy string, shardID uint64) error {
	return m.DeleteShardGroupFn(database, policy, shardID)
}

func (m MetaStore) User(username string) (*meta.UserInfo, error) {
	return m.UserFn(username)
}

func (m MetaStore) CreateUser(username, password string, admin bool) (*meta.UserInfo, error) {
	return m.CreateUserFn(username, password, admin)
}

func (m MetaStore) UpdateUser(username, password string) (*meta.UserInfo, error) {
	return m.UpdateUserFn(username, password)
}
func (m MetaStore) DeleteUser(username string) error {
	return m.DeleteUserFn(username)
}

func (m MetaStore) SetPrivilege(p influxql.Privilege, username string, dbname string) error {
	return m.SetPrivilegeFn(p, username, dbname)
}

func NewRetentionPolicy(name string, duration time.Duration, nodeCount int) *meta.RetentionPolicyInfo {

	shards := []meta.ShardInfo{}
	ownerIDs := []uint64{}
	for i := 1; i <= nodeCount; i++ {
		ownerIDs = append(ownerIDs, uint64(i))
	}

	// each node is fully replicated with each other
	shards = append(shards, meta.ShardInfo{
		ID:       nextShardID(),
		OwnerIDs: ownerIDs,
	})

	rp := &meta.RetentionPolicyInfo{
		Name:               "myrp",
		ReplicaN:           nodeCount,
		Duration:           duration,
		ShardGroupDuration: duration,
		ShardGroups: []meta.ShardGroupInfo{
			meta.ShardGroupInfo{
				ID:        nextShardID(),
				StartTime: time.Unix(0, 0),
				EndTime:   time.Unix(0, 0).Add(duration).Add(-1),
				Shards:    shards,
			},
		},
	}
	return rp
}

func AttachShardGroupInfo(rp *meta.RetentionPolicyInfo, ownerIDs []uint64) {
	var startTime, endTime time.Time
	if len(rp.ShardGroups) == 0 {
		startTime = time.Unix(0, 0)
	} else {
		startTime = rp.ShardGroups[len(rp.ShardGroups)-1].StartTime.Add(rp.ShardGroupDuration)
	}
	endTime = startTime.Add(rp.ShardGroupDuration).Add(-1)

	sh := meta.ShardGroupInfo{
		ID:        uint64(len(rp.ShardGroups) + 1),
		StartTime: startTime,
		EndTime:   endTime,
		Shards: []meta.ShardInfo{
			meta.ShardInfo{
				ID:       nextShardID(),
				OwnerIDs: ownerIDs,
			},
		},
	}
	rp.ShardGroups = append(rp.ShardGroups, sh)
}

func nextShardID() uint64 {
	return atomic.AddUint64(&shardID, 1)
}

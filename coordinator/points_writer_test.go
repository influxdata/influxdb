package coordinator_test

import (
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/coordinator"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/stretchr/testify/require"
)

// TODO(benbjohnson): Rewrite tests to use cluster_test.MetaClient.

// Ensures the points writer maps a single point to a single shard.
func TestPointsWriter_MapShards_One(t *testing.T) {
	ms := PointsWriterMetaClient{}
	rp := NewRetentionPolicy("myp", time.Now(), time.Hour, 3, 0, 0)

	ms.NodeIDFn = func() uint64 { return 1 }
	ms.RetentionPolicyFn = func(db, retentionPolicy string) (*meta.RetentionPolicyInfo, error) {
		return rp, nil
	}

	ms.CreateShardGroupIfNotExistsFn = func(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error) {
		return &rp.ShardGroups[0], nil
	}

	c := coordinator.PointsWriter{MetaClient: ms}
	pr := &coordinator.WritePointsRequest{
		Database:        "mydb",
		RetentionPolicy: "myrp",
	}
	pr.AddPoint("cpu", 1.0, time.Now(), nil)

	var (
		shardMappings *coordinator.ShardMapping
		err           error
	)
	if shardMappings, err = c.MapShards(pr); err != nil {
		t.Fatalf("unexpected an error: %v", err)
	}

	if exp := 1; len(shardMappings.Points) != exp {
		t.Errorf("MapShards() len mismatch. got %v, exp %v", len(shardMappings.Points), exp)
	}
}

func TestPointsWriter_MapShards_WriteLimits(t *testing.T) {
	ms := PointsWriterMetaClient{}
	c := coordinator.NewPointsWriter()

	MustParseDuration := func(s string) time.Duration {
		d, err := time.ParseDuration(s)
		require.NoError(t, err, "failed to parse duration: %q", s)
		return d
	}

	pastWriteLimit := MustParseDuration("10m")
	futureWriteLimit := MustParseDuration("15m")
	rp := NewRetentionPolicy("myp", time.Now().Add(-time.Minute*45), 3*time.Hour, 3, futureWriteLimit, pastWriteLimit)

	ms.NodeIDFn = func() uint64 { return 1 }
	ms.RetentionPolicyFn = func(db, retentionPolicy string) (*meta.RetentionPolicyInfo, error) {
		return rp, nil
	}

	ms.CreateShardGroupIfNotExistsFn = func(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error) {
		return &rp.ShardGroups[0], nil
	}

	c.MetaClient = ms

	pr := &coordinator.WritePointsRequest{
		Database:        "mydb",
		RetentionPolicy: "myrp",
	}

	pr.AddPoint("cpu", 0.0, time.Now(), nil)
	pr.AddPoint("cpu", 1.0, time.Now().Add(time.Second), nil)
	pr.AddPoint("cpu", 2.0, time.Now().Add(time.Minute*30), nil)
	pr.AddPoint("cpu", -1.0, time.Now().Add(-time.Minute*5), nil)
	pr.AddPoint("cpu", -2.0, time.Now().Add(-time.Minute*20), nil)
	var (
		shardMappings *coordinator.ShardMapping
		err           error
	)
	if shardMappings, err = c.MapShards(pr); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if exp := 1; len(shardMappings.Points) != exp {
		t.Errorf("MapShards() len mismatch. got %v, exp %v", len(shardMappings.Points), exp)
	}

	p := func() []models.Point {
		for _, v := range shardMappings.Points {
			return v
		}
		return nil
	}()
	values := []float64{0.0, 1.0, -1.0}
	dropped := []float64{2.0, -2.0}
	verify :=
		func(p []models.Point, values []float64) {
			require.Equal(t, len(values), len(p), "unexpected number of points")
			for i, expV := range values {
				f, err := p[i].Fields()
				require.NoError(t, err, "error retrieving fields")
				v, ok := f["value"]
				require.True(t, ok, "\"value\" field not found")
				require.Equal(t, expV, v, "unexpected value")
			}
		}
	verify(p, values)
	verify(shardMappings.Dropped, dropped)
}

// Ensures the points writer maps to a new shard group when the shard duration
// is changed.
func TestPointsWriter_MapShards_AlterShardDuration(t *testing.T) {
	ms := PointsWriterMetaClient{}
	rp := NewRetentionPolicy("myp", time.Now(), time.Hour, 3, 0, 0)

	ms.NodeIDFn = func() uint64 { return 1 }
	ms.RetentionPolicyFn = func(db, retentionPolicy string) (*meta.RetentionPolicyInfo, error) {
		return rp, nil
	}

	var (
		i   int
		now = time.Now()
	)

	ms.CreateShardGroupIfNotExistsFn = func(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error) {
		sg := []meta.ShardGroupInfo{
			meta.ShardGroupInfo{
				Shards:    make([]meta.ShardInfo, 1),
				StartTime: now, EndTime: now.Add(rp.Duration).Add(-1),
			},
			meta.ShardGroupInfo{
				Shards:    make([]meta.ShardInfo, 1),
				StartTime: now.Add(time.Hour), EndTime: now.Add(3 * time.Hour).Add(rp.Duration).Add(-1),
			},
		}[i]
		i++
		return &sg, nil
	}

	c := coordinator.NewPointsWriter()
	c.MetaClient = ms

	pr := &coordinator.WritePointsRequest{
		Database:        "mydb",
		RetentionPolicy: "myrp",
	}
	pr.AddPoint("cpu", 1.0, now, nil)
	pr.AddPoint("cpu", 2.0, now.Add(2*time.Second), nil)

	var (
		shardMappings *coordinator.ShardMapping
		err           error
	)
	if shardMappings, err = c.MapShards(pr); err != nil {
		t.Fatalf("unexpected an error: %v", err)
	}

	if got, exp := len(shardMappings.Points[0]), 2; got != exp {
		t.Fatalf("got %d point(s), expected %d", got, exp)
	}

	if got, exp := len(shardMappings.Shards), 1; got != exp {
		t.Errorf("got %d shard(s), expected %d", got, exp)
	}

	// Now we alter the retention policy duration.
	rp.ShardGroupDuration = 3 * time.Hour

	pr = &coordinator.WritePointsRequest{
		Database:        "mydb",
		RetentionPolicy: "myrp",
	}
	pr.AddPoint("cpu", 1.0, now.Add(2*time.Hour), nil)

	// Point is beyond previous shard group so a new shard group should be
	// created.
	if _, err = c.MapShards(pr); err != nil {
		t.Fatalf("unexpected an error: %v", err)
	}

	// We can check value of i since it's only incremeneted when a shard group
	// is created.
	if got, exp := i, 2; got != exp {
		t.Fatal("new shard group was not created, expected it to be")
	}
}

// Ensures the points writer maps a multiple points across shard group boundaries.
func TestPointsWriter_MapShards_Multiple(t *testing.T) {
	ms := PointsWriterMetaClient{}
	rp := NewRetentionPolicy("myp", time.Now(), time.Hour, 3, 0, 0)
	rp.ShardGroupDuration = time.Hour
	AttachShardGroupInfo(rp, []meta.ShardOwner{
		{NodeID: 1},
		{NodeID: 2},
		{NodeID: 3},
	})
	AttachShardGroupInfo(rp, []meta.ShardOwner{
		{NodeID: 1},
		{NodeID: 2},
		{NodeID: 3},
	})

	ms.NodeIDFn = func() uint64 { return 1 }
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

	c := coordinator.NewPointsWriter()
	c.MetaClient = ms
	defer c.Close()
	pr := &coordinator.WritePointsRequest{
		Database:        "mydb",
		RetentionPolicy: "myrp",
	}

	// Three points that range over the shardGroup duration (1h) and should map to two
	// distinct shards
	pr.AddPoint("cpu", 1.0, time.Now(), nil)
	pr.AddPoint("cpu", 2.0, time.Now().Add(time.Hour), nil)
	pr.AddPoint("cpu", 3.0, time.Now().Add(time.Hour+time.Second), nil)

	var (
		shardMappings *coordinator.ShardMapping
		err           error
	)
	if shardMappings, err = c.MapShards(pr); err != nil {
		t.Fatalf("unexpected an error: %v", err)
	}

	if exp := 2; len(shardMappings.Points) != exp {
		t.Errorf("MapShards() len mismatch. got %v, exp %v", len(shardMappings.Points), exp)
	}

	for _, points := range shardMappings.Points {
		// First shard should have 1 point w/ first point added
		if len(points) == 1 && points[0].Time() != pr.Points[0].Time() {
			t.Fatalf("MapShards() value mismatch. got %v, exp %v", points[0].Time(), pr.Points[0].Time())
		}

		// Second shard should have the last two points added
		if len(points) == 2 && points[0].Time() != pr.Points[1].Time() {
			t.Fatalf("MapShards() value mismatch. got %v, exp %v", points[0].Time(), pr.Points[1].Time())
		}

		if len(points) == 2 && points[1].Time() != pr.Points[2].Time() {
			t.Fatalf("MapShards() value mismatch. got %v, exp %v", points[1].Time(), pr.Points[2].Time())
		}
	}
}

// Ensures the points writer does not map points beyond the retention policy.
func TestPointsWriter_MapShards_Invalid(t *testing.T) {
	ms := PointsWriterMetaClient{}
	rp := NewRetentionPolicy("myp", time.Now(), time.Hour, 3, 0, 0)

	ms.RetentionPolicyFn = func(db, retentionPolicy string) (*meta.RetentionPolicyInfo, error) {
		return rp, nil
	}

	ms.CreateShardGroupIfNotExistsFn = func(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error) {
		return &rp.ShardGroups[0], nil
	}

	c := coordinator.NewPointsWriter()
	c.MetaClient = ms
	defer c.Close()
	pr := &coordinator.WritePointsRequest{
		Database:        "mydb",
		RetentionPolicy: "myrp",
	}

	// Add a point that goes beyond the current retention policy.
	pr.AddPoint("cpu", 1.0, time.Now().Add(-2*time.Hour), nil)

	var (
		shardMappings *coordinator.ShardMapping
		err           error
	)
	if shardMappings, err = c.MapShards(pr); err != nil {
		t.Fatalf("unexpected an error: %v", err)
	}

	if got, exp := len(shardMappings.Points), 0; got != exp {
		t.Errorf("MapShards() len mismatch. got %v, exp %v", got, exp)
	}

	if got, exp := len(shardMappings.Dropped), 1; got != exp {
		t.Fatalf("MapShard() dropped mismatch: got %v, exp %v", got, exp)
	}
}

func TestPointsWriter_WritePoints(t *testing.T) {
	tests := []struct {
		name            string
		database        string
		retentionPolicy string

		// the responses returned by each shard write call.  node ID 1 = pos 0
		err    []error
		expErr error
	}{
		{
			name:            "write one success",
			database:        "mydb",
			retentionPolicy: "myrp",
			err:             []error{nil, nil, nil},
			expErr:          nil,
		},

		// Write to non-existent database
		{
			name:            "write to non-existent database",
			database:        "doesnt_exist",
			retentionPolicy: "",
			err:             []error{nil, nil, nil},
			expErr:          fmt.Errorf("database not found: doesnt_exist"),
		},
	}

	for _, test := range tests {

		pr := &coordinator.WritePointsRequest{
			Database:        test.database,
			RetentionPolicy: test.retentionPolicy,
		}

		// Ensure that the test shard groups are created before the points
		// are created.
		ms := NewPointsWriterMetaClient()

		// Three points that range over the shardGroup duration (1h) and should map to two
		// distinct shards
		pr.AddPoint("cpu", 1.0, time.Now(), nil)
		pr.AddPoint("cpu", 2.0, time.Now().Add(time.Hour), nil)
		pr.AddPoint("cpu", 3.0, time.Now().Add(time.Hour+time.Second), nil)

		// copy to prevent data race
		theTest := test
		sm := coordinator.NewShardMapping(16)
		sm.MapPoint(nil, &meta.ShardInfo{ID: uint64(1), Owners: []meta.ShardOwner{
			{NodeID: 1},
			{NodeID: 2},
			{NodeID: 3},
		}}, pr.Points[0])
		sm.MapPoint(nil, &meta.ShardInfo{ID: uint64(2), Owners: []meta.ShardOwner{
			{NodeID: 1},
			{NodeID: 2},
			{NodeID: 3},
		}}, pr.Points[1])
		sm.MapPoint(nil, &meta.ShardInfo{ID: uint64(2), Owners: []meta.ShardOwner{
			{NodeID: 1},
			{NodeID: 2},
			{NodeID: 3},
		}}, pr.Points[2])

		// Local coordinator.Node ShardWriter
		// lock on the write increment since these functions get called in parallel
		var mu sync.Mutex

		store := &fakeStore{
			WriteFn: func(_ tsdb.WriteContext, shardID uint64, points []models.Point) error {
				mu.Lock()
				defer mu.Unlock()
				return theTest.err[0]
			},
		}

		ms.DatabaseFn = func(database string) *meta.DatabaseInfo {
			return nil
		}
		ms.NodeIDFn = func() uint64 { return 1 }

		subPoints := make(chan *coordinator.WritePointsRequest, 1)
		sub := Subscriber{}
		sub.SendFn = func(wr *coordinator.WritePointsRequest) {
			subPoints <- wr
		}

		c := coordinator.NewPointsWriter()
		c.MetaClient = ms
		c.TSDBStore = store
		c.Subscriber = sub
		c.Node = &influxdb.Node{ID: 1}

		c.Open()
		defer c.Close()

		err := c.WritePointsPrivileged(tsdb.WriteContext{}, pr.Database, pr.RetentionPolicy, models.ConsistencyLevelOne, pr.Points)
		if err == nil && test.expErr != nil {
			t.Errorf("PointsWriter.WritePointsPrivileged(): '%s' error: got %v, exp %v", test.name, err, test.expErr)
		}

		if err != nil && test.expErr == nil {
			t.Errorf("PointsWriter.WritePointsPrivileged(): '%s' error: got %v, exp %v", test.name, err, test.expErr)
		}
		if err != nil && test.expErr != nil && err.Error() != test.expErr.Error() {
			t.Errorf("PointsWriter.WritePointsPrivileged(): '%s' error: got %v, exp %v", test.name, err, test.expErr)
		}
		if test.expErr == nil {
			select {
			case p := <-subPoints:
				if !reflect.DeepEqual(p, pr) {
					t.Errorf("PointsWriter.WritePointsPrivileged(): '%s' error: unexpected WritePointsRequest got %v, exp %v", test.name, p, pr)
				}
			default:
				t.Errorf("PointsWriter.WritePointsPrivileged(): '%s' error: Subscriber.Points not called", test.name)
			}
		}
	}
}

func TestPointsWriter_WritePoints_Dropped(t *testing.T) {
	pr := &coordinator.WritePointsRequest{
		Database:        "mydb",
		RetentionPolicy: "myrp",
	}

	// Ensure that the test shard groups are created before the points
	// are created.
	ms := NewPointsWriterMetaClient()

	// Three points that range over the shardGroup duration (1h) and should map to two
	// distinct shards
	pr.AddPoint("cpu", 1.0, time.Now().Add(-24*time.Hour), nil)

	// copy to prevent data race
	sm := coordinator.NewShardMapping(16)

	// ShardMapper dropped this point
	sm.Dropped = append(sm.Dropped, pr.Points[0])

	// Local coordinator.Node ShardWriter
	// lock on the write increment since these functions get called in parallel
	var mu sync.Mutex

	store := &fakeStore{
		WriteFn: func(_ tsdb.WriteContext, shardID uint64, points []models.Point) error {
			mu.Lock()
			defer mu.Unlock()
			return nil
		},
	}

	ms.DatabaseFn = func(database string) *meta.DatabaseInfo {
		return nil
	}
	ms.NodeIDFn = func() uint64 { return 1 }

	subPoints := make(chan *coordinator.WritePointsRequest, 1)
	sub := Subscriber{}
	sub.SendFn = func(wr *coordinator.WritePointsRequest) {
		subPoints <- wr
	}

	c := coordinator.NewPointsWriter()
	c.MetaClient = ms
	c.TSDBStore = store
	c.Subscriber = sub
	c.Node = &influxdb.Node{ID: 1}

	c.Open()
	defer c.Close()

	err := c.WritePointsPrivileged(tsdb.WriteContext{}, pr.Database, pr.RetentionPolicy, models.ConsistencyLevelOne, pr.Points)
	if _, ok := err.(tsdb.PartialWriteError); !ok {
		t.Errorf("PointsWriter.WritePoints(): got %v, exp %v", err, tsdb.PartialWriteError{})
	}
}

type fakePointsWriter struct {
	WritePointsIntoFn func(*coordinator.IntoWriteRequest) error
}

func (f *fakePointsWriter) WritePointsInto(req *coordinator.IntoWriteRequest) error {
	return f.WritePointsIntoFn(req)
}

func TestBufferedPointsWriter(t *testing.T) {
	db := "db0"
	rp := "rp0"
	capacity := 10000

	writePointsIntoCnt := 0
	pointsWritten := []models.Point{}

	reset := func() {
		writePointsIntoCnt = 0
		pointsWritten = pointsWritten[:0]
	}

	fakeWriter := &fakePointsWriter{
		WritePointsIntoFn: func(req *coordinator.IntoWriteRequest) error {
			writePointsIntoCnt++
			pointsWritten = append(pointsWritten, req.Points...)
			return nil
		},
	}

	w := coordinator.NewBufferedPointsWriter(fakeWriter, db, rp, capacity)

	// Test that capacity and length are correct for new buffered writer.
	if w.Cap() != capacity {
		t.Fatalf("exp %d, got %d", capacity, w.Cap())
	} else if w.Len() != 0 {
		t.Fatalf("exp %d, got %d", 0, w.Len())
	}

	// Test flushing an empty buffer.
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	} else if writePointsIntoCnt > 0 {
		t.Fatalf("exp 0, got %d", writePointsIntoCnt)
	}

	// Test writing zero points.
	if err := w.WritePointsInto(&coordinator.IntoWriteRequest{
		Database:        db,
		RetentionPolicy: rp,
		Points:          []models.Point{},
	}); err != nil {
		t.Fatal(err)
	} else if writePointsIntoCnt > 0 {
		t.Fatalf("exp 0, got %d", writePointsIntoCnt)
	} else if w.Len() > 0 {
		t.Fatalf("exp 0, got %d", w.Len())
	}

	// Test writing single large bunch of points points.
	req := coordinator.WritePointsRequest{
		Database:        db,
		RetentionPolicy: rp,
	}

	numPoints := int(float64(capacity) * 5.5)
	for i := 0; i < numPoints; i++ {
		req.AddPoint("cpu", float64(i), time.Now().Add(time.Duration(i)*time.Second), nil)
	}

	r := coordinator.IntoWriteRequest(req)
	if err := w.WritePointsInto(&r); err != nil {
		t.Fatal(err)
	} else if writePointsIntoCnt != 5 {
		t.Fatalf("exp 5, got %d", writePointsIntoCnt)
	} else if w.Len() != capacity/2 {
		t.Fatalf("exp %d, got %d", capacity/2, w.Len())
	} else if len(pointsWritten) != numPoints-capacity/2 {
		t.Fatalf("exp %d, got %d", numPoints-capacity/2, len(pointsWritten))
	}

	if err := w.Flush(); err != nil {
		t.Fatal(err)
	} else if writePointsIntoCnt != 6 {
		t.Fatalf("exp 6, got %d", writePointsIntoCnt)
	} else if w.Len() != 0 {
		t.Fatalf("exp 0, got %d", w.Len())
	} else if len(pointsWritten) != numPoints {
		t.Fatalf("exp %d, got %d", numPoints, len(pointsWritten))
	} else if !reflect.DeepEqual(r.Points, pointsWritten) {
		t.Fatal("points don't match")
	}

	reset()

	// Test writing points one at a time.
	for i := range r.Points {
		if err := w.WritePointsInto(&coordinator.IntoWriteRequest{
			Database:        db,
			RetentionPolicy: rp,
			Points:          r.Points[i : i+1],
		}); err != nil {
			t.Fatal(err)
		}
	}

	if err := w.Flush(); err != nil {
		t.Fatal(err)
	} else if writePointsIntoCnt != 6 {
		t.Fatalf("exp 6, got %d", writePointsIntoCnt)
	} else if w.Len() != 0 {
		t.Fatalf("exp 0, got %d", w.Len())
	} else if len(pointsWritten) != numPoints {
		t.Fatalf("exp %d, got %d", numPoints, len(pointsWritten))
	} else if !reflect.DeepEqual(r.Points, pointsWritten) {
		t.Fatal("points don't match")
	}
}

var shardID uint64

type fakeStore struct {
	WriteFn       func(ctx tsdb.WriteContext, shardID uint64, points []models.Point) error
	CreateShardfn func(database, retentionPolicy string, shardID uint64, enabled bool) error
}

func (f *fakeStore) WriteToShard(ctx tsdb.WriteContext, shardID uint64, points []models.Point) error {
	return f.WriteFn(ctx, shardID, points)
}

func (f *fakeStore) CreateShard(database, retentionPolicy string, shardID uint64, enabled bool) error {
	return f.CreateShardfn(database, retentionPolicy, shardID, enabled)
}

func NewPointsWriterMetaClient() *PointsWriterMetaClient {
	ms := &PointsWriterMetaClient{}
	rp := NewRetentionPolicy("myp", time.Now(), time.Hour, 3, 0, 0)
	AttachShardGroupInfo(rp, []meta.ShardOwner{
		{NodeID: 1},
		{NodeID: 2},
		{NodeID: 3},
	})
	AttachShardGroupInfo(rp, []meta.ShardOwner{
		{NodeID: 1},
		{NodeID: 2},
		{NodeID: 3},
	})

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

type PointsWriterMetaClient struct {
	NodeIDFn                      func() uint64
	RetentionPolicyFn             func(database, name string) (*meta.RetentionPolicyInfo, error)
	CreateShardGroupIfNotExistsFn func(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error)
	DatabaseFn                    func(database string) *meta.DatabaseInfo
	ShardOwnerFn                  func(shardID uint64) (string, string, *meta.ShardGroupInfo)
}

func (m PointsWriterMetaClient) NodeID() uint64 { return m.NodeIDFn() }

func (m PointsWriterMetaClient) RetentionPolicy(database, name string) (*meta.RetentionPolicyInfo, error) {
	return m.RetentionPolicyFn(database, name)
}

func (m PointsWriterMetaClient) CreateShardGroup(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error) {
	return m.CreateShardGroupIfNotExistsFn(database, policy, timestamp)
}

func (m PointsWriterMetaClient) Database(database string) *meta.DatabaseInfo {
	return m.DatabaseFn(database)
}

func (m PointsWriterMetaClient) ShardOwner(shardID uint64) (string, string, *meta.ShardGroupInfo) {
	return m.ShardOwnerFn(shardID)
}

type Subscriber struct {
	SendFn func(*coordinator.WritePointsRequest)
}

func (s Subscriber) Send(wr *coordinator.WritePointsRequest) {
	s.SendFn(wr)
}

func NewRetentionPolicy(name string, start time.Time, duration time.Duration, nodeCount int, futureWriteLimit time.Duration, pastWriteLimit time.Duration) *meta.RetentionPolicyInfo {
	shards := []meta.ShardInfo{}
	owners := []meta.ShardOwner{}
	for i := 1; i <= nodeCount; i++ {
		owners = append(owners, meta.ShardOwner{NodeID: uint64(i)})
	}

	// each node is fully replicated with each other
	shards = append(shards, meta.ShardInfo{
		ID:     nextShardID(),
		Owners: owners,
	})

	rp := &meta.RetentionPolicyInfo{
		Name:               "myrp",
		ReplicaN:           nodeCount,
		Duration:           duration,
		ShardGroupDuration: duration,
		ShardGroups: []meta.ShardGroupInfo{
			meta.ShardGroupInfo{
				ID:        nextShardID(),
				StartTime: start,
				EndTime:   start.Add(duration).Add(-1),
				Shards:    shards,
			},
		},
		FutureWriteLimit: futureWriteLimit,
		PastWriteLimit:   pastWriteLimit,
	}
	return rp
}

func AttachShardGroupInfo(rp *meta.RetentionPolicyInfo, owners []meta.ShardOwner) {
	var startTime, endTime time.Time
	if len(rp.ShardGroups) == 0 {
		startTime = time.Now()
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
				ID:     nextShardID(),
				Owners: owners,
			},
		},
	}
	rp.ShardGroups = append(rp.ShardGroups, sh)
}

func nextShardID() uint64 {
	return atomic.AddUint64(&shardID, 1)
}

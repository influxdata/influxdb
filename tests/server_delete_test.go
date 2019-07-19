package tests

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/models"
)

var db = "db0"
var rp = "rp0"

// Makes it easy to debug times by emitting rfc formatted strings instead
// of numbers.
var tme = func(t int64) string {
	// return time.Unix(0, t).UTC().String() + fmt.Sprintf("(%d)", t)
	return fmt.Sprint(t) // Just emit the number
}

type Command func() (string, error)

func setupCommands(s *LocalServer, tracker *SeriesTracker) []Command {
	var measurementN = 10
	var seriesN = 100
	var commands []Command

	r := rand.New(rand.NewSource(seed))

	// Command for inserting some series data.
	commands = append(commands, func() (string, error) {
		name := fmt.Sprintf("m%d", r.Intn(measurementN))
		tags := models.NewTags(map[string]string{fmt.Sprintf("s%d", r.Intn(seriesN)): "a"})

		tracker.Lock()
		pt := tracker.AddSeries(name, tags)
		_, err := s.Write(db, rp, pt, nil)
		if err != nil {
			return "", err
		}

		defer tracker.Unlock()
		return fmt.Sprintf("INSERT %s", pt), tracker.Verify()
	})

	// Command for dropping an entire measurement.
	commands = append(commands, func() (string, error) {
		name := fmt.Sprintf("m%d", r.Intn(measurementN))

		tracker.Lock()
		tracker.DeleteMeasurement(name)
		query := fmt.Sprintf("DROP MEASUREMENT %s", name)
		_, err := s.QueryWithParams(query, url.Values{"db": []string{"db0"}})
		if err != nil {
			return "", err
		}

		defer tracker.Unlock()
		return query, tracker.Verify()
	})

	// Command for dropping a single series.
	commands = append(commands, func() (string, error) {
		name := fmt.Sprintf("m%d", r.Intn(measurementN))
		tagKey := fmt.Sprintf("s%d", r.Intn(seriesN))
		tags := models.NewTags(map[string]string{tagKey: "a"})

		tracker.Lock()
		tracker.DropSeries(name, tags)
		query := fmt.Sprintf("DROP SERIES FROM %q WHERE %q = 'a'", name, tagKey)
		_, err := s.QueryWithParams(query, url.Values{"db": []string{"db0"}})
		if err != nil {
			return "", err
		}

		defer tracker.Unlock()
		return query, tracker.Verify()
	})

	// Command for dropping a single series.
	commands = append(commands, func() (string, error) {
		name := fmt.Sprintf("m%d", r.Intn(measurementN))

		tracker.Lock()
		min, max := tracker.DeleteRandomRange(name)
		query := fmt.Sprintf("DELETE FROM %q WHERE time >= %d AND time <= %d ", name, min, max)
		_, err := s.QueryWithParams(query, url.Values{"db": []string{"db0"}})
		if err != nil {
			return "", err
		}

		defer tracker.Unlock()
		query = fmt.Sprintf("DELETE FROM %q WHERE time >= %s AND time <= %s ", name, tme(min), tme(max))
		return query, tracker.Verify()
	})

	return commands
}

// TestServer_Delete_Series sets up a concurrent collection of clients that continuously
// write data and delete series, measurements and shards.
//
// The purpose of this test is to provide a randomised, highly concurrent test
// to shake out bugs involving the interactions between shards, their indexes,
// and a database's series file.
func TestServer_DELETE_DROP_SERIES_DROP_MEASUREMENT(t *testing.T) {
	t.Parallel()

	if testing.Short() || os.Getenv("GORACE") != "" {
		t.Skip("Skipping test in short or race mode.")
	}

	N := 5000
	shardN := 10
	r := rand.New(rand.NewSource(seed))
	t.Logf("***** Seed set to %d *****\n", seed)

	if testing.Short() {
		t.Skip("Skipping in short mode")
	}

	s := OpenDefaultServer(NewConfig())
	defer s.Close()

	if _, ok := s.(*RemoteServer); ok {
		t.Skip("Skipping. Not implemented on remote server")
	}

	localServer := s.(*LocalServer)

	// First initialize some writes such that we end up with 10 shards.
	// The first point refers to 1970-01-01 00:00:00.01 +0000 UTC and each subsequent
	// point is 7 days into the future.
	queries := make([]string, 0, N)
	data := make([]string, 0, shardN)
	for i := int64(0); i < int64(cap(data)); i++ {
		query := fmt.Sprintf(`a val=1 %d`, 10000000+(int64(time.Hour)*24*7*i))
		queries = append(queries, fmt.Sprintf("INSERT %s", query))
		data = append(data, query)
	}

	if _, err := s.Write(db, rp, strings.Join(data, "\n"), nil); err != nil {
		t.Fatal(err)
	}

	tracker := NewSeriesTracker(r, localServer, db, rp)
	commands := setupCommands(localServer, tracker)
	for i := 0; i < N; i++ {
		query, err := commands[r.Intn(len(commands))]()
		queries = append(queries, query)
		if err != nil {
			emit := queries
			if len(queries) > 1000 {
				emit = queries[len(queries)-1000:]
			}
			t.Logf("Emitting last 1000 queries of %d total:\n%s\n", len(queries), strings.Join(emit, "\n"))
			t.Logf("Current points in Series Tracker index:\n%s\n", tracker.DumpPoints())
			t.Fatal(err)
		}
	}
}

// **** The following tests are specific examples discovered using ****

// TestServer_DELETE_DROP_SERIES_DROP_MEASUREMENT. They're added to prevent
// regressions.

// This test never explicitly failed, but it's a special case of
// TestServer_Insert_Delete_1515688266259660938.
func TestServer_Insert_Delete_1515688266259660938_same_shard(t *testing.T) {
	// Original seed was 1515688266259660938.
	t.Parallel()

	if testing.Short() || os.Getenv("GORACE") != "" {
		t.Skip("Skipping test in short or race mode.")
	}

	s := OpenDefaultServer(NewConfig())
	defer s.Close()

	for i := 0; i < 100; i++ {
		mustWrite(s, "m4,s67=a v=1 4",
			"m4,s67=a v=1 12",
			"m4,s1=a v=1 15")

		mustDelete(s, "m4", 1, 10)

		// Compare series left in index.
		gotSeries := mustGetSeries(s)
		expectedSeries := []string{"m4,s1=a", "m4,s67=a"}
		if !reflect.DeepEqual(gotSeries, expectedSeries) {
			t.Fatalf("got series %v, expected %v", gotSeries, expectedSeries)
		}
		mustDropCreate(s)
	}
}

// This test failed with seed 1515688266259660938.
func TestServer_Insert_Delete_1515688266259660938(t *testing.T) {
	// Original seed was 1515688266259660938.
	t.Parallel()

	if testing.Short() || os.Getenv("GORACE") != "" {
		t.Skip("Skipping test in short or race mode.")
	}

	s := OpenDefaultServer(NewConfig())
	defer s.Close()

	for i := 0; i < 100; i++ {
		mustWrite(s, "m4,s67=a v=1 2127318532111304", // should be deleted
			"m4,s67=a v=1 4840422259072956",
			"m4,s1=a v=1 4777375719836601")

		mustDelete(s, "m4", 1134567692141289, 2233755799041351)

		// Compare series left in index.
		gotSeries := mustGetSeries(s)
		expectedSeries := []string{"m4,s1=a", "m4,s67=a"}
		if !reflect.DeepEqual(gotSeries, expectedSeries) {
			t.Fatalf("got series %v, expected %v", gotSeries, expectedSeries)
		}

		mustDropCreate(s)
	}
}

// This test failed with seed 1515771752164780713.
func TestServer_Insert_Delete_1515771752164780713(t *testing.T) {
	// Original seed was 1515771752164780713.
	t.Parallel()

	if testing.Short() || os.Getenv("GORACE") != "" {
		t.Skip("Skipping test in short or race mode.")
	}

	s := OpenDefaultServer(NewConfig())
	defer s.Close()

	mustWrite(s, "m6,s72=a v=1 641480139110750")            // series id 257 in shard 1
	mustWrite(s, "m6,s32=a v=1 1320128148356150")           // series id 259 in shard 2
	mustDelete(s, "m6", 1316178840387070, 3095172845699329) // deletes m6,s32=a (259) - shard 2 now empty
	mustWrite(s, "m6,s61=a v=1 47161015166211")             // series id 261 in shard 3
	mustWrite(s, "m6,s67=a v=1 4466443248294177")           // series 515 in shard 4
	mustDelete(s, "m6", 495574950798826, 2963503790804876)  // deletes m6,s72 (257) - shard 1 now empty

	// Compare series left in index.
	gotSeries := mustGetSeries(s)

	expectedSeries := []string{"m6,s61=a", "m6,s67=a"}
	if !reflect.DeepEqual(gotSeries, expectedSeries) {
		t.Fatalf("got series %v, expected %v", gotSeries, expectedSeries)
	}
}

// This test failed with seed 1515777603585914810.
func TestServer_Insert_Delete_1515777603585914810(t *testing.T) {
	// Original seed was 1515777603585914810.
	t.Parallel()

	s := OpenDefaultServer(NewConfig())
	defer s.Close()

	mustWrite(s, "m5,s99=a v=1 1")
	mustDelete(s, "m5", 0, 1)
	mustWrite(s, "m5,s99=a v=1 1")

	gotSeries := mustGetSeries(s)
	expectedSeries := []string{"m5,s99=a"}
	if !reflect.DeepEqual(gotSeries, expectedSeries) {
		t.Fatalf("got series %v, expected %v", gotSeries, expectedSeries)
	}
}

// This test reproduces the issue identified in https://github.com/influxdata/influxdb/issues/10052
func TestServer_Insert_Delete_10052(t *testing.T) {
	t.Parallel()

	s := OpenDefaultServer(NewConfig())
	defer s.Close()

	mustWrite(s,
		"ping,server=ping a=1,b=2,c=3,d=4,e=5 1",
		"ping,server=ping a=1,b=2,c=3,d=4,e=5 2",
		"ping,server=ping a=1,b=2,c=3,d=4,e=5 3",
		"ping,server=ping a=1,b=2,c=3,d=4,e=5 4",
		"ping,server=ping a=1,b=2,c=3,d=4,e=5 5",
		"ping,server=ping a=1,b=2,c=3,d=4,e=5 6",
	)

	mustDropMeasurement(s, "ping")
	gotSeries := mustGetSeries(s)
	expectedSeries := []string(nil)
	if !reflect.DeepEqual(gotSeries, expectedSeries) {
		t.Fatalf("got series %v, expected %v", gotSeries, expectedSeries)
	}

	mustWrite(s, "ping v=1 1")
	gotSeries = mustGetSeries(s)
	expectedSeries = []string{"ping"}
	if !reflect.DeepEqual(gotSeries, expectedSeries) {
		t.Fatalf("got series %v, expected %v", gotSeries, expectedSeries)
	}

	gotSeries = mustGetFieldKeys(s)
	expectedSeries = []string{"v"}
	if !reflect.DeepEqual(gotSeries, expectedSeries) {
		t.Fatalf("got series %v, expected %v", gotSeries, expectedSeries)
	}
}

func mustGetSeries(s Server) []string {
	result, err := s.QueryWithParams("SHOW SERIES", url.Values{"db": []string{"db0"}})
	if err != nil {
		panic(err)
	}

	gotSeries, err := valuesFromShowQuery(result)
	if err != nil {
		panic(err)
	}
	return gotSeries
}

func mustGetFieldKeys(s Server) []string {
	result, err := s.QueryWithParams("SHOW FIELD KEYS", url.Values{"db": []string{"db0"}})
	if err != nil {
		panic(err)
	}

	gotSeries, err := valuesFromShowQuery(result)
	if err != nil {
		panic(err)
	}
	return gotSeries
}

func mustDropCreate(s Server) {
	if err := s.DropDatabase(db); err != nil {
		panic(err)
	}

	if err := s.CreateDatabaseAndRetentionPolicy(db, NewRetentionPolicySpec(rp, 1, 0), true); err != nil {
		panic(err)
	}
}

func mustWrite(s Server, points ...string) {
	if _, err := s.Write(db, rp, strings.Join(points, "\n"), nil); err != nil {
		panic(err)
	}
}

func mustDelete(s Server, name string, min, max int64) {
	query := fmt.Sprintf("DELETE FROM %q WHERE time >= %d AND time <= %d ", name, min, max)
	if _, err := s.QueryWithParams(query, url.Values{"db": []string{db}}); err != nil {
		panic(err)
	}
}

func mustDropMeasurement(s Server, name string) {
	query := fmt.Sprintf("DROP MEASUREMENT %q", name)
	if _, err := s.QueryWithParams(query, url.Values{"db": []string{db}}); err != nil {
		panic(err)
	}
}

// SeriesTracker is a lockable tracker of which shards should own which series.
type SeriesTracker struct {
	sync.RWMutex
	r *rand.Rand

	// The testing server
	server *LocalServer

	// series maps a series key to a value that determines which shards own the
	// series.
	series map[string]uint64

	// seriesPoints maps a series key to all the times that the series is written.
	seriesPoints map[string][]int64

	// measurements maps a measurement name to a value that determines which
	// shards own the measurement.
	measurements map[string]uint64

	// measurementsSeries maps which series keys belong to which measurement.
	measurementsSeries map[string]map[string]struct{}

	// shardTimeRanges maintains the time ranges that a shard spans
	shardTimeRanges map[uint64][2]int64
	shardIDs        []uint64
}

func NewSeriesTracker(r *rand.Rand, server *LocalServer, db, rp string) *SeriesTracker {
	tracker := &SeriesTracker{
		r:                  r,
		series:             make(map[string]uint64),
		seriesPoints:       make(map[string][]int64),
		measurements:       make(map[string]uint64),
		measurementsSeries: make(map[string]map[string]struct{}),
		shardTimeRanges:    make(map[uint64][2]int64),
		server:             server,
	}

	data := server.MetaClient.Data()

	sgs, err := data.ShardGroups(db, rp)
	if err != nil {
		panic(err)
	}

	for _, sg := range sgs {
		tracker.shardTimeRanges[sg.ID] = [2]int64{sg.StartTime.UnixNano(), sg.EndTime.UnixNano()}
		tracker.shardIDs = append(tracker.shardIDs, sg.ID)
	}

	// Add initial series
	for i, sid := range tracker.shardIDs {
		// Map the shard to the series.
		tracker.series["a"] = tracker.series["a"] | (1 << sid)
		// Map the shard to the measurement.
		tracker.measurements["a"] = tracker.measurements["a"] | (1 << sid)
		// Map the timstamp of the point in this shard to the series.
		tracker.seriesPoints["a"] = append(tracker.seriesPoints["a"], 10000000+(int64(time.Hour)*24*7*int64(i)))
	}
	// Map initial series to measurement.
	tracker.measurementsSeries["a"] = map[string]struct{}{"a": struct{}{}}
	return tracker
}

// AddSeries writes a point for the provided series to the provided shard. The
// exact time of the point is randomised within all the shards' boundaries.
// The point string is returned, to be inserted into the server.
func (s *SeriesTracker) AddSeries(name string, tags models.Tags) string {
	// generate a random shard and time within it.
	time, shard := s.randomTime()

	pt, err := models.NewPoint(name, tags, models.Fields{"v": 1.0}, time)
	if err != nil {
		panic(err)
	}

	key := string(pt.Key())
	s.series[key] = s.series[key] | (1 << shard)
	s.seriesPoints[key] = append(s.seriesPoints[key], time.UnixNano())

	// Update measurement map
	s.measurements[name] = s.measurements[name] | (1 << shard)

	// Update measurement -> series mapping
	if _, ok := s.measurementsSeries[name]; !ok {
		s.measurementsSeries[name] = map[string]struct{}{string(key): struct{}{}}
	} else {
		s.measurementsSeries[name][string(key)] = struct{}{}
	}
	return pt.String()
}

// DeleteMeasurement deletes all series associated with the provided measurement
// from the tracker.
func (s *SeriesTracker) DeleteMeasurement(name string) {
	// Delete from shard -> measurement mapping
	delete(s.measurements, name)

	// Get any series associated with measurement, and delete them.
	series := s.measurementsSeries[name]

	// Remove all series associated with this measurement.
	for key := range series {
		delete(s.series, key)
		delete(s.seriesPoints, key)
	}
	delete(s.measurementsSeries, name)
}

// DropSeries deletes a specific series, removing any measurements if no series
// are owned by them any longer.
func (s *SeriesTracker) DropSeries(name string, tags models.Tags) {
	pt, err := models.NewPoint(name, tags, models.Fields{"v": 1.0}, time.Now())
	if err != nil {
		panic(err)
	}

	key := string(pt.Key())
	_, ok := s.series[key]
	if ok {
		s.cleanupSeries(name, key) // Remove all series data.
	}
}

// cleanupSeries removes any traces of series that no longer have any point
// data.
func (s *SeriesTracker) cleanupSeries(name, key string) {
	// Remove series references
	delete(s.series, key)
	delete(s.measurementsSeries[name], key)
	delete(s.seriesPoints, key)

	// Check if that was the last series for a measurement
	if len(s.measurementsSeries[name]) == 0 {
		delete(s.measurementsSeries, name) // Remove the measurement
		delete(s.measurements, name)
	}
}

// DeleteRandomRange deletes all series data within a random time range for the
// provided measurement.
func (s *SeriesTracker) DeleteRandomRange(name string) (int64, int64) {
	t1, _ := s.randomTime()
	t2, _ := s.randomTime()
	min, max := t1.UnixNano(), t2.UnixNano()
	if t2.Before(t1) {
		min, max = t2.UnixNano(), t1.UnixNano()
	}
	if min > max {
		panic(fmt.Sprintf("min time %d > max %d", min, max))
	}
	// Get all the series associated with this measurement.
	series := s.measurementsSeries[name]
	if len(series) == 0 {
		// Nothing to do
		return min, max
	}

	// For each series, check for, and remove, any points that fall within the
	// time range.
	var seriesToDelete []string
	for serie := range series {
		points := s.seriesPoints[serie]
		sort.Sort(sortedInt64(points))

		// Find min and max index that fall in range.
		var minIdx, maxIdx = -1, -1
		for i, p := range points {
			if minIdx == -1 && p >= min {
				minIdx = i
			}
			if p <= max {
				maxIdx = i
			}
		}

		// If either the minIdx or maxIdx are not set, then none of the points
		// fall in that boundary of the domain.
		if minIdx == -1 || maxIdx == -1 {
			continue
		}

		// Cut those points from the series points slice.
		s.seriesPoints[serie] = append(points[:minIdx], points[maxIdx+1:]...)

		// Was that the last point for the series?
		if len(s.seriesPoints[serie]) == 0 {
			seriesToDelete = append(seriesToDelete, serie)
		}
	}

	// Cleanup any removed series/measurements.
	for _, key := range seriesToDelete {
		s.cleanupSeries(name, key)
	}

	return min, max
}

// randomTime generates a random time to insert a point, such that it will fall
// within one of the shards' time boundaries.
func (s *SeriesTracker) randomTime() (time.Time, uint64) {
	// Pick a random shard
	id := s.shardIDs[s.r.Intn(len(s.shardIDs))]

	// Get min and max time range of the shard.
	min, max := s.shardTimeRanges[id][0], s.shardTimeRanges[id][1]
	if min >= max {
		panic(fmt.Sprintf("min %d >= max %d", min, max))
	}
	tme := s.r.Int63n(max-min) + min // Will result in a range [min, max)
	if tme < min || tme >= max {
		panic(fmt.Sprintf("generated time %d is out of bounds [%d, %d)", tme, min, max))
	}
	return time.Unix(0, tme), id
}

// Verify verifies that the server's view of the index/series file matches the
// series tracker's.
func (s *SeriesTracker) Verify() error {
	res, err := s.server.QueryWithParams("SHOW SERIES", url.Values{"db": []string{"db0"}})
	if err != nil {
		return err
	}

	// Get all series...
	gotSeries, err := valuesFromShowQuery(res)
	if err != nil {
		return err
	}

	expectedSeries := make([]string, 0, len(s.series))
	for series := range s.series {
		expectedSeries = append(expectedSeries, series)
	}
	sort.Strings(expectedSeries)

	if !reflect.DeepEqual(gotSeries, expectedSeries) {
		return fmt.Errorf("verification failed:\ngot series: %v\nexpected series: %v\ndifference: %s", gotSeries, expectedSeries, cmp.Diff(gotSeries, expectedSeries))
	}
	return nil
}

// valuesFromShowQuery extracts a lexicographically sorted set of series keys
// from a SHOW SERIES query.
func valuesFromShowQuery(result string) ([]string, error) {
	// Get all series...
	var results struct {
		Results []struct {
			Series []struct {
				Values [][]string `json:"values"`
			} `json:"series"`
		} `json:"results"`
	}

	if err := json.Unmarshal([]byte(result), &results); err != nil {
		return nil, err
	}

	var gotSeries []string
	for _, ser := range results.Results {
		for _, values := range ser.Series {
			for _, v := range values.Values {
				gotSeries = append(gotSeries, v[0])
			}
		}
	}
	// series are returned sorted by name then tag key then tag value, which is
	// not the same as lexicographic order.
	sort.Strings(gotSeries)

	return gotSeries, nil
}

// DumpPoints returns all the series points.
func (s *SeriesTracker) DumpPoints() string {
	keys := make([]string, 0, len(s.seriesPoints))
	for key := range s.seriesPoints {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for i, key := range keys {
		// We skip key "a" as it's just there to initialise the shards.
		if key == "a" {
			continue
		}

		points := s.seriesPoints[key]
		sort.Sort(sortedInt64(points))

		pointStr := make([]string, 0, len(points))
		for _, p := range points {
			pointStr = append(pointStr, tme(p))
		}
		keys[i] = fmt.Sprintf("%s: %v", key, pointStr)
	}
	return strings.Join(keys, "\n")
}

type sortedInt64 []int64

func (a sortedInt64) Len() int           { return len(a) }
func (a sortedInt64) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a sortedInt64) Less(i, j int) bool { return a[i] < a[j] }

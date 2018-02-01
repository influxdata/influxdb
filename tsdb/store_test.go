package tsdb_test

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb/tsdb/index/inmem"

	"github.com/davecgh/go-spew/spew"
	"github.com/influxdata/influxdb/internal"
	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/deep"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
)

// Ensure the store can delete a retention policy and all shards under
// it.
func TestStore_DeleteRetentionPolicy(t *testing.T) {
	t.Parallel()

	test := func(index string) {
		s := MustOpenStore(index)
		defer s.Close()

		// Create a new shard and verify that it exists.
		if err := s.CreateShard("db0", "rp0", 1, true); err != nil {
			t.Fatal(err)
		} else if sh := s.Shard(1); sh == nil {
			t.Fatalf("expected shard")
		}

		// Create a new shard under the same retention policy,  and verify
		// that it exists.
		if err := s.CreateShard("db0", "rp0", 2, true); err != nil {
			t.Fatal(err)
		} else if sh := s.Shard(2); sh == nil {
			t.Fatalf("expected shard")
		}

		// Create a new shard under a different retention policy, and
		// verify that it exists.
		if err := s.CreateShard("db0", "rp1", 3, true); err != nil {
			t.Fatal(err)
		} else if sh := s.Shard(3); sh == nil {
			t.Fatalf("expected shard")
		}

		// Deleting the rp0 retention policy does not return an error.
		if err := s.DeleteRetentionPolicy("db0", "rp0"); err != nil {
			t.Fatal(err)
		}

		// It deletes the shards under that retention policy.
		if sh := s.Shard(1); sh != nil {
			t.Errorf("shard 1 was not deleted")
		}

		if sh := s.Shard(2); sh != nil {
			t.Errorf("shard 2 was not deleted")
		}

		// It deletes the retention policy directory.
		if got, exp := dirExists(filepath.Join(s.Path(), "db0", "rp0")), false; got != exp {
			t.Error("directory exists, but should have been removed")
		}

		// It deletes the WAL retention policy directory.
		if got, exp := dirExists(filepath.Join(s.EngineOptions.Config.WALDir, "db0", "rp0")), false; got != exp {
			t.Error("directory exists, but should have been removed")
		}

		// Reopen other shard and check it still exists.
		if err := s.Reopen(); err != nil {
			t.Error(err)
		} else if sh := s.Shard(3); sh == nil {
			t.Errorf("shard 3 does not exist")
		}

		// It does not delete other retention policy directories.
		if got, exp := dirExists(filepath.Join(s.Path(), "db0", "rp1")), true; got != exp {
			t.Error("directory does not exist, but should")
		}
		if got, exp := dirExists(filepath.Join(s.EngineOptions.Config.WALDir, "db0", "rp1")), true; got != exp {
			t.Error("directory does not exist, but should")
		}
	}

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) { test(index) })
	}
}

// Ensure the store can create a new shard.
func TestStore_CreateShard(t *testing.T) {
	t.Parallel()

	test := func(index string) {
		s := MustOpenStore(index)
		defer s.Close()

		// Create a new shard and verify that it exists.
		if err := s.CreateShard("db0", "rp0", 1, true); err != nil {
			t.Fatal(err)
		} else if sh := s.Shard(1); sh == nil {
			t.Fatalf("expected shard")
		}

		// Create another shard and verify that it exists.
		if err := s.CreateShard("db0", "rp0", 2, true); err != nil {
			t.Fatal(err)
		} else if sh := s.Shard(2); sh == nil {
			t.Fatalf("expected shard")
		}

		// Reopen shard and recheck.
		if err := s.Reopen(); err != nil {
			t.Fatal(err)
		} else if sh := s.Shard(1); sh == nil {
			t.Fatalf("expected shard(1)")
		} else if sh = s.Shard(2); sh == nil {
			t.Fatalf("expected shard(2)")
		}
	}

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) { test(index) })
	}
}

// Ensure the store does not return an error when delete from a non-existent db.
func TestStore_DeleteSeries_NonExistentDB(t *testing.T) {
	t.Parallel()

	test := func(index string) {
		s := MustOpenStore(index)
		defer s.Close()

		if err := s.DeleteSeries("db0", nil, nil); err != nil {
			t.Fatal(err.Error())
		}
	}

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) { test(index) })
	}
}

// Ensure the store can delete an existing shard.
func TestStore_DeleteShard(t *testing.T) {
	t.Parallel()

	test := func(index string) error {
		s := MustOpenStore(index)
		defer s.Close()

		// Create a new shard and verify that it exists.
		if err := s.CreateShard("db0", "rp0", 1, true); err != nil {
			return err
		} else if sh := s.Shard(1); sh == nil {
			return fmt.Errorf("expected shard")
		}

		// Create another shard.
		if err := s.CreateShard("db0", "rp0", 2, true); err != nil {
			return err
		} else if sh := s.Shard(2); sh == nil {
			return fmt.Errorf("expected shard")
		}

		// and another, but in a different db.
		if err := s.CreateShard("db1", "rp0", 3, true); err != nil {
			return err
		} else if sh := s.Shard(3); sh == nil {
			return fmt.Errorf("expected shard")
		}

		// Write series data to the db0 shards.
		s.MustWriteToShardString(1, "cpu,servera=a v=1", "cpu,serverb=b v=1", "mem,serverc=a v=1")
		s.MustWriteToShardString(2, "cpu,servera=a v=1", "mem,serverc=a v=1")

		// Write similar data to db1 database
		s.MustWriteToShardString(3, "cpu,serverb=b v=1")

		// Reopen the store and check all shards still exist
		if err := s.Reopen(); err != nil {
			return err
		}
		for i := uint64(1); i <= 3; i++ {
			if sh := s.Shard(i); sh == nil {
				return fmt.Errorf("shard %d missing", i)
			}
		}

		// Remove the first shard from the store.
		if err := s.DeleteShard(1); err != nil {
			return err
		}

		// cpu,serverb=b should be removed from the series file for db0 because
		// shard 1 was the only owner of that series.
		// Verify by getting  all tag keys.
		keys, err := s.TagKeys(nil, []uint64{2}, nil)
		if err != nil {
			return err
		}

		expKeys := []tsdb.TagKeys{
			{Measurement: "cpu", Keys: []string{"servera"}},
			{Measurement: "mem", Keys: []string{"serverc"}},
		}
		if got, exp := keys, expKeys; !reflect.DeepEqual(got, exp) {
			return fmt.Errorf("got keys %v, expected %v", got, exp)
		}

		// Verify that the same series was not removed from other databases'
		// series files.
		if keys, err = s.TagKeys(nil, []uint64{3}, nil); err != nil {
			return err
		}

		expKeys = []tsdb.TagKeys{{Measurement: "cpu", Keys: []string{"serverb"}}}
		if got, exp := keys, expKeys; !reflect.DeepEqual(got, exp) {
			return fmt.Errorf("got keys %v, expected %v", got, exp)
		}
		return nil
	}

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {
			if err := test(index); err != nil {
				t.Error(err)
			}
		})
	}
}

// Ensure the store can create a snapshot to a shard.
func TestStore_CreateShardSnapShot(t *testing.T) {
	t.Parallel()

	test := func(index string) {
		s := MustOpenStore(index)
		defer s.Close()

		// Create a new shard and verify that it exists.
		if err := s.CreateShard("db0", "rp0", 1, true); err != nil {
			t.Fatal(err)
		} else if sh := s.Shard(1); sh == nil {
			t.Fatalf("expected shard")
		}

		dir, e := s.CreateShardSnapshot(1)
		if e != nil {
			t.Fatal(e)
		}
		if dir == "" {
			t.Fatal("empty directory name")
		}
	}

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) { test(index) })
	}
}

func TestStore_Open(t *testing.T) {
	t.Parallel()

	test := func(index string) {
		s := NewStore(index)
		defer s.Close()

		if err := os.MkdirAll(filepath.Join(s.Path(), "db0", "rp0", "2"), 0777); err != nil {
			t.Fatal(err)
		}

		if err := os.MkdirAll(filepath.Join(s.Path(), "db0", "rp2", "4"), 0777); err != nil {
			t.Fatal(err)
		}

		if err := os.MkdirAll(filepath.Join(s.Path(), "db1", "rp0", "1"), 0777); err != nil {
			t.Fatal(err)
		}

		// Store should ignore shard since it does not have a numeric name.
		if err := s.Open(); err != nil {
			t.Fatal(err)
		} else if n := len(s.Databases()); n != 2 {
			t.Fatalf("unexpected database index count: %d", n)
		} else if n := s.ShardN(); n != 3 {
			t.Fatalf("unexpected shard count: %d", n)
		}

		expDatabases := []string{"db0", "db1"}
		gotDatabases := s.Databases()
		sort.Strings(gotDatabases)

		if got, exp := gotDatabases, expDatabases; !reflect.DeepEqual(got, exp) {
			t.Fatalf("got %#v, expected %#v", got, exp)
		}
	}

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) { test(index) })
	}
}

// Ensure the store reports an error when it can't open a database directory.
func TestStore_Open_InvalidDatabaseFile(t *testing.T) {
	t.Parallel()

	test := func(index string) {
		s := NewStore(index)
		defer s.Close()

		// Create a file instead of a directory for a database.
		if _, err := os.Create(filepath.Join(s.Path(), "db0")); err != nil {
			t.Fatal(err)
		}

		// Store should ignore database since it's a file.
		if err := s.Open(); err != nil {
			t.Fatal(err)
		} else if n := len(s.Databases()); n != 0 {
			t.Fatalf("unexpected database index count: %d", n)
		}
	}

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) { test(index) })
	}
}

// Ensure the store reports an error when it can't open a retention policy.
func TestStore_Open_InvalidRetentionPolicy(t *testing.T) {
	t.Parallel()

	test := func(index string) {
		s := NewStore(index)
		defer s.Close()

		// Create an RP file instead of a directory.
		if err := os.MkdirAll(filepath.Join(s.Path(), "db0"), 0777); err != nil {
			t.Fatal(err)
		} else if _, err := os.Create(filepath.Join(s.Path(), "db0", "rp0")); err != nil {
			t.Fatal(err)
		}

		// Store should ignore retention policy since it's a file, and there should
		// be no indices created.
		if err := s.Open(); err != nil {
			t.Fatal(err)
		} else if n := len(s.Databases()); n != 0 {
			t.Log(s.Databases())
			t.Fatalf("unexpected database index count: %d", n)
		}
	}

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) { test(index) })
	}
}

// Ensure the store reports an error when it can't open a retention policy.
func TestStore_Open_InvalidShard(t *testing.T) {
	t.Parallel()

	test := func(index string) {
		s := NewStore(index)
		defer s.Close()

		// Create a non-numeric shard file.
		if err := os.MkdirAll(filepath.Join(s.Path(), "db0", "rp0"), 0777); err != nil {
			t.Fatal(err)
		} else if _, err := os.Create(filepath.Join(s.Path(), "db0", "rp0", "bad_shard")); err != nil {
			t.Fatal(err)
		}

		// Store should ignore shard since it does not have a numeric name.
		if err := s.Open(); err != nil {
			t.Fatal(err)
		} else if n := len(s.Databases()); n != 0 {
			t.Fatalf("unexpected database index count: %d", n)
		} else if n := s.ShardN(); n != 0 {
			t.Fatalf("unexpected shard count: %d", n)
		}
	}

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) { test(index) })
	}
}

// Ensure shards can create iterators.
func TestShards_CreateIterator(t *testing.T) {
	t.Parallel()

	test := func(index string) {
		s := MustOpenStore(index)
		defer s.Close()

		// Create shard #0 with data.
		s.MustCreateShardWithData("db0", "rp0", 0,
			`cpu,host=serverA value=1  0`,
			`cpu,host=serverA value=2 10`,
			`cpu,host=serverB value=3 20`,
		)

		// Create shard #1 with data.
		s.MustCreateShardWithData("db0", "rp0", 1,
			`cpu,host=serverA value=1 30`,
			`mem,host=serverA value=2 40`, // skip: wrong source
			`cpu,host=serverC value=3 60`,
		)

		// Retrieve shard group.
		shards := s.ShardGroup([]uint64{0, 1})

		// Create iterator.
		m := &influxql.Measurement{Name: "cpu"}
		itr, err := shards.CreateIterator(context.Background(), m, query.IteratorOptions{
			Expr:       influxql.MustParseExpr(`value`),
			Dimensions: []string{"host"},
			Ascending:  true,
			StartTime:  influxql.MinTime,
			EndTime:    influxql.MaxTime,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer itr.Close()
		fitr := itr.(query.FloatIterator)

		// Read values from iterator. The host=serverA points should come first.
		if p, err := fitr.Next(); err != nil {
			t.Fatalf("unexpected error(0): %s", err)
		} else if !deep.Equal(p, &query.FloatPoint{Name: "cpu", Tags: ParseTags("host=serverA"), Time: time.Unix(0, 0).UnixNano(), Value: 1}) {
			t.Fatalf("unexpected point(0): %s", spew.Sdump(p))
		}
		if p, err := fitr.Next(); err != nil {
			t.Fatalf("unexpected error(1): %s", err)
		} else if !deep.Equal(p, &query.FloatPoint{Name: "cpu", Tags: ParseTags("host=serverA"), Time: time.Unix(10, 0).UnixNano(), Value: 2}) {
			t.Fatalf("unexpected point(1): %s", spew.Sdump(p))
		}
		if p, err := fitr.Next(); err != nil {
			t.Fatalf("unexpected error(2): %s", err)
		} else if !deep.Equal(p, &query.FloatPoint{Name: "cpu", Tags: ParseTags("host=serverA"), Time: time.Unix(30, 0).UnixNano(), Value: 1}) {
			t.Fatalf("unexpected point(2): %s", spew.Sdump(p))
		}

		// Next the host=serverB point.
		if p, err := fitr.Next(); err != nil {
			t.Fatalf("unexpected error(3): %s", err)
		} else if !deep.Equal(p, &query.FloatPoint{Name: "cpu", Tags: ParseTags("host=serverB"), Time: time.Unix(20, 0).UnixNano(), Value: 3}) {
			t.Fatalf("unexpected point(3): %s", spew.Sdump(p))
		}

		// And finally the host=serverC point.
		if p, err := fitr.Next(); err != nil {
			t.Fatalf("unexpected error(4): %s", err)
		} else if !deep.Equal(p, &query.FloatPoint{Name: "cpu", Tags: ParseTags("host=serverC"), Time: time.Unix(60, 0).UnixNano(), Value: 3}) {
			t.Fatalf("unexpected point(4): %s", spew.Sdump(p))
		}

		// Then an EOF should occur.
		if p, err := fitr.Next(); err != nil {
			t.Fatalf("expected eof, got error: %s", err)
		} else if p != nil {
			t.Fatalf("expected eof, got: %s", spew.Sdump(p))
		}
	}

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) { test(index) })
	}
}

// Ensure the store can backup a shard and another store can restore it.
func TestStore_BackupRestoreShard(t *testing.T) {
	test := func(index string) {
		s0, s1 := MustOpenStore(index), MustOpenStore(index)
		defer s0.Close()
		defer s1.Close()

		// Create shard with data.
		s0.MustCreateShardWithData("db0", "rp0", 100,
			`cpu value=1 0`,
			`cpu value=2 10`,
			`cpu value=3 20`,
		)

		if err := s0.Reopen(); err != nil {
			t.Fatal(err)
		}

		// Backup shard to a buffer.
		var buf bytes.Buffer
		if err := s0.BackupShard(100, time.Time{}, &buf); err != nil {
			t.Fatal(err)
		}

		// Create the shard on the other store and restore from buffer.
		if err := s1.CreateShard("db0", "rp0", 100, true); err != nil {
			t.Fatal(err)
		}
		if err := s1.RestoreShard(100, &buf); err != nil {
			t.Fatal(err)
		}

		// Read data from
		m := &influxql.Measurement{Name: "cpu"}
		itr, err := s0.Shard(100).CreateIterator(context.Background(), m, query.IteratorOptions{
			Expr:      influxql.MustParseExpr(`value`),
			Ascending: true,
			StartTime: influxql.MinTime,
			EndTime:   influxql.MaxTime,
		})
		if err != nil {
			t.Fatal(err)
		}
		fitr := itr.(query.FloatIterator)

		// Read values from iterator. The host=serverA points should come first.
		p, e := fitr.Next()
		if e != nil {
			t.Fatal(e)
		}
		if !deep.Equal(p, &query.FloatPoint{Name: "cpu", Time: time.Unix(0, 0).UnixNano(), Value: 1}) {
			t.Fatalf("unexpected point(0): %s", spew.Sdump(p))
		}
		p, e = fitr.Next()
		if e != nil {
			t.Fatal(e)
		}
		if !deep.Equal(p, &query.FloatPoint{Name: "cpu", Time: time.Unix(10, 0).UnixNano(), Value: 2}) {
			t.Fatalf("unexpected point(1): %s", spew.Sdump(p))
		}
		p, e = fitr.Next()
		if e != nil {
			t.Fatal(e)
		}
		if !deep.Equal(p, &query.FloatPoint{Name: "cpu", Time: time.Unix(20, 0).UnixNano(), Value: 3}) {
			t.Fatalf("unexpected point(2): %s", spew.Sdump(p))
		}
	}

	for _, index := range tsdb.RegisteredIndexes() {
		if index == "tsi1" {
			t.Skip("Skipping failing test for tsi1")
		}

		t.Run(index, func(t *testing.T) {
			test(index)
		})
	}
}
func TestStore_Shard_SeriesN(t *testing.T) {
	t.Parallel()

	test := func(index string) error {
		s := MustOpenStore(index)
		defer s.Close()

		// Create shard with data.
		s.MustCreateShardWithData("db0", "rp0", 1,
			`cpu value=1 0`,
			`cpu,host=serverA value=2 10`,
		)

		// Create 2nd shard w/ same measurements.
		s.MustCreateShardWithData("db0", "rp0", 2,
			`cpu value=1 0`,
			`cpu value=2 10`,
		)

		if got, exp := s.Shard(1).SeriesN(), int64(2); got != exp {
			return fmt.Errorf("[shard %d] got series count of %d, but expected %d", 1, got, exp)
		} else if got, exp := s.Shard(2).SeriesN(), int64(1); got != exp {
			return fmt.Errorf("[shard %d] got series count of %d, but expected %d", 2, got, exp)
		}
		return nil
	}

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {
			if err := test(index); err != nil {
				t.Error(err)
			}
		})
	}
}

func TestStore_MeasurementNames_Deduplicate(t *testing.T) {
	t.Parallel()

	test := func(index string) {
		s := MustOpenStore(index)
		defer s.Close()

		// Create shard with data.
		s.MustCreateShardWithData("db0", "rp0", 1,
			`cpu value=1 0`,
			`cpu value=2 10`,
			`cpu value=3 20`,
		)

		// Create 2nd shard w/ same measurements.
		s.MustCreateShardWithData("db0", "rp0", 2,
			`cpu value=1 0`,
			`cpu value=2 10`,
			`cpu value=3 20`,
		)

		meas, err := s.MeasurementNames(query.OpenAuthorizer, "db0", nil)
		if err != nil {
			t.Fatalf("unexpected error with MeasurementNames: %v", err)
		}

		if exp, got := 1, len(meas); exp != got {
			t.Fatalf("measurement len mismatch: exp %v, got %v", exp, got)
		}

		if exp, got := "cpu", string(meas[0]); exp != got {
			t.Fatalf("measurement name mismatch: exp %v, got %v", exp, got)
		}
	}

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) { test(index) })
	}
}

func testStoreCardinalityTombstoning(t *testing.T, store *Store) {
	// Generate point data to write to the shards.
	series := genTestSeries(10, 2, 4) // 160 series

	points := make([]models.Point, 0, len(series))
	for _, s := range series {
		points = append(points, models.MustNewPoint(s.Measurement, s.Tags, map[string]interface{}{"value": 1.0}, time.Now()))
	}

	// Create requested number of shards in the store & write points across
	// shards such that we never write the same series to multiple shards.
	for shardID := 0; shardID < 4; shardID++ {
		if err := store.CreateShard("db", "rp", uint64(shardID), true); err != nil {
			t.Errorf("create shard: %s", err)
		}

		if err := store.BatchWrite(shardID, points[shardID*40:(shardID+1)*40]); err != nil {
			t.Errorf("batch write: %s", err)
		}
	}

	// Delete all the series for each measurement.
	mnames, err := store.MeasurementNames(nil, "db", nil)
	if err != nil {
		t.Fatal(err)
	}

	for _, name := range mnames {
		if err := store.DeleteSeries("db", []influxql.Source{&influxql.Measurement{Name: string(name)}}, nil); err != nil {
			t.Fatal(err)
		}
	}

	// Estimate the series cardinality...
	cardinality, err := store.Store.SeriesCardinality("db")
	if err != nil {
		t.Fatal(err)
	}

	// Estimated cardinality should be well within 10 of the actual cardinality.
	if got, exp := int(cardinality), 10; got > exp {
		t.Errorf("series cardinality was %v (expected within %v), expected was: %d", got, exp, 0)
	}

	// Since all the series have been deleted, all the measurements should have
	// been removed from the index too.
	if cardinality, err = store.Store.MeasurementsCardinality("db"); err != nil {
		t.Fatal(err)
	}

	// Estimated cardinality should be well within 2 of the actual cardinality.
	// TODO(edd): this is totally arbitrary. How can I make it better?
	if got, exp := int(cardinality), 2; got > exp {
		t.Errorf("measurement cardinality was %v (expected within %v), expected was: %d", got, exp, 0)
	}
}

func TestStore_Cardinality_Tombstoning(t *testing.T) {
	t.Parallel()

	if testing.Short() || os.Getenv("GORACE") != "" || os.Getenv("APPVEYOR") != "" {
		t.Skip("Skipping test in short, race and appveyor mode.")
	}

	test := func(index string) {
		store := NewStore(index)
		if err := store.Open(); err != nil {
			panic(err)
		}
		defer store.Close()
		testStoreCardinalityTombstoning(t, store)
	}

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) { test(index) })
	}
}

func testStoreCardinalityUnique(t *testing.T, store *Store) {
	// Generate point data to write to the shards.
	series := genTestSeries(64, 5, 5) // 200,000 series
	expCardinality := len(series)

	points := make([]models.Point, 0, len(series))
	for _, s := range series {
		points = append(points, models.MustNewPoint(s.Measurement, s.Tags, map[string]interface{}{"value": 1.0}, time.Now()))
	}

	// Create requested number of shards in the store & write points across
	// shards such that we never write the same series to multiple shards.
	for shardID := 0; shardID < 10; shardID++ {
		if err := store.CreateShard("db", "rp", uint64(shardID), true); err != nil {
			t.Fatalf("create shard: %s", err)
		}
		if err := store.BatchWrite(shardID, points[shardID*20000:(shardID+1)*20000]); err != nil {
			t.Fatalf("batch write: %s", err)
		}
	}

	// Estimate the series cardinality...
	cardinality, err := store.Store.SeriesCardinality("db")
	if err != nil {
		t.Fatal(err)
	}

	// Estimated cardinality should be well within 1.5% of the actual cardinality.
	if got, exp := math.Abs(float64(cardinality)-float64(expCardinality))/float64(expCardinality), 0.015; got > exp {
		t.Errorf("got epsilon of %v for series cardinality %v (expected %v), which is larger than expected %v", got, cardinality, expCardinality, exp)
	}

	// Estimate the measurement cardinality...
	if cardinality, err = store.Store.MeasurementsCardinality("db"); err != nil {
		t.Fatal(err)
	}

	// Estimated cardinality should be well within 2 of the actual cardinality. (arbitrary...)
	expCardinality = 64
	if got, exp := math.Abs(float64(cardinality)-float64(expCardinality)), 2.0; got > exp {
		t.Errorf("got measurmement cardinality %v, expected upto %v; difference is larger than expected %v", cardinality, expCardinality, exp)
	}
}

func TestStore_Cardinality_Unique(t *testing.T) {
	t.Parallel()

	if testing.Short() || os.Getenv("GORACE") != "" || os.Getenv("APPVEYOR") != "" {
		t.Skip("Skipping test in short, race and appveyor mode.")
	}

	test := func(index string) {
		store := NewStore(index)
		store.EngineOptions.Config.MaxSeriesPerDatabase = 0
		if err := store.Open(); err != nil {
			panic(err)
		}
		defer store.Close()
		testStoreCardinalityUnique(t, store)
	}

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) { test(index) })
	}
}

// This test tests cardinality estimation when series data is duplicated across
// multiple shards.
func testStoreCardinalityDuplicates(t *testing.T, store *Store) {
	// Generate point data to write to the shards.
	series := genTestSeries(64, 5, 5) // 200,000 series.
	expCardinality := len(series)

	points := make([]models.Point, 0, len(series))
	for _, s := range series {
		points = append(points, models.MustNewPoint(s.Measurement, s.Tags, map[string]interface{}{"value": 1.0}, time.Now()))
	}

	// Create requested number of shards in the store & write points.
	for shardID := 0; shardID < 10; shardID++ {
		if err := store.CreateShard("db", "rp", uint64(shardID), true); err != nil {
			t.Fatalf("create shard: %s", err)
		}

		var from, to int
		if shardID == 0 {
			// if it's the first shard then write all of the points.
			from, to = 0, len(points)-1
		} else {
			// For other shards we write a random sub-section of all the points.
			// which will duplicate the series and shouldn't increase the
			// cardinality.
			from, to := rand.Intn(len(points)), rand.Intn(len(points))
			if from > to {
				from, to = to, from
			}
		}

		if err := store.BatchWrite(shardID, points[from:to]); err != nil {
			t.Fatalf("batch write: %s", err)
		}
	}

	// Estimate the series cardinality...
	cardinality, err := store.Store.SeriesCardinality("db")
	if err != nil {
		t.Fatal(err)
	}

	// Estimated cardinality should be well within 1.5% of the actual cardinality.
	if got, exp := math.Abs(float64(cardinality)-float64(expCardinality))/float64(expCardinality), 0.015; got > exp {
		t.Errorf("got epsilon of %v for series cardinality %d (expected %d), which is larger than expected %v", got, cardinality, expCardinality, exp)
	}

	// Estimate the measurement cardinality...
	if cardinality, err = store.Store.MeasurementsCardinality("db"); err != nil {
		t.Fatal(err)
	}

	// Estimated cardinality should be well within 2 of the actual cardinality. (Arbitrary...)
	expCardinality = 64
	if got, exp := math.Abs(float64(cardinality)-float64(expCardinality)), 2.0; got > exp {
		t.Errorf("got measurement cardinality %v, expected upto %v; difference is larger than expected %v", cardinality, expCardinality, exp)
	}
}

func TestStore_Cardinality_Duplicates(t *testing.T) {
	t.Parallel()

	if testing.Short() || os.Getenv("GORACE") != "" || os.Getenv("APPVEYOR") != "" {
		t.Skip("Skipping test in short, race and appveyor mode.")
	}

	test := func(index string) {
		store := NewStore(index)
		store.EngineOptions.Config.MaxSeriesPerDatabase = 0
		if err := store.Open(); err != nil {
			panic(err)
		}
		defer store.Close()
		testStoreCardinalityDuplicates(t, store)
	}

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) { test(index) })
	}
}

// Creates a large number of series in multiple shards, which will force
// compactions to occur.
func testStoreCardinalityCompactions(store *Store) error {

	// Generate point data to write to the shards.
	series := genTestSeries(300, 5, 5) // 937,500 series
	expCardinality := len(series)

	points := make([]models.Point, 0, len(series))
	for _, s := range series {
		points = append(points, models.MustNewPoint(s.Measurement, s.Tags, map[string]interface{}{"value": 1.0}, time.Now()))
	}

	// Create requested number of shards in the store & write points across
	// shards such that we never write the same series to multiple shards.
	for shardID := 0; shardID < 2; shardID++ {
		if err := store.CreateShard("db", "rp", uint64(shardID), true); err != nil {
			return fmt.Errorf("create shard: %s", err)
		}
		if err := store.BatchWrite(shardID, points[shardID*468750:(shardID+1)*468750]); err != nil {
			return fmt.Errorf("batch write: %s", err)
		}
	}

	// Estimate the series cardinality...
	cardinality, err := store.Store.SeriesCardinality("db")
	if err != nil {
		return err
	}

	// Estimated cardinality should be well within 1.5% of the actual cardinality.
	if got, exp := math.Abs(float64(cardinality)-float64(expCardinality))/float64(expCardinality), 0.015; got > exp {
		return fmt.Errorf("got epsilon of %v for series cardinality %v (expected %v), which is larger than expected %v", got, cardinality, expCardinality, exp)
	}

	// Estimate the measurement cardinality...
	if cardinality, err = store.Store.MeasurementsCardinality("db"); err != nil {
		return err
	}

	// Estimated cardinality should be well within 2 of the actual cardinality. (Arbitrary...)
	expCardinality = 300
	if got, exp := math.Abs(float64(cardinality)-float64(expCardinality)), 2.0; got > exp {
		return fmt.Errorf("got measurement cardinality %v, expected upto %v; difference is larger than expected %v", cardinality, expCardinality, exp)
	}
	return nil
}

func TestStore_Cardinality_Compactions(t *testing.T) {
	if testing.Short() || os.Getenv("GORACE") != "" || os.Getenv("APPVEYOR") != "" {
		t.Skip("Skipping test in short, race and appveyor mode.")
	}

	test := func(index string) error {
		store := NewStore(index)
		store.EngineOptions.Config.MaxSeriesPerDatabase = 0
		if err := store.Open(); err != nil {
			panic(err)
		}
		defer store.Close()
		return testStoreCardinalityCompactions(store)
	}

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {
			if err := test(index); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStore_Sketches(t *testing.T) {
	t.Parallel()

	checkCardinalities := func(store *tsdb.Store, series, tseries, measurements, tmeasurements int) error {
		// Get sketches and check cardinality...
		sketch, tsketch, err := store.SeriesSketches("db")
		if err != nil {
			return err
		}

		// delta calculates a rough 10% delta. If i is small then a minimum value
		// of 2 is used.
		delta := func(i int) int {
			v := i / 10
			if v == 0 {
				v = 2
			}
			return v
		}

		// series cardinality should be well within 10%.
		if got, exp := int(sketch.Count()), series; got-exp < -delta(series) || got-exp > delta(series) {
			return fmt.Errorf("got series cardinality %d, expected ~%d", got, exp)
		}

		// check series tombstones
		if got, exp := int(tsketch.Count()), tseries; got-exp < -delta(tseries) || got-exp > delta(tseries) {
			return fmt.Errorf("got series tombstone cardinality %d, expected ~%d", got, exp)
		}

		// Check measurement cardinality.
		if sketch, tsketch, err = store.MeasurementsSketches("db"); err != nil {
			return err
		}

		if got, exp := int(sketch.Count()), measurements; got-exp < -delta(measurements) || got-exp > delta(measurements) {
			return fmt.Errorf("got measurement cardinality %d, expected ~%d", got, exp)
		}

		if got, exp := int(tsketch.Count()), tmeasurements; got-exp < -delta(tmeasurements) || got-exp > delta(tmeasurements) {
			return fmt.Errorf("got measurement tombstone cardinality %d, expected ~%d", got, exp)
		}
		return nil
	}

	test := func(index string) error {
		store := MustOpenStore(index)
		defer store.Close()

		// Generate point data to write to the shards.
		series := genTestSeries(10, 2, 4) // 160 series

		points := make([]models.Point, 0, len(series))
		for _, s := range series {
			points = append(points, models.MustNewPoint(s.Measurement, s.Tags, map[string]interface{}{"value": 1.0}, time.Now()))
		}

		// Create requested number of shards in the store & write points across
		// shards such that we never write the same series to multiple shards.
		for shardID := 0; shardID < 4; shardID++ {
			if err := store.CreateShard("db", "rp", uint64(shardID), true); err != nil {
				return fmt.Errorf("create shard: %s", err)
			}

			if err := store.BatchWrite(shardID, points[shardID*40:(shardID+1)*40]); err != nil {
				return fmt.Errorf("batch write: %s", err)
			}
		}

		// Check cardinalities
		if err := checkCardinalities(store.Store, 160, 0, 10, 0); err != nil {
			return fmt.Errorf("[initial] %v", err)
		}

		// Reopen the store.
		if err := store.Reopen(); err != nil {
			return err
		}

		// Check cardinalities
		if err := checkCardinalities(store.Store, 160, 0, 10, 0); err != nil {
			return fmt.Errorf("[initial|re-open] %v", err)
		}

		// Delete half the the measurements data
		mnames, err := store.MeasurementNames(nil, "db", nil)
		if err != nil {
			return err
		}

		for _, name := range mnames[:len(mnames)/2] {
			if err := store.DeleteSeries("db", []influxql.Source{&influxql.Measurement{Name: string(name)}}, nil); err != nil {
				return err
			}
		}

		// Check cardinalities - tombstones should be in
		if err := checkCardinalities(store.Store, 160, 80, 10, 5); err != nil {
			return fmt.Errorf("[initial|re-open|delete] %v", err)
		}

		// Reopen the store.
		if err := store.Reopen(); err != nil {
			return err
		}

		// Check cardinalities. In this case, the indexes behave differently.
		//
		// - The inmem index will report that there are 80 series and no tombstones.
		// - The tsi1 index will report that there are 160 series and 80 tombstones.
		//
		// The result is the same, but the implementation differs.
		expS, expTS, expM, expTM := 160, 80, 10, 5
		if index == inmem.IndexName {
			expS, expTS, expM, expTM = 80, 0, 5, 0
		}

		if err := checkCardinalities(store.Store, expS, expTS, expM, expTM); err != nil {
			return fmt.Errorf("[initial|re-open|delete|re-open] %v", err)
		}
		return nil
	}

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {
			if err := test(index); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStore_TagValues(t *testing.T) {
	t.Parallel()

	// No WHERE - just get for keys host and shard
	RHSAll := &influxql.ParenExpr{
		Expr: &influxql.BinaryExpr{
			Op: influxql.OR,
			LHS: &influxql.BinaryExpr{
				Op:  influxql.EQ,
				LHS: &influxql.VarRef{Val: "_tagKey"},
				RHS: &influxql.StringLiteral{Val: "host"},
			},
			RHS: &influxql.BinaryExpr{
				Op:  influxql.EQ,
				LHS: &influxql.VarRef{Val: "_tagKey"},
				RHS: &influxql.StringLiteral{Val: "shard"},
			},
		},
	}

	// Get for host and shard, but also WHERE on foo = a
	RHSWhere := &influxql.ParenExpr{
		Expr: &influxql.BinaryExpr{
			Op: influxql.AND,
			LHS: &influxql.ParenExpr{
				Expr: &influxql.BinaryExpr{
					Op:  influxql.EQ,
					LHS: &influxql.VarRef{Val: "foo"},
					RHS: &influxql.StringLiteral{Val: "a"},
				},
			},
			RHS: RHSAll,
		},
	}

	// SHOW TAG VALUES FROM /cpu\d/ WITH KEY IN ("host", "shard")
	//
	// Switching out RHS for RHSWhere would make the query:
	//    SHOW TAG VALUES FROM /cpu\d/ WITH KEY IN ("host", "shard") WHERE foo = 'a'
	base := influxql.BinaryExpr{
		Op: influxql.AND,
		LHS: &influxql.ParenExpr{
			Expr: &influxql.BinaryExpr{
				Op:  influxql.EQREGEX,
				LHS: &influxql.VarRef{Val: "_name"},
				RHS: &influxql.RegexLiteral{Val: regexp.MustCompile(`cpu\d`)},
			},
		},
		RHS: RHSAll,
	}

	var baseWhere *influxql.BinaryExpr = influxql.CloneExpr(&base).(*influxql.BinaryExpr)
	baseWhere.RHS = RHSWhere

	examples := []struct {
		Name string
		Expr influxql.Expr
		Exp  []tsdb.TagValues
	}{
		{
			Name: "No WHERE clause",
			Expr: &base,
			Exp: []tsdb.TagValues{
				createTagValues("cpu0", map[string][]string{"host": {"nofoo", "tv0", "tv1", "tv2", "tv3"}, "shard": {"s0", "s1", "s2"}}),
				createTagValues("cpu1", map[string][]string{"host": {"nofoo", "tv0", "tv1", "tv2", "tv3"}, "shard": {"s0", "s1", "s2"}}),
				createTagValues("cpu2", map[string][]string{"host": {"nofoo", "tv0", "tv1", "tv2", "tv3"}, "shard": {"s0", "s1", "s2"}}),
			},
		},
		{
			Name: "With WHERE clause",
			Expr: baseWhere,
			Exp: []tsdb.TagValues{
				createTagValues("cpu0", map[string][]string{"host": {"tv0", "tv1", "tv2", "tv3"}, "shard": {"s0", "s1", "s2"}}),
				createTagValues("cpu1", map[string][]string{"host": {"tv0", "tv1", "tv2", "tv3"}, "shard": {"s0", "s1", "s2"}}),
				createTagValues("cpu2", map[string][]string{"host": {"tv0", "tv1", "tv2", "tv3"}, "shard": {"s0", "s1", "s2"}}),
			},
		},
	}

	var s *Store
	setup := func(index string) []uint64 { // returns shard ids
		s = MustOpenStore(index)

		fmtStr := `cpu%[1]d,foo=a,ignoreme=nope,host=tv%[2]d,shard=s%[3]d value=1 %[4]d
		cpu%[1]d,host=nofoo value=1 %[4]d
	mem,host=nothanks value=1 %[4]d
	`
		genPoints := func(sid int) []string {
			var ts int
			points := make([]string, 0, 3*4)
			for m := 0; m < 3; m++ {
				for tagvid := 0; tagvid < 4; tagvid++ {
					points = append(points, fmt.Sprintf(fmtStr, m, tagvid, sid, ts))
					ts++
				}
			}
			return points
		}

		// Create data across 3 shards.
		var ids []uint64
		for i := 0; i < 3; i++ {
			ids = append(ids, uint64(i))
			s.MustCreateShardWithData("db0", "rp0", i, genPoints(i)...)
		}
		return ids
	}

	for _, example := range examples {
		for _, index := range tsdb.RegisteredIndexes() {
			shardIDs := setup(index)
			t.Run(example.Name+"_"+index, func(t *testing.T) {
				got, err := s.TagValues(nil, shardIDs, example.Expr)
				if err != nil {
					t.Fatal(err)
				}
				exp := example.Exp

				if !reflect.DeepEqual(got, exp) {
					t.Fatalf("got:\n%#v\n\nexp:\n%#v", got, exp)
				}
			})
			s.Close()
		}
	}
}

func TestStore_Measurements_Auth(t *testing.T) {
	t.Parallel()

	test := func(index string) error {
		s := MustOpenStore(index)
		defer s.Close()

		// Create shard #0 with data.
		s.MustCreateShardWithData("db0", "rp0", 0,
			`cpu,host=serverA value=1  0`,
			`cpu,host=serverA value=2 10`,
			`cpu,region=west value=3 20`,
			`cpu,secret=foo value=5 30`, // cpu still readable because it has other series that can be read.
			`mem,secret=foo value=1 30`,
			`disk value=4 30`,
		)

		authorizer := &internal.AuthorizerMock{
			AuthorizeSeriesReadFn: func(database string, measurement []byte, tags models.Tags) bool {
				if database == "" || tags.GetString("secret") != "" {
					t.Logf("Rejecting series db=%s, m=%s, tags=%v", database, measurement, tags)
					return false
				}
				return true
			},
		}

		names, err := s.MeasurementNames(authorizer, "db0", nil)
		if err != nil {
			return err
		}

		// names should not contain any measurements where none of the associated
		// series are authorised for reads.
		expNames := 2
		var gotNames int
		for _, name := range names {
			if string(name) == "mem" {
				return fmt.Errorf("got measurement %q but it should be filtered.", name)
			}
			gotNames++
		}

		if gotNames != expNames {
			return fmt.Errorf("got %d measurements, but expected %d", gotNames, expNames)
		}

		// Now delete all of the cpu series.
		cond, err := influxql.ParseExpr("host = 'serverA' OR region = 'west'")
		if err != nil {
			return err
		}

		if err := s.DeleteSeries("db0", nil, cond); err != nil {
			return err
		}

		if names, err = s.MeasurementNames(authorizer, "db0", nil); err != nil {
			return err
		}

		// names should not contain any measurements where none of the associated
		// series are authorised for reads.
		expNames = 1
		gotNames = 0
		for _, name := range names {
			if string(name) == "mem" || string(name) == "cpu" {
				return fmt.Errorf("after delete got measurement %q but it should be filtered.", name)
			}
			gotNames++
		}

		if gotNames != expNames {
			return fmt.Errorf("after delete got %d measurements, but expected %d", gotNames, expNames)
		}

		return nil
	}

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {
			if err := test(index); err != nil {
				t.Fatal(err)
			}
		})
	}

}

func TestStore_TagKeys_Auth(t *testing.T) {
	t.Parallel()

	test := func(index string) error {
		s := MustOpenStore(index)
		defer s.Close()

		// Create shard #0 with data.
		s.MustCreateShardWithData("db0", "rp0", 0,
			`cpu,host=serverA value=1  0`,
			`cpu,host=serverA,debug=true value=2 10`,
			`cpu,region=west value=3 20`,
			`cpu,secret=foo,machine=a value=1 20`,
		)

		authorizer := &internal.AuthorizerMock{
			AuthorizeSeriesReadFn: func(database string, measurement []byte, tags models.Tags) bool {
				if database == "" || !bytes.Equal(measurement, []byte("cpu")) || tags.GetString("secret") != "" {
					t.Logf("Rejecting series db=%s, m=%s, tags=%v", database, measurement, tags)
					return false
				}
				return true
			},
		}

		keys, err := s.TagKeys(authorizer, []uint64{0}, nil)
		if err != nil {
			return err
		}

		// keys should not contain any tag keys associated with a series containing
		// a secret tag.
		expKeys := 3
		var gotKeys int
		for _, tk := range keys {
			if got, exp := tk.Measurement, "cpu"; got != exp {
				return fmt.Errorf("got measurement %q, expected %q", got, exp)
			}

			for _, key := range tk.Keys {
				if key == "secret" || key == "machine" {
					return fmt.Errorf("got tag key %q but it should be filtered.", key)
				}
				gotKeys++
			}
		}

		if gotKeys != expKeys {
			return fmt.Errorf("got %d keys, but expected %d", gotKeys, expKeys)
		}

		// Delete the series with region = west
		cond, err := influxql.ParseExpr("region = 'west'")
		if err != nil {
			return err
		}
		if err := s.DeleteSeries("db0", nil, cond); err != nil {
			return err
		}

		if keys, err = s.TagKeys(authorizer, []uint64{0}, nil); err != nil {
			return err
		}

		// keys should not contain any tag keys associated with a series containing
		// a secret tag or the deleted series
		expKeys = 2
		gotKeys = 0
		for _, tk := range keys {
			if got, exp := tk.Measurement, "cpu"; got != exp {
				return fmt.Errorf("got measurement %q, expected %q", got, exp)
			}

			for _, key := range tk.Keys {
				if key == "secret" || key == "machine" || key == "region" {
					return fmt.Errorf("got tag key %q but it should be filtered.", key)
				}
				gotKeys++
			}
		}

		if gotKeys != expKeys {
			return fmt.Errorf("got %d keys, but expected %d", gotKeys, expKeys)
		}

		return nil
	}

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {
			if err := test(index); err != nil {
				t.Fatal(err)
			}
		})
	}

}

func TestStore_TagValues_Auth(t *testing.T) {
	t.Parallel()

	test := func(index string) error {
		s := MustOpenStore(index)
		defer s.Close()

		// Create shard #0 with data.
		s.MustCreateShardWithData("db0", "rp0", 0,
			`cpu,host=serverA value=1  0`,
			`cpu,host=serverA value=2 10`,
			`cpu,host=serverB value=3 20`,
			`cpu,secret=foo,host=serverD value=1 20`,
		)

		authorizer := &internal.AuthorizerMock{
			AuthorizeSeriesReadFn: func(database string, measurement []byte, tags models.Tags) bool {
				if database == "" || !bytes.Equal(measurement, []byte("cpu")) || tags.GetString("secret") != "" {
					t.Logf("Rejecting series db=%s, m=%s, tags=%v", database, measurement, tags)
					return false
				}
				return true
			},
		}

		values, err := s.TagValues(authorizer, []uint64{0}, &influxql.BinaryExpr{
			Op:  influxql.EQ,
			LHS: &influxql.VarRef{Val: "_tagKey"},
			RHS: &influxql.StringLiteral{Val: "host"},
		})

		if err != nil {
			return err
		}

		// values should not contain any tag values associated with a series containing
		// a secret tag.
		expValues := 2
		var gotValues int
		for _, tv := range values {
			if got, exp := tv.Measurement, "cpu"; got != exp {
				return fmt.Errorf("got measurement %q, expected %q", got, exp)
			}

			for _, v := range tv.Values {
				if got, exp := v.Value, "serverD"; got == exp {
					return fmt.Errorf("got tag value %q but it should be filtered.", got)
				}
				gotValues++
			}
		}

		if gotValues != expValues {
			return fmt.Errorf("got %d tags, but expected %d", gotValues, expValues)
		}

		// Delete the series with values serverA
		cond, err := influxql.ParseExpr("host = 'serverA'")
		if err != nil {
			return err
		}
		if err := s.DeleteSeries("db0", nil, cond); err != nil {
			return err
		}

		values, err = s.TagValues(authorizer, []uint64{0}, &influxql.BinaryExpr{
			Op:  influxql.EQ,
			LHS: &influxql.VarRef{Val: "_tagKey"},
			RHS: &influxql.StringLiteral{Val: "host"},
		})

		if err != nil {
			return err
		}

		// values should not contain any tag values associated with a series containing
		// a secret tag.
		expValues = 1
		gotValues = 0
		for _, tv := range values {
			if got, exp := tv.Measurement, "cpu"; got != exp {
				return fmt.Errorf("got measurement %q, expected %q", got, exp)
			}

			for _, v := range tv.Values {
				if got, exp := v.Value, "serverD"; got == exp {
					return fmt.Errorf("got tag value %q but it should be filtered.", got)
				} else if got, exp := v.Value, "serverA"; got == exp {
					return fmt.Errorf("got tag value %q but it should be filtered.", got)
				}
				gotValues++
			}
		}

		if gotValues != expValues {
			return fmt.Errorf("got %d values, but expected %d", gotValues, expValues)
		}
		return nil
	}

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {
			if err := test(index); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Helper to create some tag values
func createTagValues(mname string, kvs map[string][]string) tsdb.TagValues {
	var sz int
	for _, v := range kvs {
		sz += len(v)
	}

	out := tsdb.TagValues{
		Measurement: mname,
		Values:      make([]tsdb.KeyValue, 0, sz),
	}

	for tk, tvs := range kvs {
		for _, tv := range tvs {
			out.Values = append(out.Values, tsdb.KeyValue{Key: tk, Value: tv})
		}
		// We have to sort the KeyValues since that's how they're provided from
		// the tsdb.Store.
		sort.Sort(tsdb.KeyValues(out.Values))
	}

	return out
}

func BenchmarkStore_SeriesCardinality_100_Shards(b *testing.B) {
	for _, index := range tsdb.RegisteredIndexes() {
		store := NewStore(index)
		if err := store.Open(); err != nil {
			panic(err)
		}

		// Write a point to n shards.
		for shardID := 0; shardID < 100; shardID++ {
			if err := store.CreateShard("db", "rp", uint64(shardID), true); err != nil {
				b.Fatalf("create shard: %s", err)
			}

			err := store.WriteToShard(uint64(shardID), []models.Point{models.MustNewPoint("cpu", nil, map[string]interface{}{"value": 1.0}, time.Now())})
			if err != nil {
				b.Fatalf("write: %s", err)
			}
		}

		b.Run(store.EngineOptions.IndexVersion, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _ = store.SeriesCardinality("db")
			}
		})
		store.Close()
	}
}

func BenchmarkStoreOpen_200KSeries_100Shards(b *testing.B) { benchmarkStoreOpen(b, 64, 5, 5, 1, 100) }

func benchmarkStoreOpen(b *testing.B, mCnt, tkCnt, tvCnt, pntCnt, shardCnt int) {
	var store *Store
	setup := func(index string) error {
		store := MustOpenStore(index)

		// Generate test series (measurements + unique tag sets).
		series := genTestSeries(mCnt, tkCnt, tvCnt)

		// Generate point data to write to the shards.
		points := []models.Point{}
		for _, s := range series {
			for val := 0.0; val < float64(pntCnt); val++ {
				p := models.MustNewPoint(s.Measurement, s.Tags, map[string]interface{}{"value": val}, time.Now())
				points = append(points, p)
			}
		}

		// Create requested number of shards in the store & write points.
		for shardID := 0; shardID < shardCnt; shardID++ {
			if err := store.CreateShard("mydb", "myrp", uint64(shardID), true); err != nil {
				return fmt.Errorf("create shard: %s", err)
			}
			if err := store.BatchWrite(shardID, points); err != nil {
				return fmt.Errorf("batch write: %s", err)
			}
		}
		return nil
	}

	for _, index := range tsdb.RegisteredIndexes() {
		if err := setup(index); err != nil {
			b.Fatal(err)
		}
		b.Run(store.EngineOptions.IndexVersion, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				store := tsdb.NewStore(store.Path())
				if err := store.Open(); err != nil {
					b.Fatalf("open store error: %s", err)
				}

				b.StopTimer()
				store.Close()
				b.StartTimer()
			}
		})
		os.RemoveAll(store.Path())
	}
}

// To store result of benchmark (ensure allocated on heap).
var tvResult []tsdb.TagValues

func BenchmarkStore_TagValues(b *testing.B) {
	benchmarks := []struct {
		name         string
		shards       int
		measurements int
		tagValues    int
	}{
		{name: "s=1_m=1_v=100", shards: 1, measurements: 1, tagValues: 100},
		{name: "s=1_m=1_v=1000", shards: 1, measurements: 1, tagValues: 1000},
		{name: "s=1_m=10_v=100", shards: 1, measurements: 10, tagValues: 100},
		{name: "s=1_m=10_v=1000", shards: 1, measurements: 10, tagValues: 1000},
		{name: "s=1_m=100_v=100", shards: 1, measurements: 100, tagValues: 100},
		{name: "s=1_m=100_v=1000", shards: 1, measurements: 100, tagValues: 1000},
		{name: "s=10_m=1_v=100", shards: 10, measurements: 1, tagValues: 100},
		{name: "s=10_m=1_v=1000", shards: 10, measurements: 1, tagValues: 1000},
		{name: "s=10_m=10_v=100", shards: 10, measurements: 10, tagValues: 100},
		{name: "s=10_m=10_v=1000", shards: 10, measurements: 10, tagValues: 1000},
		{name: "s=10_m=100_v=100", shards: 10, measurements: 100, tagValues: 100},
		{name: "s=10_m=100_v=1000", shards: 10, measurements: 100, tagValues: 1000},
	}

	var s *Store
	setup := func(shards, measurements, tagValues int, index string, useRandom bool) []uint64 { // returns shard ids
		s := NewStore(index)
		if err := s.Open(); err != nil {
			panic(err)
		}

		fmtStr := `cpu%[1]d,host=tv%[2]d,shard=s%[3]d,z1=s%[1]d%[2]d,z2=%[4]s value=1 %[5]d`
		// genPoints generates some point data. If ran is true then random tag
		// key values will be generated, meaning more work sorting and merging.
		// If ran is false, then the same set of points will be produced for the
		// same set of parameters, meaning more de-duplication of points will be
		// needed.
		genPoints := func(sid int, ran bool) []string {
			var v, ts int
			var half string
			points := make([]string, 0, measurements*tagValues)
			for m := 0; m < measurements; m++ {
				for tagvid := 0; tagvid < tagValues; tagvid++ {
					v = tagvid
					if ran {
						v = rand.Intn(100000)
					}
					half = fmt.Sprint(rand.Intn(2) == 0)
					points = append(points, fmt.Sprintf(fmtStr, m, v, sid, half, ts))
					ts++
				}
			}
			return points
		}

		// Create data across chosen number of shards.
		var shardIDs []uint64
		for i := 0; i < shards; i++ {
			shardIDs = append(shardIDs, uint64(i))
			s.MustCreateShardWithData("db0", "rp0", i, genPoints(i, useRandom)...)
		}
		return shardIDs
	}

	teardown := func() {
		if err := s.Close(); err != nil {
			b.Fatal(err)
		}
	}

	// SHOW TAG VALUES WITH KEY IN ("host", "shard")
	cond1 := &influxql.ParenExpr{
		Expr: &influxql.BinaryExpr{
			Op: influxql.OR,
			LHS: &influxql.BinaryExpr{
				Op:  influxql.EQ,
				LHS: &influxql.VarRef{Val: "_tagKey"},
				RHS: &influxql.StringLiteral{Val: "host"},
			},
			RHS: &influxql.BinaryExpr{
				Op:  influxql.EQ,
				LHS: &influxql.VarRef{Val: "_tagKey"},
				RHS: &influxql.StringLiteral{Val: "shard"},
			},
		},
	}

	cond2 := &influxql.ParenExpr{
		Expr: &influxql.BinaryExpr{
			Op: influxql.AND,
			LHS: &influxql.ParenExpr{
				Expr: &influxql.BinaryExpr{
					Op:  influxql.EQ,
					LHS: &influxql.VarRef{Val: "z2"},
					RHS: &influxql.StringLiteral{Val: "true"},
				},
			},
			RHS: cond1,
		},
	}

	var err error
	for _, index := range tsdb.RegisteredIndexes() {
		for useRand := 0; useRand < 2; useRand++ {
			for c, condition := range []influxql.Expr{cond1, cond2} {
				for _, bm := range benchmarks {
					shardIDs := setup(bm.shards, bm.measurements, bm.tagValues, index, useRand == 1)
					cnd := "Unfiltered"
					if c == 0 {
						cnd = "Filtered"
					}
					b.Run("random_values="+fmt.Sprint(useRand == 1)+"_index="+index+"_"+cnd+"_"+bm.name, func(b *testing.B) {
						for i := 0; i < b.N; i++ {
							if tvResult, err = s.TagValues(nil, shardIDs, condition); err != nil {
								b.Fatal(err)
							}
						}
					})
					teardown()
				}
			}
		}
	}
}

// Store is a test wrapper for tsdb.Store.
type Store struct {
	*tsdb.Store
	index string
}

// NewStore returns a new instance of Store with a temporary path.
func NewStore(index string) *Store {
	path, err := ioutil.TempDir("", "influxdb-tsdb-")
	if err != nil {
		panic(err)
	}

	s := &Store{Store: tsdb.NewStore(path), index: index}
	s.EngineOptions.IndexVersion = index
	s.EngineOptions.Config.WALDir = filepath.Join(path, "wal")
	s.EngineOptions.Config.TraceLoggingEnabled = true

	if testing.Verbose() {
		s.WithLogger(logger.New(os.Stdout))
	}

	return s
}

// MustOpenStore returns a new, open Store using the specified index,
// at a temporary path.
func MustOpenStore(index string) *Store {
	s := NewStore(index)

	if err := s.Open(); err != nil {
		panic(err)
	}
	return s
}

// Reopen closes and reopens the store as a new store.
func (s *Store) Reopen() error {
	if err := s.Store.Close(); err != nil {
		return err
	}

	s.Store = tsdb.NewStore(s.Path())
	s.EngineOptions.IndexVersion = s.index
	s.EngineOptions.Config.WALDir = filepath.Join(s.Path(), "wal")
	s.EngineOptions.Config.TraceLoggingEnabled = true

	if testing.Verbose() {
		s.WithLogger(logger.New(os.Stdout))
	}
	return s.Store.Open()
}

// Close closes the store and removes the underlying data.
func (s *Store) Close() error {
	defer os.RemoveAll(s.Path())
	return s.Store.Close()
}

// MustCreateShardWithData creates a shard and writes line protocol data to it.
func (s *Store) MustCreateShardWithData(db, rp string, shardID int, data ...string) {
	if err := s.CreateShard(db, rp, uint64(shardID), true); err != nil {
		panic(err)
	}
	s.MustWriteToShardString(shardID, data...)
}

// MustWriteToShardString parses the line protocol (with second precision) and
// inserts the resulting points into a shard. Panic on error.
func (s *Store) MustWriteToShardString(shardID int, data ...string) {
	var points []models.Point
	for i := range data {
		a, err := models.ParsePointsWithPrecision([]byte(strings.TrimSpace(data[i])), time.Time{}, "s")
		if err != nil {
			panic(err)
		}
		points = append(points, a...)
	}

	if err := s.WriteToShard(uint64(shardID), points); err != nil {
		panic(err)
	}
}

// BatchWrite writes points to a shard in chunks.
func (s *Store) BatchWrite(shardID int, points []models.Point) error {
	nPts := len(points)
	chunkSz := 10000
	start := 0
	end := chunkSz

	for {
		if end > nPts {
			end = nPts
		}
		if end-start == 0 {
			break
		}

		if err := s.WriteToShard(uint64(shardID), points[start:end]); err != nil {
			return err
		}
		start = end
		end += chunkSz
	}
	return nil
}

// ParseTags returns an instance of Tags for a comma-delimited list of key/values.
func ParseTags(s string) query.Tags {
	m := make(map[string]string)
	for _, kv := range strings.Split(s, ",") {
		a := strings.Split(kv, "=")
		m[a[0]] = a[1]
	}
	return query.NewTags(m)
}

func dirExists(path string) bool {
	var err error
	if _, err = os.Stat(path); err == nil {
		return true
	}
	return !os.IsNotExist(err)
}

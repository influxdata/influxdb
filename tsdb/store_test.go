//lint:file-ignore SA2002 this is older code, and `go test` will panic if its really a problem.
package tsdb_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2/predicate"

	"github.com/davecgh/go-spew/spew"
	"github.com/influxdata/influxdb/v2/influxql/query"
	"github.com/influxdata/influxdb/v2/internal"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/pkg/deep"
	"github.com/influxdata/influxdb/v2/pkg/slices"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/influxdata/influxql"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

// Ensure the store can delete a retention policy and all shards under
// it.
func TestStore_DeleteRetentionPolicy(t *testing.T) {

	test := func(t *testing.T, index string) {
		s := MustOpenStore(t, index)
		defer s.Close()

		// Create a new shard and verify that it exists.
		if err := s.CreateShard(context.Background(), "db0", "rp0", 1, true); err != nil {
			t.Fatal(err)
		} else if sh := s.Shard(1); sh == nil {
			t.Fatalf("expected shard")
		}

		// Create a new shard under the same retention policy,  and verify
		// that it exists.
		if err := s.CreateShard(context.Background(), "db0", "rp0", 2, true); err != nil {
			t.Fatal(err)
		} else if sh := s.Shard(2); sh == nil {
			t.Fatalf("expected shard")
		}

		// Create a new shard under a different retention policy, and
		// verify that it exists.
		if err := s.CreateShard(context.Background(), "db0", "rp1", 3, true); err != nil {
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
		if err := s.Reopen(t); err != nil {
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
		t.Run(index, func(t *testing.T) { test(t, index) })
	}
}

// Ensure the store can create a new shard.
func TestStore_CreateShard(t *testing.T) {

	test := func(t *testing.T, index string) {
		s := MustOpenStore(t, index)
		defer s.Close()

		// Create a new shard and verify that it exists.
		if err := s.CreateShard(context.Background(), "db0", "rp0", 1, true); err != nil {
			t.Fatal(err)
		} else if sh := s.Shard(1); sh == nil {
			t.Fatalf("expected shard")
		}

		// Create another shard and verify that it exists.
		if err := s.CreateShard(context.Background(), "db0", "rp0", 2, true); err != nil {
			t.Fatal(err)
		} else if sh := s.Shard(2); sh == nil {
			t.Fatalf("expected shard")
		}

		// Reopen shard and recheck.
		if err := s.Reopen(t); err != nil {
			t.Fatal(err)
		} else if sh := s.Shard(1); sh == nil {
			t.Fatalf("expected shard(1)")
		} else if sh = s.Shard(2); sh == nil {
			t.Fatalf("expected shard(2)")
		}
	}

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) { test(t, index) })
	}
}

// Ensure the store can create a new shard.
func TestStore_StartupShardProgress(t *testing.T) {
	t.Parallel()

	test := func(index string) {
		s := MustOpenStore(t, index)
		defer s.Close()

		// Create a new shard and verify that it exists.
		require.NoError(t, s.CreateShard(context.Background(), "db0", "rp0", 1, true))
		sh := s.Shard(1)
		require.NotNil(t, sh)

		// Create another shard and verify that it exists.
		require.NoError(t, s.CreateShard(context.Background(), "db0", "rp0", 2, true))
		sh = s.Shard(2)
		require.NotNil(t, sh)

		msl := &mockStartupLogger{}

		// Reopen shard and recheck.
		require.NoError(t, s.Reopen(t, WithStartupMetrics(msl)))

		// Equality check to make sure shards are always added prior to
		// completion being called. This test opens 3 total shards - 1 shard
		// fails, but we still want to track that it was attempted to be opened.
		require.Equal(t, msl.shardTracker, []string{
			"shard-add",
			"shard-add",
			"shard-complete",
			"shard-complete",
		})
	}

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) { test(index) })
	}
}

// Introduces a test to ensure that shard loading still accounts for bad shards. We still want these to show up
// during the initial shard loading even though its in a error state.
func TestStore_BadShardLoading(t *testing.T) {
	t.Parallel()

	test := func(index string) {
		s := MustOpenStore(t, index)
		defer s.Close()

		// Create a new shard and verify that it exists.
		require.NoError(t, s.CreateShard(context.Background(), "db0", "rp0", 1, true))
		sh := s.Shard(1)
		require.NotNil(t, sh)

		// Create another shard and verify that it exists.
		require.NoError(t, s.CreateShard(context.Background(), "db0", "rp0", 2, true))
		sh = s.Shard(2)
		require.NotNil(t, sh)

		// Create another shard and verify that it exists.
		require.NoError(t, s.CreateShard(context.Background(), "db0", "rp0", 3, true))
		sh = s.Shard(3)
		require.NotNil(t, sh)

		s.SetShardOpenErrorForTest(sh.ID(), errors.New("a shard opening error occurred"))
		err2 := s.OpenShard(context.Background(), s.Shard(sh.ID()), false)
		require.Error(t, err2, "no error opening bad shard")

		msl := &mockStartupLogger{}

		// Reopen shard and recheck.
		require.NoError(t, s.Reopen(t, WithStartupMetrics(msl)))

		// Equality check to make sure shards are always added prior to
		// completion being called. This test opens 3 total shards - 1 shard
		// fails, but we still want to track that it was attempted to be opened.
		require.Equal(t, msl.shardTracker, []string{
			"shard-add",
			"shard-add",
			"shard-add",
			"shard-complete",
			"shard-complete",
			"shard-complete",
		})
	}

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) { test(index) })
	}
}

func TestStore_BadShard(t *testing.T) {
	const errStr = "a shard open error"
	indexes := tsdb.RegisteredIndexes()
	for _, idx := range indexes {
		func() {
			s := MustOpenStore(t, idx)
			defer require.NoErrorf(t, s.Close(), "closing store with index type: %s", idx)

			sh := tsdb.NewTempShard(t, idx)
			shId := sh.ID()
			err := s.OpenShard(context.Background(), sh.Shard, false)
			require.NoError(t, err, "opening temp shard")
			require.NoError(t, sh.Close(), "closing temporary shard")

			expErr := errors.New(errStr)
			s.SetShardOpenErrorForTest(sh.ID(), expErr)
			err2 := s.OpenShard(context.Background(), sh.Shard, false)
			require.Error(t, err2, "no error opening bad shard")
			require.True(t, errors.Is(err2, tsdb.ErrPreviousShardFail{}), "exp: ErrPreviousShardFail, got: %v", err2)
			require.EqualError(t, err2, fmt.Errorf("not attempting to open shard %d; opening shard previously failed with: %w", shId, expErr).Error())

			// This should succeed with the force (and because opening an open shard automatically succeeds)
			require.NoError(t, s.OpenShard(context.Background(), sh.Shard, true), "forced re-opening previously failing shard")
			require.NoError(t, sh.Close())
		}()
	}
}

func TestStore_DropConcurrentWriteMultipleShards(t *testing.T) {

	test := func(t *testing.T, index string) {
		s := MustOpenStore(t, index)
		defer s.Close()

		if err := s.CreateShard(context.Background(), "db0", "rp0", 1, true); err != nil {
			t.Fatal(err)
		}

		s.MustWriteToShardString(1, "mem,server=a v=1 10")

		if err := s.CreateShard(context.Background(), "db0", "rp0", 2, true); err != nil {
			t.Fatal(err)
		}

		s.MustWriteToShardString(2, "mem,server=b v=1 20")

		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				s.MustWriteToShardString(1, "cpu,server=a v=1 10")
				s.MustWriteToShardString(2, "cpu,server=b v=1 20")
			}
		}()

		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				err := s.DeleteMeasurement(context.Background(), "db0", "cpu")
				if err != nil {
					t.Error(err)
					return
				}
			}
		}()

		wg.Wait()

		err := s.DeleteMeasurement(context.Background(), "db0", "cpu")
		if err != nil {
			t.Fatal(err)
		}

		measurements, err := s.MeasurementNames(context.Background(), query.OpenAuthorizer, "db0", nil)
		if err != nil {
			t.Fatal(err)
		}

		exp := [][]byte{[]byte("mem")}
		if got, exp := measurements, exp; !reflect.DeepEqual(got, exp) {
			t.Fatal(fmt.Errorf("got measurements %v, expected %v", got, exp))
		}
	}

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) { test(t, index) })
	}
}

func TestStore_WriteMixedShards(t *testing.T) {

	test := func(t *testing.T, index1 string, index2 string) {
		s := MustOpenStore(t, index1)
		defer s.Close()

		if err := s.CreateShard(context.Background(), "db0", "rp0", 1, true); err != nil {
			t.Fatal(err)
		}

		s.MustWriteToShardString(1, "mem,server=a v=1 10")

		s.EngineOptions.IndexVersion = index2
		s.index = index2
		if err := s.Reopen(t); err != nil {
			t.Fatal(err)
		}

		if err := s.CreateShard(context.Background(), "db0", "rp0", 2, true); err != nil {
			t.Fatal(err)
		}

		s.MustWriteToShardString(2, "mem,server=b v=1 20")

		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				s.MustWriteToShardString(1, fmt.Sprintf("cpu,server=a,f%0.2d=a v=1", i*2))
			}
		}()

		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				s.MustWriteToShardString(2, fmt.Sprintf("cpu,server=b,f%0.2d=b v=1 20", i*2+1))
			}
		}()

		wg.Wait()

		keys, err := s.TagKeys(context.Background(), nil, []uint64{1, 2}, nil)
		if err != nil {
			t.Fatal(err)
		}

		cpuKeys := make([]string, 101)
		for i := 0; i < 100; i++ {
			cpuKeys[i] = fmt.Sprintf("f%0.2d", i)
		}
		cpuKeys[100] = "server"
		expKeys := []tsdb.TagKeys{
			{Measurement: "cpu", Keys: cpuKeys},
			{Measurement: "mem", Keys: []string{"server"}},
		}
		if got, exp := keys, expKeys; !reflect.DeepEqual(got, exp) {
			t.Fatalf("got keys %v, expected %v", got, exp)
		}
	}

	indexes := tsdb.RegisteredIndexes()
	for i := range indexes {
		j := (i + 1) % len(indexes)
		index1 := indexes[i]
		index2 := indexes[j]
		t.Run(fmt.Sprintf("%s-%s", index1, index2), func(t *testing.T) { test(t, index1, index2) })
	}
}

// Ensure the store does not return an error when delete from a non-existent db.
func TestStore_DeleteSeries_NonExistentDB(t *testing.T) {

	test := func(t *testing.T, index string) {
		s := MustOpenStore(t, index)
		defer s.Close()

		if err := s.DeleteSeries(context.Background(), "db0", nil, nil); err != nil {
			t.Fatal(err.Error())
		}
	}

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) { test(t, index) })
	}
}

// Ensure the store can delete an existing shard.
func TestStore_DeleteShard(t *testing.T) {

	test := func(t *testing.T, index string) error {
		s := MustOpenStore(t, index)
		defer s.Close()

		// Create a new shard and verify that it exists.
		if err := s.CreateShard(context.Background(), "db0", "rp0", 1, true); err != nil {
			return err
		} else if sh := s.Shard(1); sh == nil {
			return fmt.Errorf("expected shard")
		}

		// Create another shard.
		if err := s.CreateShard(context.Background(), "db0", "rp0", 2, true); err != nil {
			return err
		} else if sh := s.Shard(2); sh == nil {
			return fmt.Errorf("expected shard")
		}

		// and another, but in a different db.
		if err := s.CreateShard(context.Background(), "db1", "rp0", 3, true); err != nil {
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
		if err := s.Reopen(t); err != nil {
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
		keys, err := s.TagKeys(context.Background(), nil, []uint64{2}, nil)
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
		if keys, err = s.TagKeys(context.Background(), nil, []uint64{3}, nil); err != nil {
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
			if err := test(t, index); err != nil {
				t.Error(err)
			}
		})
	}
}

// Ensure the store can create a snapshot to a shard.
func TestStore_CreateShardSnapShot(t *testing.T) {

	test := func(t *testing.T, index string) {
		s := MustOpenStore(t, index)
		defer s.Close()

		// Create a new shard and verify that it exists.
		if err := s.CreateShard(context.Background(), "db0", "rp0", 1, true); err != nil {
			t.Fatal(err)
		} else if sh := s.Shard(1); sh == nil {
			t.Fatalf("expected shard")
		}

		dir, e := s.CreateShardSnapshot(1, false)
		if e != nil {
			t.Fatal(e)
		}
		if dir == "" {
			t.Fatal("empty directory name")
		}
	}

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) { test(t, index) })
	}
}

func TestStore_Open(t *testing.T) {

	test := func(t *testing.T, index string) {
		s := NewStore(t, index)
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
		if err := s.Open(context.Background()); err != nil {
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
		t.Run(index, func(t *testing.T) { test(t, index) })
	}
}

// Ensure the store reports an error when it can't open a database directory.
func TestStore_Open_InvalidDatabaseFile(t *testing.T) {

	test := func(t *testing.T, index string) {
		s := NewStore(t, index)
		defer s.Close()

		// Create a file instead of a directory for a database.
		require.NoError(t, os.MkdirAll(s.Path(), 0777))
		f, err := os.Create(filepath.Join(s.Path(), "db0"))
		if err != nil {
			t.Fatal(err)
		}
		require.NoError(t, f.Close())

		// Store should ignore database since it's a file.
		if err := s.Open(context.Background()); err != nil {
			t.Fatal(err)
		} else if n := len(s.Databases()); n != 0 {
			t.Fatalf("unexpected database index count: %d", n)
		}
	}

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) { test(t, index) })
	}
}

// Ensure the store reports an error when it can't open a retention policy.
func TestStore_Open_InvalidRetentionPolicy(t *testing.T) {

	test := func(t *testing.T, index string) {
		s := NewStore(t, index)
		defer s.Close()

		// Create an RP file instead of a directory.
		if err := os.MkdirAll(filepath.Join(s.Path(), "db0"), 0777); err != nil {
			t.Fatal(err)
		}

		f, err := os.Create(filepath.Join(s.Path(), "db0", "rp0"))
		if err != nil {
			t.Fatal(err)
		}
		require.NoError(t, f.Close())

		// Store should ignore retention policy since it's a file, and there should
		// be no indices created.
		if err := s.Open(context.Background()); err != nil {
			t.Fatal(err)
		} else if n := len(s.Databases()); n != 0 {
			t.Log(s.Databases())
			t.Fatalf("unexpected database index count: %d", n)
		}
	}

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) { test(t, index) })
	}
}

// Ensure the store reports an error when it can't open a retention policy.
func TestStore_Open_InvalidShard(t *testing.T) {

	test := func(t *testing.T, index string) {
		s := NewStore(t, index)
		defer s.Close()

		// Create a non-numeric shard file.
		if err := os.MkdirAll(filepath.Join(s.Path(), "db0", "rp0"), 0777); err != nil {
			t.Fatal(err)
		}

		f, err := os.Create(filepath.Join(s.Path(), "db0", "rp0", "bad_shard"))
		if err != nil {
			t.Fatal(err)
		}
		require.NoError(t, f.Close())

		// Store should ignore shard since it does not have a numeric name.
		if err := s.Open(context.Background()); err != nil {
			t.Fatal(err)
		} else if n := len(s.Databases()); n != 0 {
			t.Fatalf("unexpected database index count: %d", n)
		} else if n := s.ShardN(); n != 0 {
			t.Fatalf("unexpected shard count: %d", n)
		}
	}

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) { test(t, index) })
	}
}

// Ensure shards can create iterators.
func TestShards_CreateIterator(t *testing.T) {

	test := func(t *testing.T, index string) {
		s := MustOpenStore(t, index)
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
		t.Run(index, func(t *testing.T) { test(t, index) })
	}
}

func requireFloatIteratorPoints(t *testing.T, fitr query.FloatIterator, expPts []*query.FloatPoint) {
	for idx, expPt := range expPts {
		pt, err := fitr.Next()
		require.NoError(t, err, "Got error on index=%d", idx)
		require.Equal(t, expPt, pt, "Mismatch on index=%d", idx)
	}
	pt, err := fitr.Next()
	require.NoError(t, err)
	require.Nil(t, pt)
}

func walFiles(t *testing.T, s *Store) []string {
	// Make sure the WAL directory is really there so we don't get false empties because
	// the path calcuation is wrong or a cleanup routine blew away the whole WAL directory.
	require.DirExists(t, s.walPath)
	var dirs, files []string
	err := filepath.Walk(s.walPath, func(path string, info fs.FileInfo, err error) error {
		if info.IsDir() {
			dirs = append(dirs, path)
		} else {
			files = append(files, path)
		}
		return nil
	})
	require.NoError(t, err)
	require.NotEmpty(t, dirs, "expected shard WAL directories, none found")
	return files
}

func TestStore_FlushWALOnClose(t *testing.T) {

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run("TestStore_FlushWALOnClose_"+index, func(t *testing.T) {
			s := MustOpenStore(t, index, WithWALFlushOnShutdown(true))
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
			expPts := []*query.FloatPoint{
				&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=serverA"), Time: time.Unix(0, 0).UnixNano(), Value: 1},
				&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=serverA"), Time: time.Unix(10, 0).UnixNano(), Value: 2},
				&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=serverA"), Time: time.Unix(30, 0).UnixNano(), Value: 1},
				&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=serverB"), Time: time.Unix(20, 0).UnixNano(), Value: 3},
				&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=serverC"), Time: time.Unix(60, 0).UnixNano(), Value: 3},
			}

			require.NotEmpty(t, walFiles(t, s))

			checkPoints := func(exp []*query.FloatPoint) {
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
				require.NoError(t, err)
				require.NotNil(t, itr)
				defer itr.Close()
				fitr, ok := itr.(query.FloatIterator)
				require.True(t, ok)
				requireFloatIteratorPoints(t, fitr, exp)
			}
			checkPoints(expPts)

			require.NoError(t, s.Close())
			s.Store = nil
			require.Empty(t, walFiles(t, s))

			require.NoError(t, s.Reopen(t))
			checkPoints(expPts)
			require.Empty(t, walFiles(t, s)) // we haven't written any points, no WAL yet.
			s.MustWriteToShardString(1, `cpu,host=serverC value=5 90`)
			expPts = append(expPts, &query.FloatPoint{Name: "cpu", Tags: ParseTags("host=serverC"), Time: time.Unix(90, 0).UnixNano(), Value: 5})
			checkPoints(expPts)
			require.NotEmpty(t, walFiles(t, s)) // we create a WAL file with the write

			// One shard has a WAL, one does not
			require.NoError(t, s.Close())
			s.Store = nil
			require.Empty(t, walFiles(t, s))

			// Open again
			require.NoError(t, s.Reopen(t))
			checkPoints(expPts)
			require.Empty(t, walFiles(t, s))

			// Close with no writes, no WAL files
			require.NoError(t, s.Close())
			s.Store = nil
			require.Empty(t, walFiles(t, s))

			// Open again, but this time don't flush WAL on shutdown
			require.NoError(t, s.Reopen(t, WithWALFlushOnShutdown(false)))
			checkPoints(expPts)
			require.Empty(t, walFiles(t, s))

			// Write new point, creating WAL on one shard
			s.MustWriteToShardString(1, `cpu,host=serverC value=6 100`)
			expPts = append(expPts, &query.FloatPoint{Name: "cpu", Tags: ParseTags("host=serverC"), Time: time.Unix(100, 0).UnixNano(), Value: 6})
			checkPoints(expPts)
			require.NotEmpty(t, walFiles(t, s)) // we create a WAL file with the write

			// Close and check that the shard is not flushed
			require.NoError(t, s.Close())
			s.Store = nil
			require.NotEmpty(t, walFiles(t, s))

			// Let's make sure we /really/ didn't flush the WAL by deleting the WAL and then making sure that last point we wrote has gone missing.
			require.NoError(t, os.RemoveAll(s.walPath))
			require.NoError(t, s.Reopen(t))
			expPts = expPts[:len(expPts)-1]
			checkPoints(expPts)
			require.Empty(t, walFiles(t, s)) // also sanity checks that WAL directories have been recreated
		})
	}
}

// Test new reader blocking.
func TestStore_NewReadersBlocked(t *testing.T) {
	//t.Parallel()

	test := func(index string) {
		t.Helper()
		s := MustOpenStore(t, index)
		defer s.Close()

		shardInUse := func(shardID uint64) bool {
			t.Helper()
			require.NoError(t, s.SetShardNewReadersBlocked(shardID, true))
			inUse, err := s.ShardInUse(shardID)
			require.NoError(t, err)
			require.NoError(t, s.SetShardNewReadersBlocked(shardID, false))
			return inUse
		}

		// Create shard #0 with data.
		s.MustCreateShardWithData("db0", "rp0", 0,
			`cpu,host=serverA value=1  0`,
			`cpu,host=serverA value=2 10`,
			`cpu,host=serverB value=3 20`,
		)

		// Flush WAL to TSM files.
		sh0 := s.Shard(0)
		require.NotNil(t, sh0)
		sh0.ScheduleFullCompaction()

		// Retrieve shard group.
		shards := s.ShardGroup([]uint64{0})

		m := &influxql.Measurement{Name: "cpu"}
		opts := query.IteratorOptions{
			Expr:       influxql.MustParseExpr(`value`),
			Dimensions: []string{"host"},
			Ascending:  true,
			StartTime:  influxql.MinTime,
			EndTime:    influxql.MaxTime,
		}

		// Block new readers, iterator we get will be a faux iterator with no data.
		require.NoError(t, s.SetShardNewReadersBlocked(0, true))
		require.False(t, shardInUse(0))
		itr, err := shards.CreateIterator(context.Background(), m, opts)
		require.NoError(t, err)
		require.False(t, shardInUse(0)) // Remember, itr is a faux iterator.
		fitr, ok := itr.(query.FloatIterator)
		require.True(t, ok)
		p, err := fitr.Next()
		require.NoError(t, err)
		require.Nil(t, p)
		require.NoError(t, itr.Close())
		require.False(t, shardInUse(0))
		require.NoError(t, s.SetShardNewReadersBlocked(0, false))

		// Create iterator, no blocks present.
		require.False(t, shardInUse(0))
		itr, err = shards.CreateIterator(context.Background(), m, opts)
		require.NoError(t, err)
		require.True(t, shardInUse(0))
		fitr, ok = itr.(query.FloatIterator)
		require.True(t, ok)
		p, err = fitr.Next()
		require.NoError(t, err)
		require.NotNil(t, p)
		require.NoError(t, itr.Close())
		require.False(t, shardInUse(0))
	}

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(fmt.Sprintf("TestStore_NewReadersBlocked_%s", index), func(t *testing.T) { test(index) })
	}
}

// Ensure the store can backup a shard and another store can restore it.
func TestStore_BackupRestoreShard(t *testing.T) {
	test := func(t *testing.T, index string) {
		s0, s1 := MustOpenStore(t, index), MustOpenStore(t, index)
		defer s0.Close()
		defer s1.Close()

		// Create shard with data.
		s0.MustCreateShardWithData("db0", "rp0", 100,
			`cpu value=1 0`,
			`cpu value=2 10`,
			`cpu value=3 20`,
		)

		if err := s0.Reopen(t); err != nil {
			t.Fatal(err)
		}

		// Backup shard to a buffer.
		var buf bytes.Buffer
		if err := s0.BackupShard(100, time.Time{}, &buf); err != nil {
			t.Fatal(err)
		}

		// Create the shard on the other store and restore from buffer.
		if err := s1.CreateShard(context.Background(), "db0", "rp0", 100, true); err != nil {
			t.Fatal(err)
		}
		if err := s1.RestoreShard(context.Background(), 100, &buf); err != nil {
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
		defer itr.Close()
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
		t.Run(index, func(t *testing.T) {
			test(t, index)
		})
	}
}
func TestStore_Shard_SeriesN(t *testing.T) {

	test := func(t *testing.T, index string) error {
		s := MustOpenStore(t, index)
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
			if err := test(t, index); err != nil {
				t.Error(err)
			}
		})
	}
}

func TestStore_MeasurementNames_Deduplicate(t *testing.T) {

	test := func(t *testing.T, index string) {
		s := MustOpenStore(t, index)
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

		meas, err := s.MeasurementNames(context.Background(), query.OpenAuthorizer, "db0", nil)
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
		t.Run(index, func(t *testing.T) { test(t, index) })
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
		if err := store.CreateShard(context.Background(), "db", "rp", uint64(shardID), true); err != nil {
			t.Errorf("create shard: %s", err)
		}

		if err := store.BatchWrite(shardID, points[shardID*40:(shardID+1)*40]); err != nil {
			t.Errorf("batch write: %s", err)
		}
	}

	// Delete all the series for each measurement.
	mnames, err := store.MeasurementNames(context.Background(), nil, "db", nil)
	if err != nil {
		t.Fatal(err)
	}

	for _, name := range mnames {
		if err := store.DeleteSeries(context.Background(), "db", []influxql.Source{&influxql.Measurement{Name: string(name)}}, nil); err != nil {
			t.Fatal(err)
		}
	}

	// Estimate the series cardinality...
	cardinality, err := store.Store.SeriesCardinality(context.Background(), "db")
	if err != nil {
		t.Fatal(err)
	}

	// Estimated cardinality should be well within 10 of the actual cardinality.
	if got, exp := int(cardinality), 10; got > exp {
		t.Errorf("series cardinality was %v (expected within %v), expected was: %d", got, exp, 0)
	}

	// Since all the series have been deleted, all the measurements should have
	// been removed from the index too.
	if cardinality, err = store.Store.MeasurementsCardinality(context.Background(), "db"); err != nil {
		t.Fatal(err)
	}

	// Estimated cardinality should be well within 2 of the actual cardinality.
	// TODO(edd): this is totally arbitrary. How can I make it better?
	if got, exp := int(cardinality), 2; got > exp {
		t.Errorf("measurement cardinality was %v (expected within %v), expected was: %d", got, exp, 0)
	}
}

func TestStore_Cardinality_Tombstoning(t *testing.T) {

	if testing.Short() || os.Getenv("GORACE") != "" || os.Getenv("APPVEYOR") != "" || os.Getenv("CIRCLECI") != "" {
		t.Skip("Skipping test in short, race, circleci and appveyor mode.")
	}

	test := func(t *testing.T, index string) {
		store := NewStore(t, index)
		if err := store.Open(context.Background()); err != nil {
			panic(err)
		}
		defer store.Close()
		testStoreCardinalityTombstoning(t, store)
	}

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) { test(t, index) })
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
		if err := store.CreateShard(context.Background(), "db", "rp", uint64(shardID), true); err != nil {
			t.Fatalf("create shard: %s", err)
		}
		if err := store.BatchWrite(shardID, points[shardID*20000:(shardID+1)*20000]); err != nil {
			t.Fatalf("batch write: %s", err)
		}
	}

	// Estimate the series cardinality...
	cardinality, err := store.Store.SeriesCardinality(context.Background(), "db")
	if err != nil {
		t.Fatal(err)
	}

	// Estimated cardinality should be well within 1.5% of the actual cardinality.
	if got, exp := math.Abs(float64(cardinality)-float64(expCardinality))/float64(expCardinality), 0.015; got > exp {
		t.Errorf("got epsilon of %v for series cardinality %v (expected %v), which is larger than expected %v", got, cardinality, expCardinality, exp)
	}

	// Estimate the measurement cardinality...
	if cardinality, err = store.Store.MeasurementsCardinality(context.Background(), "db"); err != nil {
		t.Fatal(err)
	}

	// Estimated cardinality should be well within 2 of the actual cardinality. (arbitrary...)
	expCardinality = 64
	if got, exp := math.Abs(float64(cardinality)-float64(expCardinality)), 2.0; got > exp {
		t.Errorf("got measurmement cardinality %v, expected upto %v; difference is larger than expected %v", cardinality, expCardinality, exp)
	}
}

func TestStore_Cardinality_Unique(t *testing.T) {

	if testing.Short() || os.Getenv("GORACE") != "" || os.Getenv("APPVEYOR") != "" || os.Getenv("CIRCLECI") != "" {
		t.Skip("Skipping test in short, race, circleci and appveyor mode.")
	}

	test := func(t *testing.T, index string) {
		store := NewStore(t, index)
		if err := store.Open(context.Background()); err != nil {
			panic(err)
		}
		defer store.Close()
		testStoreCardinalityUnique(t, store)
	}

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) { test(t, index) })
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
		if err := store.CreateShard(context.Background(), "db", "rp", uint64(shardID), true); err != nil {
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
			from, to = rand.Intn(len(points)), rand.Intn(len(points))
			if from > to {
				from, to = to, from
			}
		}

		if err := store.BatchWrite(shardID, points[from:to]); err != nil {
			t.Fatalf("batch write: %s", err)
		}
	}

	// Estimate the series cardinality...
	cardinality, err := store.Store.SeriesCardinality(context.Background(), "db")
	if err != nil {
		t.Fatal(err)
	}

	// Estimated cardinality should be well within 1.5% of the actual cardinality.
	if got, exp := math.Abs(float64(cardinality)-float64(expCardinality))/float64(expCardinality), 0.015; got > exp {
		t.Errorf("got epsilon of %v for series cardinality %d (expected %d), which is larger than expected %v", got, cardinality, expCardinality, exp)
	}

	// Estimate the measurement cardinality...
	if cardinality, err = store.Store.MeasurementsCardinality(context.Background(), "db"); err != nil {
		t.Fatal(err)
	}

	// Estimated cardinality should be well within 2 of the actual cardinality. (Arbitrary...)
	expCardinality = 64
	if got, exp := math.Abs(float64(cardinality)-float64(expCardinality)), 2.0; got > exp {
		t.Errorf("got measurement cardinality %v, expected upto %v; difference is larger than expected %v", cardinality, expCardinality, exp)
	}
}

func TestStore_Cardinality_Duplicates(t *testing.T) {

	if testing.Short() || os.Getenv("GORACE") != "" || os.Getenv("APPVEYOR") != "" || os.Getenv("CIRCLECI") != "" {
		t.Skip("Skipping test in short, race, circleci and appveyor mode.")
	}

	test := func(t *testing.T, index string) {
		store := NewStore(t, index)
		if err := store.Open(context.Background()); err != nil {
			panic(err)
		}
		defer store.Close()
		testStoreCardinalityDuplicates(t, store)
	}

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) { test(t, index) })
	}
}

func TestStore_MetaQuery_Timeout(t *testing.T) {
	if testing.Short() || os.Getenv("APPVEYOR") != "" {
		t.Skip("Skipping test in short and appveyor mode.")
	}

	test := func(t *testing.T, index string) {
		store := NewStore(t, index)
		require.NoError(t, store.Open(context.Background()))
		defer store.Close()
		testStoreMetaQueryTimeout(t, store, index)
	}

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {
			test(t, index)
		})
	}
}

func testStoreMetaQueryTimeout(t *testing.T, store *Store, index string) {
	shards := testStoreMetaQuerySetup(t, store)

	testStoreMakeTimedFuncs(func(ctx context.Context) (string, error) {
		const funcName = "SeriesCardinality"
		_, err := store.Store.SeriesCardinality(ctx, "db")
		return funcName, err
	}, index)(t)

	testStoreMakeTimedFuncs(func(ctx context.Context) (string, error) {
		const funcName = "MeasurementsCardinality"
		_, err := store.Store.MeasurementsCardinality(ctx, "db")
		return funcName, err
	}, index)(t)

	keyCondition, allCondition := testStoreMetaQueryCondition()

	testStoreMakeTimedFuncs(func(ctx context.Context) (string, error) {
		const funcName = "TagValues"
		_, err := store.Store.TagValues(ctx, nil, shards, allCondition)
		return funcName, err
	}, index)(t)

	testStoreMakeTimedFuncs(func(ctx context.Context) (string, error) {
		const funcName = "TagKeys"
		_, err := store.Store.TagKeys(ctx, nil, shards, keyCondition)
		return funcName, err
	}, index)(t)

	testStoreMakeTimedFuncs(func(ctx context.Context) (string, error) {
		const funcName = "MeasurementNames"
		_, err := store.Store.MeasurementNames(ctx, nil, "db", nil)
		return funcName, err
	}, index)(t)
}

func testStoreMetaQueryCondition() (influxql.Expr, influxql.Expr) {
	keyCondition := &influxql.ParenExpr{
		Expr: &influxql.BinaryExpr{
			Op: influxql.OR,
			LHS: &influxql.BinaryExpr{
				Op:  influxql.EQ,
				LHS: &influxql.VarRef{Val: "_tagKey"},
				RHS: &influxql.StringLiteral{Val: "tagKey4"},
			},
			RHS: &influxql.BinaryExpr{
				Op:  influxql.EQ,
				LHS: &influxql.VarRef{Val: "_tagKey"},
				RHS: &influxql.StringLiteral{Val: "tagKey5"},
			},
		},
	}

	whereCondition := &influxql.ParenExpr{
		Expr: &influxql.BinaryExpr{
			Op: influxql.AND,
			LHS: &influxql.ParenExpr{
				Expr: &influxql.BinaryExpr{
					Op:  influxql.EQ,
					LHS: &influxql.VarRef{Val: "tagKey1"},
					RHS: &influxql.StringLiteral{Val: "tagValue2"},
				},
			},
			RHS: keyCondition,
		},
	}

	allCondition := &influxql.BinaryExpr{
		Op: influxql.AND,
		LHS: &influxql.ParenExpr{
			Expr: &influxql.BinaryExpr{
				Op:  influxql.EQREGEX,
				LHS: &influxql.VarRef{Val: "tagKey3"},
				RHS: &influxql.RegexLiteral{Val: regexp.MustCompile(`tagValue\d`)},
			},
		},
		RHS: whereCondition,
	}
	return keyCondition, allCondition
}

func testStoreMetaQuerySetup(t *testing.T, store *Store) []uint64 {
	const measurementCnt = 64
	const tagCnt = 5
	const valueCnt = 5
	const pointsPerShard = 20000

	// Generate point data to write to the shards.
	series := genTestSeries(measurementCnt, tagCnt, valueCnt)

	points := make([]models.Point, 0, len(series))
	for _, s := range series {
		points = append(points, models.MustNewPoint(s.Measurement, s.Tags, map[string]interface{}{"value": 1.0}, time.Now()))
	}
	// Create requested number of shards in the store & write points across
	// shards such that we never write the same series to multiple shards.
	shards := make([]uint64, len(points)/pointsPerShard)
	for shardID := 0; shardID < len(points)/pointsPerShard; shardID++ {
		if err := store.CreateShard(context.Background(), "db", "rp", uint64(shardID), true); err != nil {
			t.Fatalf("create shard: %s", err)
		}
		if err := store.BatchWrite(shardID, points[shardID*pointsPerShard:(shardID+1)*pointsPerShard]); err != nil {
			t.Fatalf("batch write: %s", err)
		}
		shards[shardID] = uint64(shardID)
	}
	return shards
}

func testStoreMakeTimedFuncs(tested func(context.Context) (string, error), index string) func(*testing.T) {
	cancelTested := func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(0))
		defer cancel()

		funcName, err := tested(ctx)
		if err == nil {
			t.Fatalf("%v: failed to time out with index type %v", funcName, index)
		} else if !strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
			t.Fatalf("%v: failed with %v instead of %v with index type %v", funcName, err, context.DeadlineExceeded, index)
		}
	}
	return cancelTested
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
		if err := store.CreateShard(context.Background(), "db", "rp", uint64(shardID), true); err != nil {
			return fmt.Errorf("create shard: %s", err)
		}
		if err := store.BatchWrite(shardID, points[shardID*468750:(shardID+1)*468750]); err != nil {
			return fmt.Errorf("batch write: %s", err)
		}
	}

	// Estimate the series cardinality...
	cardinality, err := store.Store.SeriesCardinality(context.Background(), "db")
	if err != nil {
		return err
	}

	// Estimated cardinality should be well within 1.5% of the actual cardinality.
	if got, exp := math.Abs(float64(cardinality)-float64(expCardinality))/float64(expCardinality), 0.015; got > exp {
		return fmt.Errorf("got epsilon of %v for series cardinality %v (expected %v), which is larger than expected %v", got, cardinality, expCardinality, exp)
	}

	// Estimate the measurement cardinality...
	if cardinality, err = store.Store.MeasurementsCardinality(context.Background(), "db"); err != nil {
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
	if testing.Short() || os.Getenv("GORACE") != "" || os.Getenv("APPVEYOR") != "" || os.Getenv("CIRCLECI") != "" {
		t.Skip("Skipping test in short, race, circleci and appveyor mode.")
	}

	test := func(t *testing.T, index string) error {
		store := NewStore(t, index)
		if err := store.Open(context.Background()); err != nil {
			panic(err)
		}
		defer store.Close()
		return testStoreCardinalityCompactions(store)
	}

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {
			if err := test(t, index); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStore_Sketches(t *testing.T) {

	checkCardinalities := func(store *tsdb.Store, series, tseries, measurements, tmeasurements int) error {
		// Get sketches and check cardinality...
		sketch, tsketch, err := store.SeriesSketches(context.Background(), "db")
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
		if sketch, tsketch, err = store.MeasurementsSketches(context.Background(), "db"); err != nil {
			return err
		}

		if got, exp := int(sketch.Count()), measurements; got-exp < -delta(measurements) || got-exp > delta(measurements) {
			return fmt.Errorf("got measurement cardinality %d, expected ~%d", got, exp)
		}

		if got, exp := int(tsketch.Count()), tmeasurements; got-exp < -delta(tmeasurements) || got-exp > delta(tmeasurements) {
			return fmt.Errorf("got measurement tombstone cardinality %d, expected ~%d", got, exp)
		}

		if mc, err := store.MeasurementsCardinality(context.Background(), "db"); err != nil {
			return fmt.Errorf("unexpected error from MeasurementsCardinality: %w", err)
		} else {
			if mc < 0 {
				return fmt.Errorf("MeasurementsCardinality returned < 0 (%v)", mc)
			}
			expMc := int64(sketch.Count() - tsketch.Count())
			if expMc < 0 {
				expMc = 0
			}
			if got, exp := int(mc), int(expMc); got-exp < -delta(exp) || got-exp > delta(exp) {
				return fmt.Errorf("got measurement cardinality %d, expected ~%d", mc, exp)
			}
		}
		return nil
	}

	test := func(t *testing.T, index string) error {
		store := MustOpenStore(t, index)
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
			if err := store.CreateShard(context.Background(), "db", "rp", uint64(shardID), true); err != nil {
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
		if err := store.Reopen(t); err != nil {
			return err
		}

		// Check cardinalities
		if err := checkCardinalities(store.Store, 160, 0, 10, 0); err != nil {
			return fmt.Errorf("[initial|re-open] %v", err)
		}

		// Delete half the measurements data
		mnames, err := store.MeasurementNames(context.Background(), nil, "db", nil)
		if err != nil {
			return err
		}

		for _, name := range mnames[:len(mnames)/2] {
			if err := store.DeleteSeries(context.Background(), "db", []influxql.Source{&influxql.Measurement{Name: string(name)}}, nil); err != nil {
				return err
			}
		}

		// Check cardinalities.
		expS, expTS, expM, expTM := 160, 80, 10, 5

		// Check cardinalities - tombstones should be in
		if err := checkCardinalities(store.Store, expS, expTS, expM, expTM); err != nil {
			return fmt.Errorf("[initial|re-open|delete] %v", err)
		}

		// Reopen the store.
		if err := store.Reopen(t); err != nil {
			return err
		}

		// Check cardinalities.
		expS, expTS, expM, expTM = 80, 80, 5, 5

		if err := checkCardinalities(store.Store, expS, expTS, expM, expTM); err != nil {
			return fmt.Errorf("[initial|re-open|delete|re-open] %v", err)
		}

		// Now delete the rest of the measurements.
		// This will cause the measurement tombstones to exceed the measurement cardinality for TSI.
		mnames, err = store.MeasurementNames(context.Background(), nil, "db", nil)
		if err != nil {
			return err
		}

		for _, name := range mnames {
			if err := store.DeleteSeries(context.Background(), "db", []influxql.Source{&influxql.Measurement{Name: string(name)}}, nil); err != nil {
				return err
			}
		}

		// Check cardinalities. In this case, the indexes behave differently.
		expS, expTS, expM, expTM = 80, 159, 5, 10
		/*
			if index == inmem.IndexName {
				expS, expTS, expM, expTM = 80, 80, 5, 5
			}
		*/

		// Check cardinalities - tombstones should be in
		if err := checkCardinalities(store.Store, expS, expTS, expM, expTM); err != nil {
			return fmt.Errorf("[initial|re-open|delete] %v", err)
		}

		return nil
	}

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {
			if err := test(t, index); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStore_TagValues(t *testing.T) {

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
				createTagValues("cpu0", map[string][]string{"shard": {"s0"}}),
				createTagValues("cpu1", map[string][]string{"shard": {"s1"}}),
				createTagValues("cpu10", map[string][]string{"host": {"nofoo", "tv0", "tv1", "tv2", "tv3"}, "shard": {"s0", "s1", "s2"}}),
				createTagValues("cpu11", map[string][]string{"host": {"nofoo", "tv0", "tv1", "tv2", "tv3"}, "shard": {"s0", "s1", "s2"}}),
				createTagValues("cpu12", map[string][]string{"host": {"nofoo", "tv0", "tv1", "tv2", "tv3"}, "shard": {"s0", "s1", "s2"}}),
				createTagValues("cpu2", map[string][]string{"shard": {"s2"}}),
			},
		},
		{
			Name: "With WHERE clause",
			Expr: baseWhere,
			Exp: []tsdb.TagValues{
				createTagValues("cpu0", map[string][]string{"shard": {"s0"}}),
				createTagValues("cpu1", map[string][]string{"shard": {"s1"}}),
				createTagValues("cpu10", map[string][]string{"host": {"tv0", "tv1", "tv2", "tv3"}, "shard": {"s0", "s1", "s2"}}),
				createTagValues("cpu11", map[string][]string{"host": {"tv0", "tv1", "tv2", "tv3"}, "shard": {"s0", "s1", "s2"}}),
				createTagValues("cpu12", map[string][]string{"host": {"tv0", "tv1", "tv2", "tv3"}, "shard": {"s0", "s1", "s2"}}),
				createTagValues("cpu2", map[string][]string{"shard": {"s2"}}),
			},
		},
	}

	setup := func(t *testing.T, index string) (*Store, []uint64) { // returns shard ids
		s := MustOpenStore(t, index)

		fmtStr := `cpu1%[1]d,foo=a,ignoreme=nope,host=tv%[2]d,shard=s%[3]d value=1 %[4]d
	cpu1%[1]d,host=nofoo value=1 %[4]d
	mem,host=nothanks value=1 %[4]d
	cpu%[3]d,shard=s%[3]d,foo=a value=2 %[4]d
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
		return s, ids
	}

	for _, example := range examples {
		for _, index := range tsdb.RegisteredIndexes() {
			t.Run(example.Name+"_"+index, func(t *testing.T) {
				s, shardIDs := setup(t, index)
				defer s.Close()
				got, err := s.TagValues(context.Background(), nil, shardIDs, example.Expr)
				if err != nil {
					t.Fatal(err)
				}
				exp := example.Exp

				if !reflect.DeepEqual(got, exp) {
					t.Fatalf("got:\n%#v\n\nexp:\n%#v", got, exp)
				}
			})
		}
	}
}

func TestStore_Measurements_Auth(t *testing.T) {

	test := func(t *testing.T, index string) error {
		s := MustOpenStore(t, index)
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

		names, err := s.MeasurementNames(context.Background(), authorizer, "db0", nil)
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

		if err := s.DeleteSeries(context.Background(), "db0", nil, cond); err != nil {
			return err
		}

		if names, err = s.MeasurementNames(context.Background(), authorizer, "db0", nil); err != nil {
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
			if err := test(t, index); err != nil {
				t.Fatal(err)
			}
		})
	}

}

func TestStore_TagKeys_Auth(t *testing.T) {

	test := func(t *testing.T, index string) error {
		s := MustOpenStore(t, index)
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

		keys, err := s.TagKeys(context.Background(), authorizer, []uint64{0}, nil)
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
		if err := s.DeleteSeries(context.Background(), "db0", nil, cond); err != nil {
			return err
		}

		if keys, err = s.TagKeys(context.Background(), authorizer, []uint64{0}, nil); err != nil {
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
			if err := test(t, index); err != nil {
				t.Fatal(err)
			}
		})
	}

}

func TestStore_TagValues_Auth(t *testing.T) {

	test := func(t *testing.T, index string) error {
		s := MustOpenStore(t, index)
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

		values, err := s.TagValues(context.Background(), authorizer, []uint64{0}, &influxql.BinaryExpr{
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
		if err := s.DeleteSeries(context.Background(), "db0", nil, cond); err != nil {
			return err
		}

		values, err = s.TagValues(context.Background(), authorizer, []uint64{0}, &influxql.BinaryExpr{
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
			if err := test(t, index); err != nil {
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

func TestStore_MeasurementNames_ConcurrentDropShard(t *testing.T) {
	for _, index := range tsdb.RegisteredIndexes() {
		s := MustOpenStore(t, index)
		defer s.Close()

		shardN := 10
		for i := 0; i < shardN; i++ {
			// Create new shards with some data
			s.MustCreateShardWithData("db0", "rp0", i,
				`cpu,host=serverA value=1 30`,
				`mem,region=west value=2 40`, // skip: wrong source
				`cpu,host=serverC value=3 60`,
			)
		}

		done := make(chan struct{})
		errC := make(chan error, 2)

		// Randomly close and open the shards.
		go func() {
			for {
				select {
				case <-done:
					errC <- nil
					return
				default:
					i := uint64(rand.Intn(int(shardN)))
					if sh := s.Shard(i); sh == nil {
						errC <- errors.New("shard should not be nil")
						return
					} else {
						if err := sh.Close(); err != nil {
							errC <- err
							return
						}
						time.Sleep(500 * time.Microsecond)
						if err := s.OpenShard(context.Background(), sh, false); err != nil {
							errC <- err
							return
						}
					}
				}
			}
		}()

		// Attempt to get tag keys from the shards.
		go func() {
			for {
				select {
				case <-done:
					errC <- nil
					return
				default:
					names, err := s.MeasurementNames(context.Background(), nil, "db0", nil)
					if err == tsdb.ErrIndexClosing || err == tsdb.ErrEngineClosed {
						continue // These errors are expected
					}

					if err != nil {
						errC <- err
						return
					}

					if got, exp := names, slices.StringsToBytes("cpu", "mem"); !reflect.DeepEqual(got, exp) {
						errC <- fmt.Errorf("got keys %v, expected %v", got, exp)
						return
					}
				}
			}
		}()

		// Run for 500ms
		time.Sleep(500 * time.Millisecond)
		close(done)

		// Check for errors.
		if err := <-errC; err != nil {
			t.Fatal(err)
		}
		if err := <-errC; err != nil {
			t.Fatal(err)
		}
	}
}

func TestStore_TagKeys_ConcurrentDropShard(t *testing.T) {
	for _, index := range tsdb.RegisteredIndexes() {
		s := MustOpenStore(t, index)
		defer s.Close()

		shardN := 10
		for i := 0; i < shardN; i++ {
			// Create new shards with some data
			s.MustCreateShardWithData("db0", "rp0", i,
				`cpu,host=serverA value=1 30`,
				`mem,region=west value=2 40`, // skip: wrong source
				`cpu,host=serverC value=3 60`,
			)
		}

		done := make(chan struct{})
		errC := make(chan error, 2)

		// Randomly close and open the shards.
		go func() {
			for {
				select {
				case <-done:
					errC <- nil
					return
				default:
					i := uint64(rand.Intn(int(shardN)))
					if sh := s.Shard(i); sh == nil {
						errC <- errors.New("shard should not be nil")
						return
					} else {
						if err := sh.Close(); err != nil {
							errC <- err
							return
						}
						time.Sleep(500 * time.Microsecond)
						if err := s.OpenShard(context.Background(), sh, false); err != nil {
							errC <- err
							return
						}
					}
				}
			}
		}()

		// Attempt to get tag keys from the shards.
		go func() {
			for {
				select {
				case <-done:
					errC <- nil
					return
				default:
					keys, err := s.TagKeys(context.Background(), nil, []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, nil)
					if err == tsdb.ErrIndexClosing || err == tsdb.ErrEngineClosed {
						continue // These errors are expected
					}

					if err != nil {
						errC <- err
						return
					}

					if got, exp := keys[0].Keys, []string{"host"}; !reflect.DeepEqual(got, exp) {
						errC <- fmt.Errorf("got keys %v, expected %v", got, exp)
						return
					}

					if got, exp := keys[1].Keys, []string{"region"}; !reflect.DeepEqual(got, exp) {
						errC <- fmt.Errorf("got keys %v, expected %v", got, exp)
						return
					}
				}
			}
		}()

		// Run for 500ms
		time.Sleep(500 * time.Millisecond)

		close(done)

		// Check for errors
		if err := <-errC; err != nil {
			t.Fatal(err)
		}
		if err := <-errC; err != nil {
			t.Fatal(err)
		}
	}
}

func TestStore_TagValues_ConcurrentDropShard(t *testing.T) {
	for _, index := range tsdb.RegisteredIndexes() {
		s := MustOpenStore(t, index)
		defer s.Close()

		shardN := 10
		for i := 0; i < shardN; i++ {
			// Create new shards with some data
			s.MustCreateShardWithData("db0", "rp0", i,
				`cpu,host=serverA value=1 30`,
				`mem,region=west value=2 40`, // skip: wrong source
				`cpu,host=serverC value=3 60`,
			)
		}

		done := make(chan struct{})
		errC := make(chan error, 2)

		// Randomly close and open the shards.
		go func() {
			for {
				select {
				case <-done:
					errC <- nil
					return
				default:
					i := uint64(rand.Intn(int(shardN)))
					if sh := s.Shard(i); sh == nil {
						errC <- errors.New("shard should not be nil")
						return
					} else {
						if err := sh.Close(); err != nil {
							errC <- err
							return
						}
						time.Sleep(500 * time.Microsecond)
						if err := s.OpenShard(context.Background(), sh, false); err != nil {
							errC <- err
							return
						}
					}
				}
			}
		}()

		// Attempt to get tag keys from the shards.
		go func() {
			for {
				select {
				case <-done:
					errC <- nil
					return
				default:
					stmt, err := influxql.ParseStatement(`SHOW TAG VALUES WITH KEY = "host"`)
					if err != nil {
						t.Error(err)
						return
					}
					rewrite, err := query.RewriteStatement(stmt)
					if err != nil {
						t.Error(err)
						return
					}

					cond := rewrite.(*influxql.ShowTagValuesStatement).Condition
					values, err := s.TagValues(context.Background(), nil, []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, cond)
					if err == tsdb.ErrIndexClosing || err == tsdb.ErrEngineClosed {
						continue // These errors are expected
					}

					if err != nil {
						errC <- err
						return
					}

					exp := tsdb.TagValues{
						Measurement: "cpu",
						Values: []tsdb.KeyValue{
							tsdb.KeyValue{Key: "host", Value: "serverA"},
							tsdb.KeyValue{Key: "host", Value: "serverC"},
						},
					}

					if got := values[0]; !reflect.DeepEqual(got, exp) {
						errC <- fmt.Errorf("got keys %v, expected %v", got, exp)
						return
					}
				}
			}
		}()

		// Run for 500ms
		time.Sleep(500 * time.Millisecond)

		close(done)

		// Check for errors
		if err := <-errC; err != nil {
			t.Fatal(err)
		}
		if err := <-errC; err != nil {
			t.Fatal(err)
		}
	}
}

func TestStore_DeleteByPredicate(t *testing.T) {
	test := func(t *testing.T, index string) error {
		s := MustOpenStore(t, index)
		defer s.Close()

		s.MustCreateShardWithData("db0", "rp0", 0,
			`cpu,host=serverA value=1  0`,
			`cpu,region=west value=3 20`,
			`cpu,secret=foo value=5 30`,
			`mem,secret=foo value=1 30`,
			`disk value=4 30`,
		)

		p, err := predicate.Parse(`_measurement="cpu"`)
		if err != nil {
			return err
		}

		pred, err := predicate.New(p)
		if err != nil {
			return err
		}

		expr, err := influxql.ParseExpr(`_measurement="cpu"`)
		if err != nil {
			return err
		}

		err = s.DeleteSeriesWithPredicate(context.Background(), "db0", math.MinInt, math.MaxInt, pred, expr)
		if err != nil {
			return err
		}

		names, err := s.MeasurementNames(context.Background(), query.OpenAuthorizer, "db0", nil)
		if err != nil {
			return err
		}

		require.Equal(t, 2, len(names), "expected cpu to be deleted, leaving 2 measurements")

		return nil
	}

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {
			if err := test(t, index); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func BenchmarkStore_SeriesCardinality_100_Shards(b *testing.B) {
	for _, index := range tsdb.RegisteredIndexes() {
		store := NewStore(b, index)
		if err := store.Open(context.Background()); err != nil {
			panic(err)
		}

		// Write a point to n shards.
		for shardID := 0; shardID < 100; shardID++ {
			if err := store.CreateShard(context.Background(), "db", "rp", uint64(shardID), true); err != nil {
				b.Fatalf("create shard: %s", err)
			}

			err := store.WriteToShard(context.Background(), uint64(shardID), []models.Point{models.MustNewPoint("cpu", nil, map[string]interface{}{"value": 1.0}, time.Now())})
			if err != nil {
				b.Fatalf("write: %s", err)
			}
		}

		b.Run(store.EngineOptions.IndexVersion, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _ = store.SeriesCardinality(context.Background(), "db")
			}
		})
		store.Close()
	}
}

func BenchmarkStoreOpen_200KSeries_100Shards(b *testing.B) { benchmarkStoreOpen(b, 64, 5, 5, 1, 100) }

func benchmarkStoreOpen(b *testing.B, mCnt, tkCnt, tvCnt, pntCnt, shardCnt int) {
	var store *Store
	setup := func(index string) error {
		store := MustOpenStore(b, index)

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
			if err := store.CreateShard(context.Background(), "mydb", "myrp", uint64(shardID), true); err != nil {
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
				if err := store.Open(context.Background()); err != nil {
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

	setup := func(shards, measurements, tagValues int, index string, useRandom bool) (*Store, []uint64) { // returns shard ids
		s := NewStore(b, index)
		if err := s.Open(context.Background()); err != nil {
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
		return s, shardIDs
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
					s, shardIDs := setup(bm.shards, bm.measurements, bm.tagValues, index, useRand == 1)
					teardown := func() {
						if err := s.Close(); err != nil {
							b.Fatal(err)
						}
					}
					cnd := "Unfiltered"
					if c == 0 {
						cnd = "Filtered"
					}
					b.Run("random_values="+fmt.Sprint(useRand == 1)+"_index="+index+"_"+cnd+"_"+bm.name, func(b *testing.B) {
						for i := 0; i < b.N; i++ {
							if tvResult, err = s.TagValues(context.Background(), nil, shardIDs, condition); err != nil {
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
	path    string
	index   string
	walPath string
	opts    []StoreOption
}

type StoreOption func(s *Store) error

func WithWALFlushOnShutdown(flush bool) StoreOption {
	return func(s *Store) error {
		s.EngineOptions.Config.WALFlushOnShutdown = flush
		return nil
	}
}

func WithStartupMetrics(sm *mockStartupLogger) StoreOption {
	return func(s *Store) error {
		s.WithStartupMetrics(sm)
		return nil
	}
}

// NewStore returns a new instance of Store with a temporary path.
func NewStore(tb testing.TB, index string, opts ...StoreOption) *Store {
	tb.Helper()

	// The WAL directory must not be rooted under the data path. Otherwise reopening
	// the store will generate series indices for the WAL directories.
	rootPath := tb.TempDir()
	path := filepath.Join(rootPath, "data")
	walPath := filepath.Join(rootPath, "wal")

	s := &Store{
		Store:   tsdb.NewStore(path),
		path:    path,
		index:   index,
		walPath: walPath,
		opts:    opts,
	}
	s.EngineOptions.IndexVersion = index
	s.EngineOptions.Config.WALDir = walPath
	s.EngineOptions.Config.TraceLoggingEnabled = true
	s.WithLogger(zaptest.NewLogger(tb))

	for _, o := range s.opts {
		err := o(s)
		require.NoError(tb, err)
	}

	return s
}

// MustOpenStore returns a new, open Store using the specified index,
// at a temporary path.
func MustOpenStore(tb testing.TB, index string, opts ...StoreOption) *Store {
	tb.Helper()

	s := NewStore(tb, index, opts...)

	if err := s.Open(context.Background()); err != nil {
		panic(err)
	}
	return s
}

// Reopen closes and reopens the store as a new store.
func (s *Store) Reopen(tb testing.TB, newOpts ...StoreOption) error {
	tb.Helper()

	if s.Store != nil {
		if err := s.Store.Close(); err != nil {
			return err
		}
	}

	s.Store = tsdb.NewStore(s.path)
	s.EngineOptions.IndexVersion = s.index
	s.EngineOptions.Config.WALDir = s.walPath
	s.EngineOptions.Config.TraceLoggingEnabled = true
	s.WithLogger(zaptest.NewLogger(tb))
	if len(newOpts) > 0 {
		s.opts = newOpts
	}

	for _, o := range s.opts {
		err := o(s)
		require.NoError(tb, err)
	}

	return s.Store.Open(context.Background())
}

// Close closes the store and removes the underlying data.
func (s *Store) Close() error {
	if s.Store != nil {
		return s.Store.Close()
	}
	return nil
}

// MustCreateShardWithData creates a shard and writes line protocol data to it.
func (s *Store) MustCreateShardWithData(db, rp string, shardID int, data ...string) {
	if err := s.CreateShard(context.Background(), db, rp, uint64(shardID), true); err != nil {
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

	if err := s.WriteToShard(context.Background(), uint64(shardID), points); err != nil {
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

		if err := s.WriteToShard(context.Background(), uint64(shardID), points[start:end]); err != nil {
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

type mockStartupLogger struct {
	shardTracker []string
	mu           sync.Mutex
}

func (m *mockStartupLogger) AddShard() {
	m.mu.Lock()
	m.shardTracker = append(m.shardTracker, "shard-add")
	m.mu.Unlock()
}

func (m *mockStartupLogger) CompletedShard() {
	m.mu.Lock()
	m.shardTracker = append(m.shardTracker, "shard-complete")
	m.mu.Unlock()
}

package tsm1_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/tsdb/cursors"
	"github.com/influxdata/influxdb/v2/tsdb/tsm1"
	"github.com/influxdata/influxql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func strL(s ...string) []string { return s }

func TestEngine_MeasurementNamesNoTime(t *testing.T) {
	e, err := NewEngine(tsm1.NewConfig(), t)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	orgs := []struct {
		org, bucket influxdb.ID
	}{
		{
			org:    0x5020,
			bucket: 0x5100,
		},
		{
			org:    0x6000,
			bucket: 0x6100,
		},
	}

	// this org will require escaping the 0x20 byte
	e.MustWritePointsString(orgs[0].org, orgs[0].bucket, `
cpu,cpu0=v,cpu1=v,cpu2=v f=1 101
cpu,cpu1=v               f=1 103
cpu,cpu2=v               f=1 105
cpu,cpu0=v,cpu2=v        f=1 107
cpu,cpu2=v,cpu3=v,other=c        f=1 109
mem,mem0=v,mem1=v,other=m        f=1 101`)
	e.MustWritePointsString(orgs[1].org, orgs[1].bucket, `
cpu2,cpu0=v,cpu1=v,cpu2=v f=1 101
cpu2,cpu1=v               f=1 103
cpu2,cpu2=v               f=1 105
cpu2,cpu0=v,cpu2=v        f=1 107
cpu2,cpu2=v,cpu3=v,other=c        f=1 109
mem2,mem0=v,mem1=v,other=m        f=1 101`)

	// this test verifies the index is immediately queryable before TSM is written
	t.Run("gets all measurements before snapshot", func(t *testing.T) {
		iter, err := e.MeasurementNamesNoTime(context.Background(), orgs[0].org, orgs[0].bucket, nil)
		require.NoError(t, err)
		assert.Equal(t, cursors.StringIteratorToSlice(iter), strL("cpu", "mem"))
	})

	// this test verifies the index is immediately queryable before TSM is written
	t.Run("verify subset of measurements with predicate", func(t *testing.T) {
		iter, err := e.MeasurementNamesNoTime(context.Background(), orgs[0].org, orgs[0].bucket, influxql.MustParseExpr("other = 'c'"))
		require.NoError(t, err)
		assert.Equal(t, cursors.StringIteratorToSlice(iter), strL("cpu"))
	})

	// delete some data from the first bucket
	e.MustDeleteBucketRange(orgs[0].org, orgs[0].bucket, 0, 105)

	// this test verifies measurement disappears if deleted whilst in cache
	t.Run("only contains cpu measurement", func(t *testing.T) {
		iter, err := e.MeasurementNamesNoTime(context.Background(), orgs[0].org, orgs[0].bucket, nil)
		require.NoError(t, err)
		assert.Equal(t, cursors.StringIteratorToSlice(iter), strL("cpu"))
	})

	// write the values back
	e.MustWritePointsString(orgs[0].org, orgs[0].bucket, `
cpu,cpu0=v,cpu1=v,cpu2=v f=1 101
cpu,cpu1=v               f=1 103
cpu,cpu2=v               f=1 105
mem,mem0=v,mem1=v,other=m        f=1 101`)

	// send some points to TSM data
	e.MustWriteSnapshot()

	// this test verifies the index is immediately queryable before TSM is written
	t.Run("contains cpu and mem measurement in TSM", func(t *testing.T) {
		iter, err := e.MeasurementNamesNoTime(context.Background(), orgs[0].org, orgs[0].bucket, nil)
		require.NoError(t, err)
		assert.Equal(t, cursors.StringIteratorToSlice(iter), strL("cpu", "mem"))
	})

	// delete some data from the first bucket
	e.MustDeleteBucketRange(orgs[0].org, orgs[0].bucket, 0, 105)

	// this test verifies measurement disappears if deleted from TSM
	t.Run("only contains cpu measurement in TSM", func(t *testing.T) {
		iter, err := e.MeasurementNamesNoTime(context.Background(), orgs[0].org, orgs[0].bucket, nil)
		require.NoError(t, err)
		assert.Equal(t, cursors.StringIteratorToSlice(iter), strL("cpu"))
	})

	e.MustDeleteBucketRange(orgs[0].org, orgs[0].bucket, 0, 1000)

	// this test verifies all measurements disappears if deleted
	t.Run("no measurements", func(t *testing.T) {
		iter, err := e.MeasurementNamesNoTime(context.Background(), orgs[0].org, orgs[0].bucket, nil)
		require.NoError(t, err)
		assert.Equal(t, cursors.StringIteratorToSlice(iter), strL())
	})

}

func TestEngine_MeasurementTagValuesNoTime(t *testing.T) {
	e, err := NewEngine(tsm1.NewConfig(), t)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	orgs := []struct {
		org, bucket influxdb.ID
	}{
		{
			org:    0x5020,
			bucket: 0x5100,
		},
		{
			org:    0x6000,
			bucket: 0x6100,
		},
	}

	// this org will require escaping the 0x20 byte
	e.MustWritePointsString(orgs[0].org, orgs[0].bucket, `
cpuA,host=0A,os=linux value=1.1 101
cpuA,host=AA,os=linux value=1.2 102
cpuA,host=AA,os=linux value=1.3 104
cpuA,host=CA,os=linux value=1.3 104
cpuA,host=CA,os=linux value=1.3 105
cpuA,host=DA,os=macOS value=1.3 106
memA,host=DA,os=macOS value=1.3 101`)
	e.MustWritePointsString(orgs[1].org, orgs[1].bucket, `
cpuB,host=0B,os=linux value=1.1 101
cpuB,host=AB,os=linux value=1.2 102
cpuB,host=AB,os=linux value=1.3 104
cpuB,host=CB,os=linux value=1.3 104
cpuB,host=CB,os=linux value=1.3 105
cpuB,host=DB,os=macOS value=1.3 106
memB,host=DB,os=macOS value=1.3 101`)

	t.Run("before snapshot", func(t *testing.T) {
		t.Run("cpuA", func(t *testing.T) {
			t.Run("host tag returns all values", func(t *testing.T) {
				iter, err := e.MeasurementTagValuesNoTime(context.Background(), orgs[0].org, orgs[0].bucket, "cpuA", "host", nil)
				require.NoError(t, err)
				assert.Equal(t, cursors.StringIteratorToSlice(iter), strL("0A", "AA", "CA", "DA"))
			})

			t.Run("host tag returns subset with predicate", func(t *testing.T) {
				iter, err := e.MeasurementTagValuesNoTime(context.Background(), orgs[0].org, orgs[0].bucket, "cpuA", "host", influxql.MustParseExpr("os = 'macOS'"))
				require.NoError(t, err)
				assert.Equal(t, cursors.StringIteratorToSlice(iter), strL("DA"))
			})
		})

		t.Run("memA", func(t *testing.T) {
			t.Run("host tag returns all values", func(t *testing.T) {
				iter, err := e.MeasurementTagValuesNoTime(context.Background(), orgs[0].org, orgs[0].bucket, "memA", "host", nil)
				require.NoError(t, err)
				assert.Equal(t, cursors.StringIteratorToSlice(iter), strL("DA"))
			})
			t.Run("os tag returns all values", func(t *testing.T) {
				iter, err := e.MeasurementTagValuesNoTime(context.Background(), orgs[0].org, orgs[0].bucket, "memA", "os", nil)
				require.NoError(t, err)
				assert.Equal(t, cursors.StringIteratorToSlice(iter), strL("macOS"))
			})
		})
	})

	e.MustDeleteBucketRange(orgs[0].org, orgs[0].bucket, 102, 105)

	t.Run("before snapshot after delete", func(t *testing.T) {
		t.Run("cpuA", func(t *testing.T) {
			t.Run("host tag returns all values", func(t *testing.T) {
				iter, err := e.MeasurementTagValuesNoTime(context.Background(), orgs[0].org, orgs[0].bucket, "cpuA", "host", nil)
				require.NoError(t, err)
				assert.Equal(t, cursors.StringIteratorToSlice(iter), strL("0A", "DA"))
			})

			t.Run("host tag returns subset with predicate", func(t *testing.T) {
				iter, err := e.MeasurementTagValuesNoTime(context.Background(), orgs[0].org, orgs[0].bucket, "cpuA", "host", influxql.MustParseExpr("os = 'macOS'"))
				require.NoError(t, err)
				assert.Equal(t, cursors.StringIteratorToSlice(iter), strL("DA"))
			})
		})

		t.Run("memA", func(t *testing.T) {
			t.Run("host tag returns all values", func(t *testing.T) {
				iter, err := e.MeasurementTagValuesNoTime(context.Background(), orgs[0].org, orgs[0].bucket, "memA", "host", nil)
				require.NoError(t, err)
				assert.Equal(t, cursors.StringIteratorToSlice(iter), strL("DA"))
			})
			t.Run("os tag returns all values", func(t *testing.T) {
				iter, err := e.MeasurementTagValuesNoTime(context.Background(), orgs[0].org, orgs[0].bucket, "memA", "os", nil)
				require.NoError(t, err)
				assert.Equal(t, cursors.StringIteratorToSlice(iter), strL("macOS"))
			})
		})
	})

	// send some points to TSM data
	e.MustWriteSnapshot()

	// leave some points in the cache
	e.MustWritePointsString(orgs[0].org, orgs[0].bucket, `
cpuA,host=0A,os=linux value=1.1 201
cpuA,host=AA,os=linux value=1.2 202
cpuA,host=AA,os=linux value=1.3 204
cpuA,host=BA,os=macOS value=1.3 204
cpuA,host=BA,os=macOS value=1.3 205
cpuA,host=EA,os=linux value=1.3 206
memA,host=EA,os=linux value=1.3 201`)
	e.MustWritePointsString(orgs[1].org, orgs[1].bucket, `
cpuB,host=0B,os=linux value=1.1 201
cpuB,host=AB,os=linux value=1.2 202
cpuB,host=AB,os=linux value=1.3 204
cpuB,host=BB,os=linux value=1.3 204
cpuB,host=BB,os=linux value=1.3 205
cpuB,host=EB,os=macOS value=1.3 206
memB,host=EB,os=macOS value=1.3 201`)

	t.Run("after snapshot", func(t *testing.T) {
		t.Run("cpuA", func(t *testing.T) {
			t.Run("host tag returns all values", func(t *testing.T) {
				iter, err := e.MeasurementTagValuesNoTime(context.Background(), orgs[0].org, orgs[0].bucket, "cpuA", "host", nil)
				require.NoError(t, err)
				assert.Equal(t, cursors.StringIteratorToSlice(iter), strL("0A", "AA", "BA", "DA", "EA"))
			})

			t.Run("host tag returns subset with predicate", func(t *testing.T) {
				iter, err := e.MeasurementTagValuesNoTime(context.Background(), orgs[0].org, orgs[0].bucket, "cpuA", "host", influxql.MustParseExpr("os = 'macOS'"))
				require.NoError(t, err)
				assert.Equal(t, cursors.StringIteratorToSlice(iter), strL("BA", "DA"))
			})
		})
	})

	e.MustDeleteBucketRange(orgs[0].org, orgs[0].bucket, 0, 1000)

	t.Run("returns no data after deleting everything", func(t *testing.T) {
		iter, err := e.MeasurementTagValuesNoTime(context.Background(), orgs[0].org, orgs[0].bucket, "cpuA", "host", nil)
		require.NoError(t, err)
		assert.Equal(t, cursors.StringIteratorToSlice(iter), strL())
	})
}

func TestEngine_MeasurementFieldsNoTime(t *testing.T) {
	e, err := NewEngine(tsm1.NewConfig(), t)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	orgs := []struct {
		org, bucket influxdb.ID
	}{
		{
			org:    0x5020,
			bucket: 0x5100,
		},
		{
			org:    0x6000,
			bucket: 0x6100,
		},
	}

	// this org will require escaping the 0x20 byte
	e.MustWritePointsString(orgs[0].org, orgs[0].bucket, `
m00,tag00=v00,tag10=v10 i=1i 101
m00,tag00=v00,tag10=v11 i=1i 102
m00,tag00=v00,tag10=v12 f=1  101
m00,tag00=v00,tag10=v13 i=1i 108
m00,tag00=v00,tag10=v14 f=1  109
m00,tag00=v00,tag10=v15 i=1i 109
m01,tag00=v00,tag10=v10 b=true 101
`)
	e.MustWritePointsString(orgs[1].org, orgs[1].bucket, `
m10,foo=v barF=50 101
`)

	fldL := func(t *testing.T, kv ...interface{}) []cursors.MeasurementField {
		t.Helper()
		if len(kv)&1 == 1 {
			panic("uneven kv slice")
		}

		res := make([]cursors.MeasurementField, 0, len(kv)/2)
		for i := 0; i < len(kv); i += 2 {
			res = append(res, cursors.MeasurementField{
				Key:  kv[i].(string),
				Type: kv[i+1].(cursors.FieldType),
			})
		}
		return res
	}

	t.Run("first writes", func(t *testing.T) {
		t.Run("m00 no predicate", func(t *testing.T) {
			iter, err := e.MeasurementFieldsNoTime(context.Background(), orgs[0].org, orgs[0].bucket, "m00", nil)
			require.NoError(t, err)
			assert.Equal(t, cursors.MeasurementFieldsIteratorFlatMap(iter), fldL(t, "f", cursors.Float, "i", cursors.Integer))
		})

		t.Run("m00 with predicate", func(t *testing.T) {
			iter, err := e.MeasurementFieldsNoTime(context.Background(), orgs[0].org, orgs[0].bucket, "m00", influxql.MustParseExpr("tag10 = 'v15'"))
			require.NoError(t, err)
			assert.Equal(t, cursors.MeasurementFieldsIteratorFlatMap(iter), fldL(t, "i", cursors.Integer))
		})

		t.Run("m01 no predicate", func(t *testing.T) {
			iter, err := e.MeasurementFieldsNoTime(context.Background(), orgs[0].org, orgs[0].bucket, "m01", nil)
			require.NoError(t, err)
			assert.Equal(t, cursors.MeasurementFieldsIteratorFlatMap(iter), fldL(t, "b", cursors.Boolean))
		})
	})

	// change type of field i (which is not expected, and won't be supported in the future)
	e.MustWritePointsString(orgs[0].org, orgs[0].bucket, `
m00,tag00=v00,tag10=v22 f=1  201
m00,tag00=v00,tag10=v21 i="s" 202
m00,tag00=v00,tag10=v20 b=true 210
`)

	t.Run("i is still integer", func(t *testing.T) {
		iter, err := e.MeasurementFieldsNoTime(context.Background(), orgs[0].org, orgs[0].bucket, "m00", nil)
		require.NoError(t, err)
		assert.Equal(t, cursors.MeasurementFieldsIteratorFlatMap(iter), fldL(t, "b", cursors.Boolean, "f", cursors.Float, "i", cursors.Integer))
	})

	// delete earlier data
	e.MustDeleteBucketRange(orgs[0].org, orgs[0].bucket, 0, 200)

	t.Run("i is now a string", func(t *testing.T) {
		iter, err := e.MeasurementFieldsNoTime(context.Background(), orgs[0].org, orgs[0].bucket, "m00", nil)
		require.NoError(t, err)
		assert.Equal(t, cursors.MeasurementFieldsIteratorFlatMap(iter), fldL(t, "b", cursors.Boolean, "f", cursors.Float, "i", cursors.String))
	})
}

func TestEngine_MeasurementTagKeysNoTime(t *testing.T) {
	e, err := NewEngine(tsm1.NewConfig(), t)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	orgs := []struct {
		org, bucket influxdb.ID
	}{
		{
			org:    0x5020,
			bucket: 0x5100,
		},
		{
			org:    0x6000,
			bucket: 0x6100,
		},
	}

	// this org will require escaping the 0x20 byte
	e.MustWritePointsString(orgs[0].org, orgs[0].bucket, `
cpuA,host=0A,os=linux value=1.1 101
cpuA,host=AA,os=linux value=1.2 102
cpuA,host=AA,os=linux value=1.3 104
cpuA,host=CA,os=linux value=1.3 104
cpuA,host=CA,os=linux value=1.3 105
cpuA,host=DA,os=macOS,release=10.15 value=1.3 106
memA,host=DA,os=macOS,release=10.15 value=1.3 101`)
	e.MustWritePointsString(orgs[1].org, orgs[1].bucket, `
cpuB,host=0B,os=linux value=1.1 101
cpuB,host=AB,os=linux value=1.2 102
cpuB,host=AB,os=linux value=1.3 104
cpuB,host=CB,os=linux value=1.3 104
cpuB,host=CB,os=linux value=1.3 105
cpuB,host=DB,os=macOS,release=10.15 value=1.3 106
memB,host=DB,os=macOS,release=10.15 value=1.3 101`)

	t.Run("before snapshot", func(t *testing.T) {
		t.Run("cpuA", func(t *testing.T) {
			t.Run("measurement name returns all keys", func(t *testing.T) {
				iter, err := e.MeasurementTagKeysNoTime(context.Background(), orgs[0].org, orgs[0].bucket, "cpuA", nil)
				require.NoError(t, err)
				assert.Equal(t, strL("\x00", "host", "os", "release", "\xff"), cursors.StringIteratorToSlice(iter))
			})
		})
	})

	e.MustDeleteBucketRange(orgs[0].org, orgs[0].bucket, 102, 105)

	t.Run("before snapshot after delete", func(t *testing.T) {
		t.Run("cpuA", func(t *testing.T) {
			t.Run("measurement name returns all keys", func(t *testing.T) {
				iter, err := e.MeasurementTagKeysNoTime(context.Background(), orgs[0].org, orgs[0].bucket, "cpuA", nil)
				require.NoError(t, err)
				assert.Equal(t, strL("\x00", "host", "os", "release", "\xff"), cursors.StringIteratorToSlice(iter))
			})

			t.Run("measurement name returns subset with predicate", func(t *testing.T) {
				iter, err := e.MeasurementTagKeysNoTime(context.Background(), orgs[0].org, orgs[0].bucket, "cpuA", influxql.MustParseExpr("os = 'linux'"))
				require.NoError(t, err)
				assert.Equal(t, strL("\x00", "host", "os", "\xff"), cursors.StringIteratorToSlice(iter))
			})
		})
	})

	// send some points to TSM data
	e.MustWriteSnapshot()

	// leave some points in the cache
	e.MustWritePointsString(orgs[0].org, orgs[0].bucket, `
cpuA,host=0A,os=linux value=1.1 201
cpuA,host=AA,os=linux value=1.2 202
cpuA,host=AA,os=linux value=1.3 204
cpuA,host=BA,os=macOS,release=10.15,shell=zsh value=1.3 204
cpuA,host=BA,os=macOS,release=10.15,shell=zsh value=1.3 205
cpuA,host=EA,os=linux value=1.3 206
memA,host=EA,os=linux value=1.3 201`)
	e.MustWritePointsString(orgs[1].org, orgs[1].bucket, `
cpuB,host=0B,os=linux value=1.1 201
cpuB,host=AB,os=linux value=1.2 202
cpuB,host=AB,os=linux value=1.3 204
cpuB,host=BB,os=linux value=1.3 204
cpuB,host=BB,os=linux value=1.3 205
cpuB,host=EB,os=macOS,release=10.15,shell=zsh value=1.3 206
memB,host=EB,os=macOS,release=10.15,shell=zsh value=1.3 201`)

	t.Run("after snapshot", func(t *testing.T) {
		t.Run("cpuA", func(t *testing.T) {
			t.Run("measurement name returns all keys", func(t *testing.T) {
				iter, err := e.MeasurementTagKeysNoTime(context.Background(), orgs[0].org, orgs[0].bucket, "cpuA", nil)
				require.NoError(t, err)
				assert.Equal(t, strL("\x00", "host", "os", "release", "shell", "\xff"), cursors.StringIteratorToSlice(iter))
			})

			t.Run("measurement name returns subset with predicate", func(t *testing.T) {
				iter, err := e.MeasurementTagKeysNoTime(context.Background(), orgs[0].org, orgs[0].bucket, "cpuA", influxql.MustParseExpr("os = 'linux'"))
				require.NoError(t, err)
				assert.Equal(t, strL("\x00", "host", "os", "\xff"), cursors.StringIteratorToSlice(iter))
			})

			t.Run("measurement name returns subset with composite predicate", func(t *testing.T) {
				iter, err := e.MeasurementTagKeysNoTime(context.Background(), orgs[0].org, orgs[0].bucket, "cpuA", influxql.MustParseExpr("os = 'linux' AND host = 'AA'"))
				require.NoError(t, err)
				assert.Equal(t, strL("\x00", "host", "os", "\xff"), cursors.StringIteratorToSlice(iter))
			})

			t.Run("measurement name returns no results with bad predicate", func(t *testing.T) {
				iter, err := e.MeasurementTagKeysNoTime(context.Background(), orgs[0].org, orgs[0].bucket, "cpuA", influxql.MustParseExpr("os = 'darwin'"))
				require.NoError(t, err)
				assert.Equal(t, strL(), cursors.StringIteratorToSlice(iter))
			})

			t.Run("bad measurement name returns no results", func(t *testing.T) {
				iter, err := e.MeasurementTagKeysNoTime(context.Background(), orgs[0].org, orgs[0].bucket, "cpuC", nil)
				require.NoError(t, err)
				assert.Equal(t, strL(), cursors.StringIteratorToSlice(iter))
			})
		})
	})

	e.MustDeleteBucketRange(orgs[0].org, orgs[0].bucket, 0, 1000)

	t.Run("returns no data after deleting everything", func(t *testing.T) {
		iter, err := e.MeasurementTagKeysNoTime(context.Background(), orgs[0].org, orgs[0].bucket, "cpuA", nil)
		require.NoError(t, err)
		assert.Equal(t, strL(), cursors.StringIteratorToSlice(iter))
	})
}

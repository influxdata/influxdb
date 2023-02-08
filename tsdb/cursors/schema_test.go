package cursors_test

import (
	"math/rand"
	"sort"
	"testing"

	"github.com/influxdata/influxdb/v2/pkg/testing/assert"
	"github.com/influxdata/influxdb/v2/tsdb/cursors"
)

// Verifies FieldType precedence behavior is equivalent to influxql.DataType#LessThan
func TestFieldTypeDataTypePrecedenceEquivalence(t *testing.T) {
	var fieldTypes = []cursors.FieldType{
		cursors.Float,
		cursors.Integer,
		cursors.Unsigned,
		cursors.Boolean,
		cursors.String,
		cursors.Undefined,
	}

	for _, fta := range fieldTypes {
		for _, ftb := range fieldTypes {
			if fta == ftb {
				continue
			}

			got := fta.IsLower(ftb)
			exp := cursors.FieldTypeToDataType(fta).LessThan(cursors.FieldTypeToDataType(ftb))
			assert.Equal(t, got, exp, "failed %s.LessThan(%s)", fta.String(), ftb.String())
		}
	}
}

// Verifies sorting behavior of MeasurementFieldSlice
func TestMeasurementFieldSliceSort(t *testing.T) {
	mfs := func(d ...cursors.MeasurementField) cursors.MeasurementFieldSlice {
		return d
	}

	mf := func(key string, timestamp int64, ft cursors.FieldType) cursors.MeasurementField {
		return cursors.MeasurementField{
			Key:       key,
			Type:      ft,
			Timestamp: timestamp,
		}
	}

	fltF := func(key string, ts int64) cursors.MeasurementField {
		return mf(key, ts, cursors.Float)
	}
	intF := func(key string, ts int64) cursors.MeasurementField {
		return mf(key, ts, cursors.Integer)
	}
	strF := func(key string, ts int64) cursors.MeasurementField {
		return mf(key, ts, cursors.String)
	}
	blnF := func(key string, ts int64) cursors.MeasurementField {
		return mf(key, ts, cursors.Boolean)
	}

	cases := []struct {
		name string
		in   cursors.MeasurementFieldSlice
		exp  cursors.MeasurementFieldSlice
	}{
		{
			name: "keys:diff types:same ts:same",
			in: mfs(
				fltF("bbb", 0),
				fltF("aaa", 0),
				fltF("ccc", 0),
			),
			exp: mfs(
				fltF("aaa", 0),
				fltF("bbb", 0),
				fltF("ccc", 0),
			),
		},
		{
			name: "keys:same types:same ts:diff",
			in: mfs(
				fltF("aaa", 10),
				fltF("ccc", 20),
				fltF("aaa", 0),
				fltF("ccc", 0),
			),
			exp: mfs(
				fltF("aaa", 0),
				fltF("aaa", 10),
				fltF("ccc", 0),
				fltF("ccc", 20),
			),
		},
		{
			name: "keys:same types:diff ts:same",
			in: mfs(
				strF("aaa", 0),
				intF("aaa", 0),
				fltF("aaa", 0),
				blnF("aaa", 0),
			),
			exp: mfs(
				blnF("aaa", 0),
				strF("aaa", 0),
				intF("aaa", 0),
				fltF("aaa", 0),
			),
		},
		{
			name: "keys:same types:diff ts:diff",
			in: mfs(
				strF("aaa", 20),
				intF("aaa", 10),
				fltF("aaa", 0),
				blnF("aaa", 30),
			),
			exp: mfs(
				fltF("aaa", 0),
				intF("aaa", 10),
				strF("aaa", 20),
				blnF("aaa", 30),
			),
		},
		{
			name: "keys:diff types:diff ts:diff",
			in: mfs(
				intF("ccc", 10),
				blnF("fff", 30),
				strF("aaa", 20),
				fltF("ddd", 0),
			),
			exp: mfs(
				strF("aaa", 20),
				intF("ccc", 10),
				fltF("ddd", 0),
				blnF("fff", 30),
			),
		},
		{
			name: "keys:many types:many ts:same",
			in: mfs(
				intF("ccc", 10),
				blnF("fff", 30),
				strF("aaa", 20),
				fltF("ddd", 0),
				fltF("ccc", 10),
				strF("fff", 30),
				intF("aaa", 20),
				blnF("ddd", 0),
			),
			exp: mfs(
				strF("aaa", 20),
				intF("aaa", 20),
				intF("ccc", 10),
				fltF("ccc", 10),
				blnF("ddd", 0),
				fltF("ddd", 0),
				blnF("fff", 30),
				strF("fff", 30),
			),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.in

			// randomize order using fixed seed to
			// ensure tests are deterministic on a given platform
			seededRand := rand.New(rand.NewSource(100))
			for i := 0; i < 5; i++ {
				seededRand.Shuffle(len(got), func(i, j int) {
					got[i], got[j] = got[j], got[i]
				})

				sort.Sort(got)
				assert.Equal(t, got, tc.exp, "failed at index", i)
			}
		})
	}
}

func TestMeasurementFieldSlice_UniqueByKey(t *testing.T) {
	mfs := func(d ...cursors.MeasurementField) cursors.MeasurementFieldSlice {
		return d
	}

	mf := func(key string, timestamp int64, ft cursors.FieldType) cursors.MeasurementField {
		return cursors.MeasurementField{
			Key:       key,
			Type:      ft,
			Timestamp: timestamp,
		}
	}

	fltF := func(key string, ts int64) cursors.MeasurementField {
		return mf(key, ts, cursors.Float)
	}

	t.Run("multiple start end", func(t *testing.T) {
		got := mfs(
			fltF("aaa", 0),
			fltF("aaa", 10),
			fltF("bbb", 10),
			fltF("ccc", 10),
			fltF("ccc", 20),
		)

		exp := mfs(
			fltF("aaa", 0),
			fltF("bbb", 10),
			fltF("ccc", 10),
		)

		got.UniqueByKey()
		assert.Equal(t, got, exp)
	})

	t.Run("multiple at end", func(t *testing.T) {
		got := mfs(
			fltF("aaa", 0),
			fltF("bbb", 10),
			fltF("ccc", 10),
			fltF("ccc", 20),
			fltF("ccc", 30),
		)

		exp := mfs(
			fltF("aaa", 0),
			fltF("bbb", 10),
			fltF("ccc", 10),
		)

		got.UniqueByKey()
		assert.Equal(t, got, exp)
	})

	t.Run("no duplicates many", func(t *testing.T) {
		got := mfs(
			fltF("aaa", 0),
			fltF("bbb", 10),
			fltF("ccc", 20),
		)

		exp := mfs(
			fltF("aaa", 0),
			fltF("bbb", 10),
			fltF("ccc", 20),
		)

		got.UniqueByKey()
		assert.Equal(t, got, exp)
	})

	t.Run("no duplicates two elements", func(t *testing.T) {
		got := mfs(
			fltF("aaa", 0),
			fltF("bbb", 10),
		)

		exp := mfs(
			fltF("aaa", 0),
			fltF("bbb", 10),
		)

		got.UniqueByKey()
		assert.Equal(t, got, exp)
	})

	t.Run("duplicates one key", func(t *testing.T) {
		got := mfs(
			fltF("aaa", 0),
			fltF("aaa", 10),
			fltF("aaa", 10),
			fltF("aaa", 10),
			fltF("aaa", 10),
			fltF("aaa", 10),
		)

		exp := mfs(
			fltF("aaa", 0),
		)

		got.UniqueByKey()
		assert.Equal(t, got, exp)
	})

	t.Run("one element", func(t *testing.T) {
		got := mfs(
			fltF("aaa", 0),
		)

		exp := mfs(
			fltF("aaa", 0),
		)

		got.UniqueByKey()
		assert.Equal(t, got, exp)
	})

	t.Run("empty", func(t *testing.T) {
		got := mfs()
		exp := mfs()

		got.UniqueByKey()
		assert.Equal(t, got, exp)
	})
}

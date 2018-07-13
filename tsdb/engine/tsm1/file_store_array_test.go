package tsm1_test

import (
	"context"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

func TestFileStore_Array(t *testing.T) {
	makeFile := func(d ...interface{}) keyValues {
		t.Helper()
		if len(d)&1 == 1 {
			panic("input should be even")
		}

		vals := make([]tsm1.Value, len(d)/2)
		for i := 0; i < len(d); i += 2 {
			vals[i/2] = tsm1.NewFloatValue(int64(d[i].(int)), d[i+1].(float64))
		}
		return keyValues{key: "cpu", values: vals}
	}

	// sel selects files and values from the keyValues slice
	// and used to build the expected output.
	type sel struct {
		// f is the index of the tsm file
		f int
		// i is the index of the value to select from the file
		i int
	}

	// del represents a file delete in order to generate tombstones
	type del struct {
		// f is the index of the tsm file to perform a delete.
		// Specifying -1 will perform a delete over the entire FileStore.
		f        int
		min, max int64
	}

	type read []sel

	cases := []struct {
		name    string
		data    []keyValues
		time    int64
		asc     bool
		deletes []del
		reads   []read
	}{
		{
			name: "SeekToAsc_FromStart",

			data: []keyValues{
				makeFile(0, 1.0),
				makeFile(1, 2.0),
				makeFile(2, 3.0),
			},
			time: 0,
			asc:  true,
			reads: []read{
				[]sel{{0, 0}},
			},
		},
		{
			name: "SeekToAsc_BeforeStart",

			data: []keyValues{
				makeFile(1, 1.0),
				makeFile(2, 2.0),
				makeFile(3, 3.0),
			},
			time: 0,
			asc:  true,
			reads: []read{
				[]sel{{0, 0}},
			},
		},
		{
			// Tests that seeking and reading all blocks that contain overlapping points does
			// not skip any blocks.
			name: "SeekToAsc_BeforeStart_OverlapFloat",

			data: []keyValues{
				makeFile(0, 0.0, 1, 1.0),
				makeFile(2, 2.0),
				makeFile(3, 3.0),
				makeFile(0, 4.0, 2, 7.0),
			},
			time: 0,
			asc:  true,
			reads: []read{
				[]sel{{3, 0}, {0, 1}, {3, 1}},
				[]sel{{2, 0}},
			},
		},
		{
			// Tests that blocks with a lower min time in later files are not returned
			// more than once causing unsorted results.
			name: "SeekToAsc_OverlapMinFloat",

			data: []keyValues{
				makeFile(1, 1.0, 3, 3.0),
				makeFile(2, 2.0, 4, 4.0),
				makeFile(0, 0.0, 1, 1.1),
				makeFile(2, 2.2),
			},
			time: 0,
			asc:  true,
			reads: []read{
				[]sel{{2, 0}, {2, 1}, {3, 0}, {0, 1}},
				[]sel{{1, 1}},
				[]sel{},
			},
		},
		{
			name: "SeekToAsc_Middle",

			data: []keyValues{
				makeFile(1, 1.0, 2, 2.0, 3, 3.0),
				makeFile(4, 4.0),
			},
			time: 3,
			asc:  true,
			reads: []read{
				[]sel{{0, 2}},
				[]sel{{1, 0}},
			},
		},
		{
			name: "SeekToAsc_End",

			data: []keyValues{
				makeFile(0, 1.0),
				makeFile(1, 2.0),
				makeFile(2, 3.0),
			},
			time: 2,
			asc:  true,
			reads: []read{
				[]sel{{2, 0}},
			},
		},

		// descending cursor tests
		{
			name: "SeekToDesc_FromStart",

			data: []keyValues{
				makeFile(0, 1.0),
				makeFile(1, 2.0),
				makeFile(2, 3.0),
			},
			time: 0,
			asc:  false,
			reads: []read{
				[]sel{{0, 0}},
			},
		},
		{
			name: "SeekToDesc_Duplicate",

			data: []keyValues{
				makeFile(0, 4.0),
				makeFile(0, 1.0),
				makeFile(2, 2.0),
				makeFile(2, 3.0),
			},
			time: 2,
			asc:  false,
			reads: []read{
				[]sel{{3, 0}},
				[]sel{{1, 0}},
			},
		},
		{
			name: "SeekToDesc_OverlapMaxFloat",

			data: []keyValues{
				makeFile(1, 1.0, 3, 3.0),
				makeFile(2, 2.0, 4, 4.0),
				makeFile(0, 0.0, 1, 1.1),
				makeFile(2, 2.2),
			},
			time: 5,
			asc:  false,
			reads: []read{
				[]sel{{3, 0}, {0, 1}, {1, 1}},
				[]sel{{2, 0}, {2, 1}},
			},
		},
		{
			name: "SeekToDesc_AfterEnd",

			data: []keyValues{
				makeFile(1, 1.0),
				makeFile(2, 2.0),
				makeFile(3, 3.0),
			},
			time: 4,
			asc:  false,
			reads: []read{
				[]sel{{2, 0}},
			},
		},
		{
			name: "SeekToDesc_AfterEnd_OverlapFloat",

			data: []keyValues{
				makeFile(8, 0.0, 9, 1.0),
				makeFile(2, 2.0),
				makeFile(3, 3.0),
				makeFile(3, 4.0, 7, 7.0),
			},
			time: 10,
			asc:  false,
			reads: []read{
				[]sel{{0, 0}, {0, 1}},
				[]sel{{3, 0}, {3, 1}},
				[]sel{{1, 0}},
				[]sel{},
			},
		},
		{
			name: "SeekToDesc_Middle",

			data: []keyValues{
				makeFile(1, 1.0),
				makeFile(2, 2.0, 3, 3.0, 4, 4.0),
			},
			time: 3,
			asc:  false,
			reads: []read{
				[]sel{{1, 0}, {1, 1}},
			},
		},
		{
			name: "SeekToDesc_End",

			data: []keyValues{
				makeFile(0, 1.0),
				makeFile(1, 2.0),
				makeFile(2, 3.0),
			},
			time: 2,
			asc:  false,
			reads: []read{
				[]sel{{2, 0}},
			},
		},

		// tombstone tests
		{
			name: "TombstoneRange",

			data: []keyValues{
				makeFile(0, 1.0),
				makeFile(1, 2.0),
				makeFile(2, 3.0),
			},
			time: 0,
			asc:  true,
			deletes: []del{
				{-1, 1, 1},
			},
			reads: []read{
				[]sel{{0, 0}},
				[]sel{{2, 0}},
				[]sel{},
			},
		},
		{
			name: "TombstoneRange_PartialFirst",

			data: []keyValues{
				makeFile(0, 0.0, 1, 1.0),
				makeFile(2, 2.0),
			},
			time: 0,
			asc:  true,
			deletes: []del{
				{0, 1, 3},
			},
			reads: []read{
				[]sel{{0, 0}},
				[]sel{{1, 0}},
				[]sel{},
			},
		},
		{
			name: "TombstoneRange_PartialFloat",

			data: []keyValues{
				makeFile(0, 0.0, 1, 1.0, 2, 2.0),
			},
			time: 0,
			asc:  true,
			deletes: []del{
				{-1, 1, 1},
			},
			reads: []read{
				[]sel{{0, 0}, {0, 2}},
				[]sel{},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := MustTempDir()
			defer os.RemoveAll(dir)
			fs := tsm1.NewFileStore(dir)

			files, err := newFiles(dir, tc.data...)
			if err != nil {
				t.Fatalf("unexpected error creating files: %v", err)
			}

			for _, del := range tc.deletes {
				if del.f > -1 {
					// Delete part of the block in the first file.
					r := MustOpenTSMReader(files[del.f])
					r.DeleteRange([][]byte{[]byte("cpu")}, del.min, del.max)
				}
			}

			fs.Replace(nil, files)

			for _, del := range tc.deletes {
				if del.f == -1 {
					if err := fs.DeleteRange([][]byte{[]byte("cpu")}, del.min, del.max); err != nil {
						t.Fatalf("unexpected error delete range: %v", err)
					}
				}
			}

			buf := tsdb.NewFloatArrayLen(1000)
			c := fs.KeyCursor(context.Background(), []byte("cpu"), tc.time, tc.asc)

			for i, read := range tc.reads {
				// Search for an entry that exists in the second file
				values, err := c.ReadFloatArrayBlock(buf)
				if err != nil {
					t.Fatalf("read %d failed: unexpected error reading values: %v", i, err)
				}

				exp := &tsdb.FloatArray{}
				for _, s := range read {
					vals := tc.data[s.f].values
					exp.Timestamps = append(exp.Timestamps, vals[s.i].UnixNano())
					exp.Values = append(exp.Values, vals[s.i].Value().(float64))
				}

				if len(read) == 0 {
					exp = tsdb.NewFloatArrayLen(0)
				}

				if !cmp.Equal(values, exp) {
					t.Fatalf("read %d failed: unexpected values -got/+exp\n%s", i, cmp.Diff(values, exp))
				}

				c.Next()
			}
		})
	}
}

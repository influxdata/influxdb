package tsm1

import (
	"fmt"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/influxdata/influxdb/v2/tsdb/cursors"
)

func TestTimeRangeIterator(t *testing.T) {
	tsm := mustWriteTSM(
		bucket{
			org:    0x50,
			bucket: 0x60,
			w: writes(
				mw("cpu",
					kw("tag0=val0",
						vals(tvi(1000, 1), tvi(1010, 2), tvi(1020, 3)),
						vals(tvi(2000, 1), tvi(2010, 2), tvi(2020, 3)),
					),
					kw("tag0=val1",
						vals(tvi(2000, 1), tvi(2010, 2), tvi(2020, 3)),
						vals(tvi(3000, 1), tvi(3010, 2), tvi(3020, 3)),
					),
				),
			),
		},

		bucket{
			org:    0x51,
			bucket: 0x61,
			w: writes(
				mw("mem",
					kw("tag0=val0",
						vals(tvi(1000, 1), tvi(1010, 2), tvi(1020, 3)),
						vals(tvi(2000, 1), tvi(2010, 2), tvi(2020, 3)),
					),
					kw("tag0=val1",
						vals(tvi(1000, 1), tvi(1010, 2), tvi(1020, 3)),
						vals(tvi(2000, 1)),
					),
					kw("tag0=val2",
						vals(tvi(2000, 1), tvi(2010, 2), tvi(2020, 3)),
						vals(tvi(3000, 1), tvi(3010, 2), tvi(3020, 3)),
					),
				),
			),
		},
	)
	defer tsm.RemoveAll()

	orgBucket := func(org, bucket uint) []byte {
		n := tsdb.EncodeName(influxdb.ID(org), influxdb.ID(bucket))
		return n[:]
	}

	type args struct {
		min int64
		max int64
	}

	type res struct {
		k       string
		hasData bool
	}

	EXP := func(r ...interface{}) (rr []res) {
		for i := 0; i+1 < len(r); i += 2 {
			rr = append(rr, res{k: r[i].(string), hasData: r[i+1].(bool)})
		}
		return
	}

	type test struct {
		name     string
		args     args
		exp      []res
		expStats cursors.CursorStats
	}

	type bucketTest struct {
		org, bucket uint
		m           string
		tests       []test
	}

	r := tsm.TSMReader()

	runTests := func(name string, tests []bucketTest) {
		t.Run(name, func(t *testing.T) {
			for _, bt := range tests {
				key := orgBucket(bt.org, bt.bucket)
				t.Run(fmt.Sprintf("0x%x-0x%x", bt.org, bt.bucket), func(t *testing.T) {
					for _, tt := range bt.tests {
						t.Run(tt.name, func(t *testing.T) {
							iter := r.TimeRangeIterator(key, tt.args.min, tt.args.max)
							count := 0
							for i, exp := range tt.exp {
								if !iter.Next() {
									t.Errorf("Next(%d): expected true", i)
								}

								expKey := makeKey(influxdb.ID(bt.org), influxdb.ID(bt.bucket), bt.m, exp.k)
								if got := iter.Key(); !cmp.Equal(got, expKey) {
									t.Errorf("Key(%d): -got/+exp\n%v", i, cmp.Diff(got, expKey))
								}

								if got := iter.HasData(); got != exp.hasData {
									t.Errorf("HasData(%d): -got/+exp\n%v", i, cmp.Diff(got, exp.hasData))
								}
								count++
							}
							if count != len(tt.exp) {
								t.Errorf("count: -got/+exp\n%v", cmp.Diff(count, len(tt.exp)))
							}

							if got := iter.Stats(); !cmp.Equal(got, tt.expStats) {
								t.Errorf("Stats: -got/+exp\n%v", cmp.Diff(got, tt.expStats))
							}
						})

					}
				})
			}
		})
	}

	runTests("before delete", []bucketTest{
		{
			org:    0x50,
			bucket: 0x60,
			m:      "cpu",
			tests: []test{
				{
					name: "cover file",
					args: args{
						min: 900,
						max: 10000,
					},
					exp:      EXP("tag0=val0", true, "tag0=val1", true),
					expStats: cursors.CursorStats{ScannedValues: 0, ScannedBytes: 0},
				},
				{
					name: "within block",
					args: args{
						min: 2001,
						max: 2011,
					},
					exp:      EXP("tag0=val0", true, "tag0=val1", true),
					expStats: cursors.CursorStats{ScannedValues: 6, ScannedBytes: 48},
				},
				{
					name: "to_2999",
					args: args{
						min: 0,
						max: 2999,
					},
					exp:      EXP("tag0=val0", true, "tag0=val1", true),
					expStats: cursors.CursorStats{ScannedValues: 0, ScannedBytes: 0},
				},
				{
					name: "intersects block",
					args: args{
						min: 1500,
						max: 2500,
					},
					exp:      EXP("tag0=val0", true, "tag0=val1", true),
					expStats: cursors.CursorStats{ScannedValues: 0, ScannedBytes: 0},
				},
			},
		},

		{
			org:    0x51,
			bucket: 0x61,
			m:      "mem",
			tests: []test{
				{
					name: "cover file",
					args: args{
						min: 900,
						max: 10000,
					},
					exp:      EXP("tag0=val0", true, "tag0=val1", true, "tag0=val2", true),
					expStats: cursors.CursorStats{ScannedValues: 0, ScannedBytes: 0},
				},
				{
					name: "within block",
					args: args{
						min: 2001,
						max: 2011,
					},
					exp:      EXP("tag0=val0", true, "tag0=val1", false, "tag0=val2", true),
					expStats: cursors.CursorStats{ScannedValues: 6, ScannedBytes: 48},
				},
				{
					name: "1000_2999",
					args: args{
						min: 1000,
						max: 2500,
					},
					exp:      EXP("tag0=val0", true, "tag0=val1", true, "tag0=val2", true),
					expStats: cursors.CursorStats{ScannedValues: 0, ScannedBytes: 0},
				},
			},
		},
	})

	tsm.MustDeletePrefix(orgBucket(0x50, 0x60), 0, 2999)
	tsm.MustDelete(makeKey(0x51, 0x61, "mem", "tag0=val0"))
	tsm.MustDeleteRange(2000, 2999,
		makeKey(0x51, 0x61, "mem", "tag0=val1"),
		makeKey(0x51, 0x61, "mem", "tag0=val2"),
	)

	runTests("after delete", []bucketTest{
		{
			org:    0x50,
			bucket: 0x60,
			m:      "cpu",
			tests: []test{
				{
					name: "cover file",
					args: args{
						min: 900,
						max: 10000,
					},
					exp:      EXP("tag0=val1", true),
					expStats: cursors.CursorStats{ScannedValues: 6, ScannedBytes: 48},
				},
				{
					name: "within block",
					args: args{
						min: 2001,
						max: 2011,
					},
					exp:      nil,
					expStats: cursors.CursorStats{ScannedValues: 0, ScannedBytes: 0},
				},
				{
					name: "to_2999",
					args: args{
						min: 0,
						max: 2999,
					},
					exp:      EXP("tag0=val1", false),
					expStats: cursors.CursorStats{ScannedValues: 3, ScannedBytes: 24},
				},
				{
					name: "intersects block",
					args: args{
						min: 1500,
						max: 2500,
					},
					exp:      EXP("tag0=val1", false),
					expStats: cursors.CursorStats{ScannedValues: 3, ScannedBytes: 24},
				},
				{
					name: "beyond all tombstones",
					args: args{
						min: 3000,
						max: 4000,
					},
					exp:      EXP("tag0=val1", true),
					expStats: cursors.CursorStats{ScannedValues: 0, ScannedBytes: 0},
				},
			},
		},

		{
			org:    0x51,
			bucket: 0x61,
			m:      "mem",
			tests: []test{
				{
					name: "cover file",
					args: args{
						min: 900,
						max: 10000,
					},
					exp:      EXP("tag0=val1", true, "tag0=val2", true),
					expStats: cursors.CursorStats{ScannedValues: 9, ScannedBytes: 72},
				},
				{
					name: "within block",
					args: args{
						min: 2001,
						max: 2011,
					},
					exp:      EXP("tag0=val1", false, "tag0=val2", false),
					expStats: cursors.CursorStats{ScannedValues: 3, ScannedBytes: 24},
				},
				{
					name: "1000_2500",
					args: args{
						min: 1000,
						max: 2500,
					},
					exp:      EXP("tag0=val1", true, "tag0=val2", false),
					expStats: cursors.CursorStats{ScannedValues: 6, ScannedBytes: 48},
				},
			},
		},
	})
}

func TestExcludeEntries(t *testing.T) {
	entries := func(ts ...int64) (e []IndexEntry) {
		for i := 0; i+1 < len(ts); i += 2 {
			e = append(e, IndexEntry{MinTime: ts[i], MaxTime: ts[i+1]})
		}
		return
	}

	eq := func(a, b []IndexEntry) bool {
		if len(a) == 0 && len(b) == 0 {
			return true
		}
		return cmp.Equal(a, b)
	}

	type args struct {
		e   []IndexEntry
		min int64
		max int64
	}
	tests := []struct {
		name string
		args args
		exp  []IndexEntry
	}{
		{
			args: args{
				e:   entries(0, 10, 12, 15, 19, 21),
				min: 11,
				max: 13,
			},
			exp: entries(12, 15),
		},
		{
			args: args{
				e:   entries(0, 10, 12, 15, 19, 21),
				min: 10,
				max: 13,
			},
			exp: entries(0, 10, 12, 15),
		},
		{
			args: args{
				e:   entries(0, 10, 12, 15, 19, 21),
				min: 12,
				max: 30,
			},
			exp: entries(12, 15, 19, 21),
		},
		{
			args: args{
				e:   entries(0, 10, 12, 15, 19, 21),
				min: 13,
				max: 20,
			},
			exp: entries(12, 15, 19, 21),
		},
		{
			args: args{
				e:   entries(0, 10, 12, 15, 19, 21),
				min: 0,
				max: 100,
			},
			exp: entries(0, 10, 12, 15, 19, 21),
		},
		{
			args: args{
				e:   entries(0, 10, 13, 15, 19, 21),
				min: 11,
				max: 12,
			},
			exp: entries(),
		},
		{
			args: args{
				e:   entries(12, 15, 19, 21),
				min: 0,
				max: 9,
			},
			exp: entries(),
		},
		{
			args: args{
				e:   entries(12, 15, 19, 21),
				min: 22,
				max: 30,
			},
			exp: entries(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := excludeEntries(tt.args.e, TimeRange{tt.args.min, tt.args.max}); !cmp.Equal(got, tt.exp, cmp.Comparer(eq)) {
				t.Errorf("excludeEntries() -got/+exp\n%v", cmp.Diff(got, tt.exp))
			}
		})
	}
}

func TestExcludeTimeRanges(t *testing.T) {
	entries := func(ts ...int64) (e []TimeRange) {
		for i := 0; i+1 < len(ts); i += 2 {
			e = append(e, TimeRange{Min: ts[i], Max: ts[i+1]})
		}
		return
	}

	eq := func(a, b []TimeRange) bool {
		if len(a) == 0 && len(b) == 0 {
			return true
		}
		return cmp.Equal(a, b)
	}

	type args struct {
		e   []TimeRange
		min int64
		max int64
	}
	tests := []struct {
		name string
		args args
		exp  []TimeRange
	}{
		{
			args: args{
				e:   entries(0, 10, 12, 15, 19, 21),
				min: 11,
				max: 13,
			},
			exp: entries(12, 15),
		},
		{
			args: args{
				e:   entries(0, 10, 12, 15, 19, 21),
				min: 10,
				max: 13,
			},
			exp: entries(0, 10, 12, 15),
		},
		{
			args: args{
				e:   entries(0, 10, 12, 15, 19, 21),
				min: 12,
				max: 30,
			},
			exp: entries(12, 15, 19, 21),
		},
		{
			args: args{
				e:   entries(0, 10, 12, 15, 19, 21),
				min: 0,
				max: 100,
			},
			exp: entries(0, 10, 12, 15, 19, 21),
		},
		{
			args: args{
				e:   entries(0, 10, 13, 15, 19, 21),
				min: 11,
				max: 12,
			},
			exp: entries(),
		},
		{
			args: args{
				e:   entries(12, 15, 19, 21),
				min: 0,
				max: 9,
			},
			exp: entries(),
		},
		{
			args: args{
				e:   entries(12, 15, 19, 21),
				min: 22,
				max: 30,
			},
			exp: entries(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := excludeTimeRanges(tt.args.e, TimeRange{tt.args.min, tt.args.max}); !cmp.Equal(got, tt.exp, cmp.Comparer(eq)) {
				t.Errorf("excludeEntries() -got/+exp\n%v", cmp.Diff(got, tt.exp))
			}
		})
	}
}

func TestIntersectsEntries(t *testing.T) {
	entries := func(ts ...int64) (e []IndexEntry) {
		for i := 0; i+1 < len(ts); i += 2 {
			e = append(e, IndexEntry{MinTime: ts[i], MaxTime: ts[i+1]})
		}
		return
	}

	type args struct {
		e  []IndexEntry
		tr TimeRange
	}
	tests := []struct {
		name string
		args args
		exp  bool
	}{
		{
			name: "",
			args: args{
				e:  entries(5, 10, 13, 15, 19, 21, 22, 27),
				tr: TimeRange{6, 9},
			},
			exp: false,
		},
		{
			args: args{
				e:  entries(5, 10, 13, 15, 19, 21, 22, 27),
				tr: TimeRange{11, 12},
			},
			exp: false,
		},
		{
			args: args{
				e:  entries(5, 10, 13, 15, 19, 21, 22, 27),
				tr: TimeRange{2, 4},
			},
			exp: false,
		},
		{
			args: args{
				e:  entries(5, 10, 13, 15, 19, 21, 22, 27),
				tr: TimeRange{28, 40},
			},
			exp: false,
		},

		{
			args: args{
				e:  entries(5, 10, 13, 15, 19, 21, 22, 27),
				tr: TimeRange{3, 11},
			},
			exp: true,
		},
		{
			args: args{
				e:  entries(5, 10, 13, 15, 19, 21, 22, 27),
				tr: TimeRange{5, 27},
			},
			exp: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := intersectsEntry(tt.args.e, tt.args.tr); got != tt.exp {
				t.Errorf("excludeEntries() -got/+exp\n%v", cmp.Diff(got, tt.exp))
			}
		})
	}
}

type bucket struct {
	org, bucket influxdb.ID
	w           []measurementWrite
}

func writes(w ...measurementWrite) []measurementWrite {
	return w
}

type measurementWrite struct {
	m string
	w []keyWrite
}

func mw(m string, w ...keyWrite) measurementWrite {
	return measurementWrite{m, w}
}

type keyWrite struct {
	k string
	w []Values
}

func kw(k string, w ...Values) keyWrite { return keyWrite{k, w} }
func vals(tv ...Value) Values           { return tv }
func tvi(ts int64, v int64) Value       { return NewIntegerValue(ts, v) }

type tsmState struct {
	dir  string
	file string
	r    *TSMReader
}

const fieldName = "v"

func makeKey(org, bucket influxdb.ID, m string, k string) []byte {
	name := tsdb.EncodeName(org, bucket)
	line := string(m) + "," + k
	tags := make(models.Tags, 1)
	tags[0] = models.NewTag(models.MeasurementTagKeyBytes, []byte(m))
	tags = append(tags, models.ParseTags([]byte(line))...)
	tags = append(tags, models.NewTag(models.FieldKeyTagKeyBytes, []byte(fieldName)))
	return SeriesFieldKeyBytes(string(models.MakeKey(name[:], tags)), fieldName)
}

func mustWriteTSM(writes ...bucket) (s *tsmState) {
	dir := mustTempDir()
	defer func() {
		if s == nil {
			_ = os.RemoveAll(dir)
		}
	}()

	f := mustTempFile(dir)

	w, err := NewTSMWriter(f)
	if err != nil {
		panic(fmt.Sprintf("unexpected error creating writer: %v", err))
	}

	for _, ob := range writes {
		for _, mw := range ob.w {
			for _, kw := range mw.w {
				key := makeKey(ob.org, ob.bucket, mw.m, kw.k)
				for _, vw := range kw.w {
					if err := w.Write(key, vw); err != nil {
						panic(fmt.Sprintf("Write failed: %v", err))
					}
				}
			}
		}
	}

	if err := w.WriteIndex(); err != nil {
		panic(fmt.Sprintf("WriteIndex: %v", err))
	}

	if err := w.Close(); err != nil {
		panic(fmt.Sprintf("Close: %v", err))
	}

	fd, err := os.Open(f.Name())
	if err != nil {
		panic(fmt.Sprintf("os.Open: %v", err))
	}

	r, err := NewTSMReader(fd)
	if err != nil {
		panic(fmt.Sprintf("NewTSMReader: %v", err))
	}

	return &tsmState{
		dir:  dir,
		file: f.Name(),
		r:    r,
	}
}

func (s *tsmState) TSMReader() *TSMReader {
	return s.r
}

func (s *tsmState) RemoveAll() {
	_ = os.RemoveAll(s.dir)
}

func (s *tsmState) MustDeletePrefix(key []byte, min, max int64) {
	err := s.r.DeletePrefix(key, min, max, nil, nil)
	if err != nil {
		panic(fmt.Sprintf("DeletePrefix: %v", err))
	}
}

func (s *tsmState) MustDelete(keys ...[]byte) {
	err := s.r.Delete(keys)
	if err != nil {
		panic(fmt.Sprintf("Delete: %v", err))
	}
}

func (s *tsmState) MustDeleteRange(min, max int64, keys ...[]byte) {
	err := s.r.DeleteRange(keys, min, max)
	if err != nil {
		panic(fmt.Sprintf("DeleteRange: %v", err))
	}
}

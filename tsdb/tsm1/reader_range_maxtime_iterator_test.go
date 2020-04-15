package tsm1

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/influxdata/influxdb/v2/tsdb/cursors"
)

func TestTimeRangeMaxTimeIterator(t *testing.T) {
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
		maxTime int64
	}

	EXP := func(r ...interface{}) (rr []res) {
		for i := 0; i+2 < len(r); i += 3 {
			rr = append(rr, res{k: r[i].(string), hasData: r[i+1].(bool), maxTime: int64(r[i+2].(int))})
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
							iter := r.TimeRangeMaxTimeIterator(key, tt.args.min, tt.args.max)
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

								if got := iter.MaxTime(); got != exp.maxTime {
									t.Errorf("MaxTime(%d): -got/+exp\n%v", i, cmp.Diff(got, exp.maxTime))
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
					exp:      EXP("tag0=val0", true, 2020, "tag0=val1", true, 3020),
					expStats: cursors.CursorStats{ScannedValues: 0, ScannedBytes: 0},
				},
				{
					name: "within block",
					args: args{
						min: 2001,
						max: 2011,
					},
					exp:      EXP("tag0=val0", true, 2011, "tag0=val1", true, 2011),
					expStats: cursors.CursorStats{ScannedValues: 6, ScannedBytes: 48},
				},
				{
					name: "to_2999",
					args: args{
						min: 0,
						max: 2999,
					},
					exp:      EXP("tag0=val0", true, 2020, "tag0=val1", true, 2020),
					expStats: cursors.CursorStats{ScannedValues: 0, ScannedBytes: 0},
				},
				{
					name: "intersects block",
					args: args{
						min: 1500,
						max: 2500,
					},
					exp:      EXP("tag0=val0", true, 2020, "tag0=val1", true, 2020),
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
					exp:      EXP("tag0=val0", true, 2020, "tag0=val1", true, 2000, "tag0=val2", true, 3020),
					expStats: cursors.CursorStats{ScannedValues: 0, ScannedBytes: 0},
				},
				{
					name: "within block",
					args: args{
						min: 2001,
						max: 2011,
					},
					exp:      EXP("tag0=val0", true, 2011, "tag0=val1", false, int(InvalidMinNanoTime), "tag0=val2", true, 2011),
					expStats: cursors.CursorStats{ScannedValues: 6, ScannedBytes: 48},
				},
				{
					name: "1000_2999",
					args: args{
						min: 1000,
						max: 2500,
					},
					exp:      EXP("tag0=val0", true, 2020, "tag0=val1", true, 2000, "tag0=val2", true, 2020),
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
					exp:      EXP("tag0=val1", true, 3020),
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
					exp:      EXP("tag0=val1", false, int(InvalidMinNanoTime)),
					expStats: cursors.CursorStats{ScannedValues: 3, ScannedBytes: 24},
				},
				{
					name: "intersects block",
					args: args{
						min: 1500,
						max: 2500,
					},
					exp:      EXP("tag0=val1", false, int(InvalidMinNanoTime)),
					expStats: cursors.CursorStats{ScannedValues: 3, ScannedBytes: 24},
				},
				{
					name: "beyond all tombstones",
					args: args{
						min: 3000,
						max: 4000,
					},
					exp:      EXP("tag0=val1", true, 3020),
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
					exp:      EXP("tag0=val1", true, 1020, "tag0=val2", true, 3020),
					expStats: cursors.CursorStats{ScannedValues: 10, ScannedBytes: 80},
				},
				{
					name: "within block",
					args: args{
						min: 2001,
						max: 2011,
					},
					exp:      EXP("tag0=val1", false, int(InvalidMinNanoTime), "tag0=val2", false, int(InvalidMinNanoTime)),
					expStats: cursors.CursorStats{ScannedValues: 3, ScannedBytes: 24},
				},
				{
					name: "1000_2500",
					args: args{
						min: 1000,
						max: 2500,
					},
					exp:      EXP("tag0=val1", true, 1020, "tag0=val2", false, int(InvalidMinNanoTime)),
					expStats: cursors.CursorStats{ScannedValues: 7, ScannedBytes: 56},
				},
			},
		},
	})
}

package export

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/services/meta"
)

func ts(s int64) time.Time { return time.Unix(s, 0).UTC() }

func ms(times ...int64) meta.ShardGroupInfos {
	sgis := make(meta.ShardGroupInfos, len(times)-1)
	for i := range sgis {
		sgis[i] = meta.ShardGroupInfo{ID: uint64(i), StartTime: ts(times[i]), EndTime: ts(times[i+1])}
	}
	return sgis
}

func TestMakeShardGroupsForDuration(t *testing.T) {
	tests := []struct {
		name string
		min  time.Time
		max  time.Time
		d    time.Duration
		exp  meta.ShardGroupInfos
	}{
		{
			min: ts(15),
			max: ts(25),
			d:   10 * time.Second,
			exp: ms(10, 20, 30),
		},
		{
			min: ts(15),
			max: ts(20),
			d:   10 * time.Second,
			exp: ms(10, 20, 30),
		},
		{
			min: ts(15),
			max: ts(17),
			d:   10 * time.Second,
			exp: ms(10, 20),
		},
		{
			min: ts(10),
			max: ts(20),
			d:   10 * time.Second,
			exp: ms(10, 20, 30),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := makeShardGroupsForDuration(tt.min, tt.max, tt.d); !cmp.Equal(got, tt.exp) {
				t.Errorf("unexpected value -got/+exp\n%s", cmp.Diff(got, tt.exp))
			}
		})
	}
}

func ms2(times ...int64) meta.ShardGroupInfos {
	sgis := make(meta.ShardGroupInfos, len(times)/2)
	for i := 0; i < len(times); i += 2 {
		sgis[i/2] = meta.ShardGroupInfo{ID: uint64(i / 2), StartTime: ts(times[i]), EndTime: ts(times[i+1])}
	}
	return sgis
}

func shardGroupEqual(x, y meta.ShardGroupInfo) bool {
	return x.StartTime == y.StartTime && x.EndTime == y.EndTime
}

func TestPlanShardGroups(t *testing.T) {
	tests := []struct {
		name string
		g    meta.ShardGroupInfos
		d    time.Duration
		exp  meta.ShardGroupInfos
	}{
		{
			name: "20s->10s.nogap",
			g:    ms2(20, 40, 40, 60, 60, 80),
			d:    10 * time.Second,
			exp:  ms2(20, 30, 30, 40, 40, 50, 50, 60, 60, 70, 70, 80),
		},
		{
			name: "20s->10s.gap",
			g:    ms2(20, 40, 60, 80, 80, 100),
			d:    10 * time.Second,
			exp:  ms2(20, 30, 30, 40, 60, 70, 70, 80, 80, 90, 90, 100),
		},
		{
			name: "05s->10s.nogap",
			g:    ms2(15, 20, 20, 25, 25, 30),
			d:    10 * time.Second,
			exp:  ms2(10, 20, 20, 30),
		},
		{
			name: "05s->10s.gap",
			g:    ms2(15, 20, 20, 25, 50, 55, 55, 60),
			d:    10 * time.Second,
			exp:  ms2(10, 20, 20, 30, 50, 60),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			min, max := tc.g[0].StartTime, tc.g[len(tc.g)-1].EndTime
			got := planShardGroups(tc.g, min, max, tc.d)
			if !cmp.Equal(got, tc.exp, cmp.Comparer(shardGroupEqual)) {
				t.Errorf("unexpected value -got/+exp\n%s", cmp.Diff(got, tc.exp))
			}
		})
	}
}

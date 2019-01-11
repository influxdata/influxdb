package tsm1

import (
	"reflect"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestTimeRangeMerger(t *testing.T) {
	ranges := func(ns ...int64) (out []TimeRange) {
		for _, n := range ns {
			out = append(out, TimeRange{n, n})
		}
		return out
	}

	check := func(t *testing.T, exp []TimeRange, merger timeRangeMerger) {
		t.Helper()

		var got []TimeRange
		for {
			tr, ok := merger.Pop()
			if !ok {
				break
			}
			got = append(got, tr)
		}

		if !reflect.DeepEqual(got, exp) {
			t.Fatalf("bad merge:\n%v", cmp.Diff(got, exp))
		}
	}

	check(t, ranges(0, 1, 2, 3, 4, 5, 6), timeRangeMerger{
		sorted:   ranges(0, 2, 6),
		unsorted: ranges(1, 3, 5),
		single:   TimeRange{4, 4},
	})

	check(t, ranges(0, 1, 2), timeRangeMerger{
		sorted: ranges(0, 1, 2),
		used:   true,
	})

	check(t, ranges(0, 1, 2), timeRangeMerger{
		unsorted: ranges(0, 1, 2),
		used:     true,
	})

	check(t, ranges(0), timeRangeMerger{
		single: TimeRange{0, 0},
	})

	check(t, ranges(0, 0, 0), timeRangeMerger{
		sorted:   ranges(0),
		unsorted: ranges(0),
		single:   TimeRange{0, 0},
	})
}

func TestTimeRangeCoverEntries(t *testing.T) {
	ranges := func(ns ...int64) (out []TimeRange) {
		for i := 0; i+1 < len(ns); i += 2 {
			out = append(out, TimeRange{ns[i], ns[i+1]})
		}
		return out
	}

	entries := func(ns ...int64) (out []IndexEntry) {
		for i := 0; i+1 < len(ns); i += 2 {
			out = append(out, IndexEntry{MinTime: ns[i], MaxTime: ns[i+1]})
		}
		return out
	}

	check := func(t *testing.T, ranges []TimeRange, entries []IndexEntry, covers bool) {
		t.Helper()
		sort.Slice(ranges, func(i, j int) bool { return ranges[i].Less(ranges[j]) })
		got := timeRangesCoverEntries(timeRangeMerger{sorted: ranges, used: true}, entries)
		if got != covers {
			t.Fatalf("bad covers:\nranges: %v\nentries: %v\ncovers: %v\ngot: %v",
				ranges, entries, covers, got)
		}
	}

	check(t, ranges(0, 0, 1, 1, 2, 2), entries(0, 0, 1, 1, 2, 2), true)
	check(t, ranges(0, 0, 1, 1, 2, 2), entries(0, 0, 2, 2), true)
	check(t, ranges(0, 0, 1, 1, 2, 2), entries(3, 3), false)
	check(t, ranges(0, 0, 1, 1, 2, 2), entries(-1, -1), false)
	check(t, ranges(0, 10), entries(1, 1, 2, 2), true)
	check(t, ranges(0, 1, 1, 2), entries(0, 0, 1, 1, 2, 2), true)
	check(t, ranges(0, 10), entries(0, 0, 2, 2), true)
	check(t, ranges(0, 1, 1, 2), entries(0, 0, 2, 2), true)
	check(t, ranges(0, 1, 4, 5), entries(0, 0, 5, 5), true)
	check(t, ranges(), entries(), true)
	check(t, ranges(), entries(0, 0), false)
	check(t, ranges(0, 0), entries(), true)
}

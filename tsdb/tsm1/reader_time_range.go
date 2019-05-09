package tsm1

// TimeRange holds a min and max timestamp.
type TimeRange struct {
	Min, Max int64
}

func (t TimeRange) Overlaps(min, max int64) bool {
	return t.Min <= max && t.Max >= min
}

// Within returns true if min < t.Min and t.Max < max and therefore the interval [t.Min, t.Max] is
// contained within [min, max]
func (t TimeRange) Within(min, max int64) bool {
	return min < t.Min && t.Max < max
}

func (t TimeRange) Less(o TimeRange) bool {
	return t.Min < o.Min || (t.Min == o.Min && t.Max < o.Max)
}

// timeRangesCoverEntries returns true if the time ranges fully cover the entries.
func timeRangesCoverEntries(merger timeRangeMerger, entries []IndexEntry) (covers bool) {
	if len(entries) == 0 {
		return true
	}

	mustCover := entries[0].MinTime
	ts, ok := merger.Pop()

	for len(entries) > 0 && ok {
		switch {
		// If the tombstone does not include mustCover, we
		// know we do not fully cover every entry.
		case ts.Min > mustCover:
			return false

		// Otherwise, if the tombstone covers the rest of
		// the entry, consume it and bump mustCover to the
		// start of the next entry.
		case ts.Max >= entries[0].MaxTime:
			entries = entries[1:]
			if len(entries) > 0 {
				mustCover = entries[0].MinTime
			}

		// Otherwise, we're still inside of an entry, and
		// so the tombstone must adjoin the current tombstone.
		default:
			if ts.Max >= mustCover {
				mustCover = ts.Max + 1
			}
			ts, ok = merger.Pop()
		}
	}

	return len(entries) == 0
}

// timeRangeMerger is a special purpose data structure to merge three sources of
// TimeRanges so that we can check if they cover a slice of index entries.
type timeRangeMerger struct {
	fromMap    []TimeRange
	fromPrefix []TimeRange
	single     TimeRange
	used       bool // if single has been used
}

// Pop returns the next TimeRange in sorted order and a boolean indicating that
// there was a TimeRange to read.
func (t *timeRangeMerger) Pop() (out TimeRange, ok bool) {
	var where *[]TimeRange
	var what []TimeRange

	if len(t.fromMap) > 0 {
		where, what = &t.fromMap, t.fromMap[1:]
		out, ok = t.fromMap[0], true
	}

	if len(t.fromPrefix) > 0 && (!ok || t.fromPrefix[0].Less(out)) {
		where, what = &t.fromPrefix, t.fromPrefix[1:]
		out, ok = t.fromPrefix[0], true
	}

	if !t.used && (!ok || t.single.Less(out)) {
		t.used = true
		return t.single, true
	}

	if ok {
		*where = what
	}

	return out, ok
}

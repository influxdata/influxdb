package coordinator

import (
	"context"

	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxql"
)

// isLimitPushdownEligible reports whether this fetch may treat
// opt.Limit/opt.Offset as a true global cap on total output rows, letting
// CreateIterator skip opening chronologically later (or, for descending
// queries, earlier) shard groups once already satisfied by groups opened
// so far.
//
// This is only safe for a plain, ungrouped, non-aggregate fetch of a single
// concrete measurement: opt.GlobalLimitEligible (set only by
// buildAuxIterator, and only when this is the sole source feeding the
// enclosing LIMIT/OFFSET) rules out multi-measurement unions and any
// aggregate/selector call fetch (buildCallIterator always zeroes
// Limit/Offset before recursing, so opt.Limit > 0 never co-occurs with a
// call fetch here). GROUP BY (tags or time) and SLIMIT/SOFFSET are excluded
// because they change what "enough rows" means per shard group.
func isLimitPushdownEligible(m *influxql.Measurement, opt query.IteratorOptions) bool {
	return m.Regex == nil &&
		opt.Limit > 0 &&
		opt.SLimit == 0 && opt.SOffset == 0 &&
		len(opt.Dimensions) == 0 &&
		opt.Interval.IsZero() &&
		opt.GlobalLimitEligible
}

// newShardGroupLimitIterator lazily opens groups (reordered to match
// opt.Ascending) one at a time, stopping once enough rows have been
// forwarded to satisfy opt.Limit+opt.Offset without opening every
// remaining group. It is a pure pass-through: all point filtering still
// happens in the caller's unchanged top-level LimitIterator.
func newShardGroupLimitIterator(ctx context.Context, m *influxql.Measurement, opt query.IteratorOptions, groups []groupedShards) (query.Iterator, error) {
	ordered := groups
	if !opt.Ascending {
		ordered = make([]groupedShards, len(groups))
		for i, g := range groups {
			ordered[len(groups)-1-i] = g
		}
	}

	idx := 0
	openNext := func() (query.Iterator, error) {
		for idx < len(ordered) {
			g := ordered[idx]
			idx++
			select {
			case <-opt.InterruptCh:
				return nil, query.ErrQueryInterrupted
			default:
			}
			itr, err := g.Shards.CreateIterator(ctx, m, opt)
			if err != nil || itr != nil {
				return itr, err
			}
			// Empty group (no matching data) — try the next one.
		}
		return nil, nil
	}

	first, err := openNext()
	if err != nil || first == nil {
		return first, err
	}
	return query.NewLazyGroupChainIterator(first, openNext, opt), nil
}

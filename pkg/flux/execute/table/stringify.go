package table

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/semantic"
	"github.com/influxdata/flux/values"
)

// Stringify will read a table and turn it into a human-readable string.
func Stringify(table flux.Table) string {
	var sb strings.Builder
	stringifyKey(&sb, table)
	if err := table.Do(func(cr flux.ColReader) error {
		stringifyRows(&sb, cr)
		return nil
	}); err != nil {
		_, _ = fmt.Fprintf(&sb, "table error: %s\n", err)
	}
	return sb.String()
}

func getSortedIndices(key flux.GroupKey, cols []flux.ColMeta) ([]flux.ColMeta, []int) {
	indices := make([]int, len(cols))
	for i := range indices {
		indices[i] = i
	}
	sort.Slice(indices, func(i, j int) bool {
		ci, cj := cols[indices[i]], cols[indices[j]]
		if key.HasCol(ci.Label) && !key.HasCol(cj.Label) {
			return true
		} else if !key.HasCol(ci.Label) && key.HasCol(cj.Label) {
			return false
		}
		return ci.Label < cj.Label
	})
	return cols, indices
}

func stringifyKey(sb *strings.Builder, table flux.Table) {
	key := table.Key()
	cols, indices := getSortedIndices(table.Key(), table.Cols())

	sb.WriteString("# ")
	if len(cols) == 0 {
		sb.WriteString("(none)")
	} else {
		nkeys := 0
		for _, idx := range indices {
			c := cols[idx]
			kidx := execute.ColIdx(c.Label, key.Cols())
			if kidx < 0 {
				continue
			}

			if nkeys > 0 {
				sb.WriteString(",")
			}
			sb.WriteString(cols[idx].Label)
			sb.WriteString("=")

			v := key.Value(kidx)
			stringifyValue(sb, v)
			nkeys++
		}
	}
	sb.WriteString(" ")

	ncols := 0
	for _, idx := range indices {
		c := cols[idx]
		if key.HasCol(c.Label) {
			continue
		}

		if ncols > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(cols[idx].Label)
		sb.WriteString("=")
		sb.WriteString(cols[idx].Type.String())
		ncols++
	}
	sb.WriteString("\n")
}

func stringifyRows(sb *strings.Builder, cr flux.ColReader) {
	key := cr.Key()
	cols, indices := getSortedIndices(cr.Key(), cr.Cols())

	for i, sz := 0, cr.Len(); i < sz; i++ {
		inKey := true
		for j, idx := range indices {
			c := cols[idx]
			if j > 0 {
				if inKey && !key.HasCol(c.Label) {
					sb.WriteString(" ")
					inKey = false
				} else {
					sb.WriteString(",")
				}
			} else if !key.HasCol(c.Label) {
				inKey = false
			}
			sb.WriteString(cols[idx].Label)
			sb.WriteString("=")

			v := execute.ValueForRow(cr, i, idx)
			stringifyValue(sb, v)
		}
		sb.WriteString("\n")
	}
}

func stringifyValue(sb *strings.Builder, v values.Value) {
	if v.IsNull() {
		sb.WriteString("!(nil)")
		return
	}

	switch v.Type().Nature() {
	case semantic.Int:
		_, _ = fmt.Fprintf(sb, "%di", v.Int())
	case semantic.UInt:
		_, _ = fmt.Fprintf(sb, "%du", v.UInt())
	case semantic.Float:
		_, _ = fmt.Fprintf(sb, "%.3f", v.Float())
	case semantic.String:
		sb.WriteString(v.Str())
	case semantic.Bool:
		if v.Bool() {
			sb.WriteString("true")
		} else {
			sb.WriteString("false")
		}
	case semantic.Time:
		ts := v.Time().Time()
		if ts.Nanosecond() > 0 {
			sb.WriteString(ts.Format(time.RFC3339Nano))
		} else {
			sb.WriteString(ts.Format(time.RFC3339))
		}
	default:
		sb.WriteString("!(invalid)")
	}
}

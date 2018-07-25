package execute

import (
	"fmt"
	"strings"

	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/values"
)

type groupKey struct {
	cols   []query.ColMeta
	values []values.Value
}

func NewGroupKey(cols []query.ColMeta, values []values.Value) query.GroupKey {
	return &groupKey{
		cols:   cols,
		values: values,
	}
}

func (k *groupKey) Cols() []query.ColMeta {
	return k.cols
}
func (k *groupKey) HasCol(label string) bool {
	return ColIdx(label, k.cols) >= 0
}
func (k *groupKey) LabelValue(label string) values.Value {
	if !k.HasCol(label) {
		return nil
	}
	return k.Value(ColIdx(label, k.cols))
}
func (k *groupKey) Value(j int) values.Value {
	return k.values[j]
}
func (k *groupKey) ValueBool(j int) bool {
	return k.values[j].Bool()
}
func (k *groupKey) ValueUInt(j int) uint64 {
	return k.values[j].UInt()
}
func (k *groupKey) ValueInt(j int) int64 {
	return k.values[j].Int()
}
func (k *groupKey) ValueFloat(j int) float64 {
	return k.values[j].Float()
}
func (k *groupKey) ValueString(j int) string {
	return k.values[j].Str()
}
func (k *groupKey) ValueDuration(j int) Duration {
	return k.values[j].Duration()
}
func (k *groupKey) ValueTime(j int) Time {
	return k.values[j].Time()
}

func (k *groupKey) Equal(o query.GroupKey) bool {
	return groupKeyEqual(k, o)
}

func (k *groupKey) Less(o query.GroupKey) bool {
	return groupKeyLess(k, o)
}

func (k *groupKey) String() string {
	var b strings.Builder
	b.WriteRune('{')
	for j, c := range k.cols {
		if j != 0 {
			b.WriteRune(',')
		}
		fmt.Fprintf(&b, "%s=%v", c.Label, k.values[j])
	}
	b.WriteRune('}')
	return b.String()
}

func groupKeyEqual(a, b query.GroupKey) bool {
	aCols := a.Cols()
	bCols := b.Cols()
	if len(aCols) != len(bCols) {
		return false
	}
	for j, c := range aCols {
		if aCols[j] != bCols[j] {
			return false
		}
		switch c.Type {
		case query.TBool:
			if a.ValueBool(j) != b.ValueBool(j) {
				return false
			}
		case query.TInt:
			if a.ValueInt(j) != b.ValueInt(j) {
				return false
			}
		case query.TUInt:
			if a.ValueUInt(j) != b.ValueUInt(j) {
				return false
			}
		case query.TFloat:
			if a.ValueFloat(j) != b.ValueFloat(j) {
				return false
			}
		case query.TString:
			if a.ValueString(j) != b.ValueString(j) {
				return false
			}
		case query.TTime:
			if a.ValueTime(j) != b.ValueTime(j) {
				return false
			}
		}
	}
	return true
}

func groupKeyLess(a, b query.GroupKey) bool {
	aCols := a.Cols()
	bCols := b.Cols()
	if av, bv := len(aCols), len(bCols); av != bv {
		return av < bv
	}
	for j, c := range aCols {
		if av, bv := aCols[j].Label, bCols[j].Label; av != bv {
			return av < bv
		}
		if av, bv := aCols[j].Type, bCols[j].Type; av != bv {
			return av < bv
		}
		switch c.Type {
		case query.TBool:
			if av, bv := a.ValueBool(j), b.ValueBool(j); av != bv {
				return av
			}
		case query.TInt:
			if av, bv := a.ValueInt(j), b.ValueInt(j); av != bv {
				return av < bv
			}
		case query.TUInt:
			if av, bv := a.ValueUInt(j), b.ValueUInt(j); av != bv {
				return av < bv
			}
		case query.TFloat:
			if av, bv := a.ValueFloat(j), b.ValueFloat(j); av != bv {
				return av < bv
			}
		case query.TString:
			if av, bv := a.ValueString(j), b.ValueString(j); av != bv {
				return av < bv
			}
		case query.TTime:
			if av, bv := a.ValueTime(j), b.ValueTime(j); av != bv {
				return av < bv
			}
		}
	}
	return false
}

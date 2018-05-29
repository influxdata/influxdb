package execute

import (
	"fmt"
	"strings"

	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/values"
)

type partitionKey struct {
	cols   []query.ColMeta
	values []values.Value
}

func NewPartitionKey(cols []query.ColMeta, values []values.Value) query.PartitionKey {
	return &partitionKey{
		cols:   cols,
		values: values,
	}
}

func (k *partitionKey) Cols() []query.ColMeta {
	return k.cols
}
func (k *partitionKey) HasCol(label string) bool {
	return ColIdx(label, k.cols) >= 0
}
func (k *partitionKey) Value(j int) values.Value {
	return k.values[j]
}
func (k *partitionKey) ValueBool(j int) bool {
	return k.values[j].Bool()
}
func (k *partitionKey) ValueUInt(j int) uint64 {
	return k.values[j].UInt()
}
func (k *partitionKey) ValueInt(j int) int64 {
	return k.values[j].Int()
}
func (k *partitionKey) ValueFloat(j int) float64 {
	return k.values[j].Float()
}
func (k *partitionKey) ValueString(j int) string {
	return k.values[j].Str()
}
func (k *partitionKey) ValueDuration(j int) Duration {
	return k.values[j].Duration()
}
func (k *partitionKey) ValueTime(j int) Time {
	return k.values[j].Time()
}

func (k *partitionKey) Equal(o query.PartitionKey) bool {
	return partitionKeyEqual(k, o)
}

func (k *partitionKey) Less(o query.PartitionKey) bool {
	return partitionKeyLess(k, o)
}

func (k *partitionKey) String() string {
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

func partitionKeyEqual(a, b query.PartitionKey) bool {
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

func partitionKeyLess(a, b query.PartitionKey) bool {
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

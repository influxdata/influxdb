package executetest

import (
	"fmt"

	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/values"
)

// Block is an implementation of execute.Block
// It is designed to make it easy to statically declare the data within the block.
// Not all fields need to be set. See comments on each field.
// Use Normalize to ensure that all fields are set before equality comparisons.
type Block struct {
	// PartitionKey of the block. Does not need to be set explicitly.
	PartitionKey query.PartitionKey
	// KeyCols is a list of column that are part of the partition key.
	// The column type is deduced from the ColMeta slice.
	KeyCols []string
	// KeyValues is a list of values for the partition key columns.
	// Only needs to be set when no data is present on the Block.
	KeyValues []interface{}
	// ColMeta is a list of columns of the block.
	ColMeta []query.ColMeta
	// Data is a list of rows, i.e. Data[row][col]
	// Each row must be a list with length equal to len(ColMeta)
	Data [][]interface{}
}

// Normalize ensures all fields of the Block are set correctly.
func (b *Block) Normalize() {
	if b.PartitionKey == nil {
		cols := make([]query.ColMeta, len(b.KeyCols))
		vs := make([]values.Value, len(b.KeyCols))
		if len(b.KeyValues) != len(b.KeyCols) {
			b.KeyValues = make([]interface{}, len(b.KeyCols))
		}
		for j, label := range b.KeyCols {
			idx := execute.ColIdx(label, b.ColMeta)
			if idx < 0 {
				panic(fmt.Errorf("block invalid: missing partition column %q", label))
			}
			cols[j] = b.ColMeta[idx]
			if len(b.Data) > 0 {
				b.KeyValues[j] = b.Data[0][idx]
			}
			v, err := values.NewValue(b.KeyValues[j], execute.ConvertToKind(cols[j].Type))
			if err != nil {
				panic(err)
			}
			vs[j] = v
		}
		b.PartitionKey = execute.NewPartitionKey(cols, vs)
	}
}

func (b *Block) Empty() bool {
	return len(b.Data) == 0
}

func (b *Block) RefCount(n int) {}

func (b *Block) Cols() []query.ColMeta {
	return b.ColMeta
}

func (b *Block) Key() query.PartitionKey {
	b.Normalize()
	return b.PartitionKey
}

func (b *Block) Do(f func(query.ColReader) error) error {
	for _, r := range b.Data {
		if err := f(ColReader{
			key:  b.Key(),
			cols: b.ColMeta,
			row:  r,
		}); err != nil {
			return err
		}
	}
	return nil
}

type ColReader struct {
	key  query.PartitionKey
	cols []query.ColMeta
	row  []interface{}
}

func (cr ColReader) Cols() []query.ColMeta {
	return cr.cols
}

func (cr ColReader) Key() query.PartitionKey {
	return cr.key
}
func (cr ColReader) Len() int {
	return 1
}

func (cr ColReader) Bools(j int) []bool {
	return []bool{cr.row[j].(bool)}
}

func (cr ColReader) Ints(j int) []int64 {
	return []int64{cr.row[j].(int64)}
}

func (cr ColReader) UInts(j int) []uint64 {
	return []uint64{cr.row[j].(uint64)}
}

func (cr ColReader) Floats(j int) []float64 {
	return []float64{cr.row[j].(float64)}
}

func (cr ColReader) Strings(j int) []string {
	return []string{cr.row[j].(string)}
}

func (cr ColReader) Times(j int) []execute.Time {
	return []execute.Time{cr.row[j].(execute.Time)}
}

func BlocksFromCache(c execute.DataCache) (blocks []*Block, err error) {
	c.ForEach(func(key query.PartitionKey) {
		if err != nil {
			return
		}
		var b query.Block
		b, err = c.Block(key)
		if err != nil {
			return
		}
		var cb *Block
		cb, err = ConvertBlock(b)
		if err != nil {
			return
		}
		blocks = append(blocks, cb)
	})
	return blocks, nil
}

func ConvertBlock(b query.Block) (*Block, error) {
	key := b.Key()
	blk := &Block{
		PartitionKey: key,
		ColMeta:      b.Cols(),
	}

	keyCols := key.Cols()
	if len(keyCols) > 0 {
		blk.KeyCols = make([]string, len(keyCols))
		blk.KeyValues = make([]interface{}, len(keyCols))
		for j, c := range keyCols {
			blk.KeyCols[j] = c.Label
			var v interface{}
			switch c.Type {
			case query.TBool:
				v = key.ValueBool(j)
			case query.TUInt:
				v = key.ValueUInt(j)
			case query.TInt:
				v = key.ValueInt(j)
			case query.TFloat:
				v = key.ValueFloat(j)
			case query.TString:
				v = key.ValueString(j)
			case query.TTime:
				v = key.ValueTime(j)
			default:
				return nil, fmt.Errorf("unsupported column type %v", c.Type)
			}
			blk.KeyValues[j] = v
		}
	}

	err := b.Do(func(cr query.ColReader) error {
		l := cr.Len()
		for i := 0; i < l; i++ {
			row := make([]interface{}, len(blk.ColMeta))
			for j, c := range blk.ColMeta {
				var v interface{}
				switch c.Type {
				case query.TBool:
					v = cr.Bools(j)[i]
				case query.TInt:
					v = cr.Ints(j)[i]
				case query.TUInt:
					v = cr.UInts(j)[i]
				case query.TFloat:
					v = cr.Floats(j)[i]
				case query.TString:
					v = cr.Strings(j)[i]
				case query.TTime:
					v = cr.Times(j)[i]
				default:
					panic(fmt.Errorf("unknown column type %s", c.Type))
				}
				row[j] = v
			}
			blk.Data = append(blk.Data, row)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return blk, nil
}

type SortedBlocks []*Block

func (b SortedBlocks) Len() int {
	return len(b)
}

func (b SortedBlocks) Less(i int, j int) bool {
	return b[i].Key().Less(b[j].Key())
}

func (b SortedBlocks) Swap(i int, j int) {
	b[i], b[j] = b[j], b[i]
}

// NormalizeBlocks ensures that each block is normalized
func NormalizeBlocks(bs []*Block) {
	for _, b := range bs {
		b.Key()
	}
}

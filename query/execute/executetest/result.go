package executetest

import "github.com/influxdata/platform/query/execute"

type Result struct {
	Blks []*Block
}

func NewResult(blocks []*Block) *Result {
	return &Result{Blks: blocks}
}

func (r *Result) Blocks() execute.BlockIterator {
	return &BlockIterator{
		r.Blks,
	}
}

func (r *Result) Normalize() {
	NormalizeBlocks(r.Blks)
}

type BlockIterator struct {
	blocks []*Block
}

func (bi *BlockIterator) Do(f func(execute.Block) error) error {
	for _, b := range bi.blocks {
		if err := f(b); err != nil {
			return err
		}
	}
	return nil
}

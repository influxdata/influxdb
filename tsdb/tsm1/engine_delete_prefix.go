package tsm1

import (
	"bytes"
	"math"

	"github.com/influxdata/influxql"
	"github.com/influxdata/platform/pkg/bytesutil"
)

func (e *Engine) DeletePrefix(prefix []byte, min, max int64) error {
	// TODO(jeff): we need to block writes to this prefix while deletes are in progress
	// otherwise we can end up in a situation where we have staged data in the cache or
	// WAL that was deleted from the index, or worse. This needs to happen at a higher
	// layer.

	// Ensure that the index does not compact away the measurement or series we're
	// going to delete before we're done with them.
	e.index.DisableCompactions()
	defer e.index.EnableCompactions()
	e.index.Wait()

	fs, err := e.index.RetainFileSet()
	if err != nil {
		return err
	}
	defer fs.Release()

	// TODO(jeff): are the query language values still a thing?
	// Min and max time in the engine are slightly different from the query language values.
	if min == influxql.MinTime {
		min = math.MinInt64
	}
	if max == influxql.MaxTime {
		max = math.MaxInt64
	}

	// Run the delete on each TSM file in parallel
	if err := e.FileStore.Apply(func(r TSMFile) error {
		return r.DeletePrefix(prefix, min, max)
	}); err != nil {
		return err
	}

	// TODO(jeff): block writes matching the prefix while this function is in progress to
	// avoid races with the cache/wal.
	// TODO(jeff): add a DeletePrefix to the Cache and WAL.
	// TODO(jeff): add a Tombstone entry into the WAL for deletes.

	var deleteKeys [][]byte

	// ApplySerialEntryFn cannot return an error in this invocation.
	_ = e.Cache.ApplyEntryFn(func(k []byte, _ *entry) error {
		if bytes.HasPrefix(k, prefix) {
			if deleteKeys == nil {
				deleteKeys = make([][]byte, 0, 10000)
			}
			deleteKeys = append(deleteKeys, k)
		}
		return nil
	})

	// Sort the series keys because ApplyEntryFn iterates over the keys randomly.
	bytesutil.Sort(deleteKeys)

	// delete from the cache and wal
	e.Cache.DeleteRange(deleteKeys, min, max)
	if _, err := e.WAL.DeleteRange(deleteKeys, min, max); err != nil {
		return err
	}

	// TODO(jeff): have to clean up the index. it's hard to know which keys are fully
	// deleted now due to a couple of complications:
	// 1. the reader/index don't keep track of which keys have been deleted, which means
	//    that they will have a hard time responding to the query "which keys are deleted
	//    with this prefix?"
	// 2. even if they did, deletion is a global property. in the presense of concurrent
	//    deletes, some deletes may observe a TSM file with some key deleted, but some other
	//    TSM file still has it, while another delete observes the opposite. These orderings
	//    make it hard to check or know which keys to fully delete out of the index.
	// 3. it's important to atomically update the index with the deletions that happened in
	//    the TSM file. note: this is already botched because DeletePrefix on the TSMFile saves
	//    the tombstone each time.

	return nil
}

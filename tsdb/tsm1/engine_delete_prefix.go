package tsm1

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/influxdb/kit/tracing"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/tsi1"
	"github.com/influxdata/influxql"
)

// DeletePrefixRange removes all TSM data belonging to a bucket, and removes all index
// and series file data associated with the bucket. The provided time range ensures
// that only bucket data for that range is removed.
func (e *Engine) DeletePrefixRange(rootCtx context.Context, name []byte, min, max int64, pred Predicate) error {
	span, ctx := tracing.StartSpanFromContext(rootCtx)
	span.LogKV("name_prefix", fmt.Sprintf("%x", name),
		"min", time.Unix(0, min), "max", time.Unix(0, max),
		"has_pred", pred != nil,
	)
	defer span.Finish()
	// TODO(jeff): we need to block writes to this prefix while deletes are in progress
	// otherwise we can end up in a situation where we have staged data in the cache or
	// WAL that was deleted from the index, or worse. This needs to happen at a higher
	// layer.

	// TODO(jeff): ensure the engine is not closed while we're running this. At least
	// now we know that the series file or index won't be closed out from underneath
	// of us.

	// Ensure that the index does not compact away the measurement or series we're
	// going to delete before we're done with them.
	span, _ = tracing.StartSpanFromContextWithOperationName(rootCtx, "disable index compactions")
	e.index.DisableCompactions()
	defer e.index.EnableCompactions()
	e.index.Wait()
	span.Finish()

	// Disable and abort running compactions so that tombstones added existing tsm
	// files don't get removed. This would cause deleted measurements/series to
	// re-appear once the compaction completed. We only disable the level compactions
	// so that snapshotting does not stop while writing out tombstones. If it is stopped,
	// and writing tombstones takes a long time, writes can get rejected due to the cache
	// filling up.
	span, _ = tracing.StartSpanFromContextWithOperationName(rootCtx, "disable tsm compactions")
	e.disableLevelCompactions(true)
	defer e.enableLevelCompactions(true)
	span.Finish()

	span, _ = tracing.StartSpanFromContextWithOperationName(rootCtx, "disable series file compactions")
	e.sfile.DisableCompactions()
	defer e.sfile.EnableCompactions()
	span.Finish()

	// TODO(jeff): are the query language values still a thing?
	// Min and max time in the engine are slightly different from the query language values.
	if min == influxql.MinTime {
		min = math.MinInt64
	}
	if max == influxql.MaxTime {
		max = math.MaxInt64
	}

	// Run the delete on each TSM file in parallel and keep track of possibly dead keys.

	// TODO(jeff): keep a set of keys for each file to avoid contention.
	// TODO(jeff): come up with a better way to figure out what keys we need to delete
	// from the index.

	var possiblyDead struct {
		sync.RWMutex
		keys map[string]struct{}
	}
	possiblyDead.keys = make(map[string]struct{})

	if err := e.FileStore.Apply(func(r TSMFile) error {
		var predClone Predicate // Apply executes concurrently across files.
		if pred != nil {
			predClone = pred.Clone()
		}

		// TODO(edd): tracing this deep down is currently speculative, so I have
		// not added the tracing into the TSMReader API.
		span, _ := tracing.StartSpanFromContextWithOperationName(rootCtx, "TSMFile delete prefix")
		span.LogKV("file_path", r.Path())
		defer span.Finish()

		return r.DeletePrefix(name, min, max, predClone, func(key []byte) {
			possiblyDead.Lock()
			possiblyDead.keys[string(key)] = struct{}{}
			possiblyDead.Unlock()
		})
	}); err != nil {
		return err
	}

	span, _ = tracing.StartSpanFromContextWithOperationName(rootCtx, "Cache find delete keys")
	span.LogKV("cache_size", e.Cache.Size())
	var keysChecked int // For tracing information.
	// ApplySerialEntryFn cannot return an error in this invocation.
	nameStr := string(name)
	_ = e.Cache.ApplyEntryFn(func(k string, _ *entry) error {
		keysChecked++
		if !strings.HasPrefix(k, nameStr) {
			return nil
		}
		// TODO(edd): either use an unsafe conversion to []byte, or add a MatchesString
		// method to tsm1.Predicate.
		if pred != nil && !pred.Matches([]byte(k)) {
			return nil
		}

		// we have to double check every key in the cache because maybe
		// it exists in the index but not yet on disk.
		possiblyDead.keys[k] = struct{}{}

		return nil
	})
	span.LogKV("cache_cardinality", keysChecked)
	span.Finish()

	// Delete from the cache (traced in cache).
	e.Cache.DeleteBucketRange(ctx, nameStr, min, max, pred)

	// Now that all of the data is purged, we need to find if some keys are fully deleted
	// and if so, remove them from the index.
	if err := e.FileStore.Apply(func(r TSMFile) error {
		var predClone Predicate // Apply executes concurrently across files.
		if pred != nil {
			predClone = pred.Clone()
		}

		// TODO(edd): tracing this deep down is currently speculative, so I have
		// not added the tracing into the Engine API.
		span, _ := tracing.StartSpanFromContextWithOperationName(rootCtx, "TSMFile determine fully deleted")
		span.LogKV("file_path", r.Path())
		defer span.Finish()

		possiblyDead.RLock()
		defer possiblyDead.RUnlock()

		var keysChecked int
		iter := r.Iterator(name)
		for i := 0; iter.Next(); i++ {
			key := iter.Key()
			if !bytes.HasPrefix(key, name) {
				break
			}
			if predClone != nil && !predClone.Matches(key) {
				continue
			}

			// TODO(jeff): benchmark the locking here.
			if i%1024 == 0 { // allow writes to proceed.
				possiblyDead.RUnlock()
				possiblyDead.RLock()
			}

			if _, ok := possiblyDead.keys[string(key)]; ok {
				possiblyDead.RUnlock()
				possiblyDead.Lock()
				delete(possiblyDead.keys, string(key))
				possiblyDead.Unlock()
				possiblyDead.RLock()
			}
		}
		span.LogKV("keys_checked", keysChecked)
		return iter.Err()
	}); err != nil {
		return err
	}

	span, _ = tracing.StartSpanFromContextWithOperationName(rootCtx, "Cache find delete keys")
	span.LogKV("cache_size", e.Cache.Size())
	keysChecked = 0
	// ApplySerialEntryFn cannot return an error in this invocation.
	_ = e.Cache.ApplyEntryFn(func(k string, _ *entry) error {
		keysChecked++
		if !strings.HasPrefix(k, nameStr) {
			return nil
		}
		// TODO(edd): either use an unsafe conversion to []byte, or add a MatchesString
		// method to tsm1.Predicate.
		if pred != nil && !pred.Matches([]byte(k)) {
			return nil
		}

		delete(possiblyDead.keys, k)
		return nil
	})
	span.LogKV("cache_cardinality", keysChecked)
	span.Finish()

	if len(possiblyDead.keys) > 0 {
		buf := make([]byte, 1024)

		// TODO(jeff): all of these methods have possible errors which opens us to partial
		// failure scenarios. we need to either ensure that partial errors here are ok or
		// do something to fix it.
		// TODO(jeff): it's also important that all of the deletes happen atomically with
		// the deletes of the data in the tsm files.

		// In this case the entire measurement (bucket) can be removed from the index.
		if min == math.MinInt64 && max == math.MaxInt64 && pred == nil {
			// The TSI index and Series File do not store series data in escaped form.
			name = models.UnescapeMeasurement(name)

			// Build up a set of series IDs that we need to remove from the series file.
			set := tsdb.NewSeriesIDSet()
			itr, err := e.index.MeasurementSeriesIDIterator(name)
			if err != nil {
				return err
			}

			var elem tsdb.SeriesIDElem
			for elem, err = itr.Next(); err != nil; elem, err = itr.Next() {
				if elem.SeriesID.IsZero() {
					break
				}

				set.AddNoLock(elem.SeriesID)
			}

			if err != nil {
				return err
			} else if err := itr.Close(); err != nil {
				return err
			}

			// Remove the measurement from the index before the series file.
			span, _ = tracing.StartSpanFromContextWithOperationName(rootCtx, "TSI drop measurement")
			span.LogKV("measurement_name", fmt.Sprintf("%x", name))
			if err := e.index.DropMeasurement(name); err != nil {
				return err
			}
			span.Finish()

			// Iterate over the series ids we previously extracted from the index
			// and remove from the series file.
			span, _ = tracing.StartSpanFromContextWithOperationName(rootCtx, "SFile Delete Series IDs")
			span.LogKV("measurement_name", fmt.Sprintf("%x", name), "series_id_set_size", set.Cardinality())
			var ids []tsdb.SeriesID
			set.ForEachNoLock(func(id tsdb.SeriesID) { ids = append(ids, id) })
			if err = e.sfile.DeleteSeriesIDs(ids); err != nil {
				return err
			}
			span.Finish()
			return err
		}

		// This is the slow path, when not dropping the entire bucket (measurement)
		span, _ = tracing.StartSpanFromContextWithOperationName(rootCtx, "TSI/SFile Delete keys")
		span.LogKV("measurement_name", fmt.Sprintf("%x", name), "keys_to_delete", len(possiblyDead.keys))

		// Convert key map to a slice.
		possiblyDeadKeysSlice := make([][]byte, 0, len(possiblyDead.keys))
		for key := range possiblyDead.keys {
			possiblyDeadKeysSlice = append(possiblyDeadKeysSlice, []byte(key))
		}

		const batchSize = 1000
		batch := make([]tsi1.DropSeriesItem, 0, batchSize)
		ids := make([]tsdb.SeriesID, 0, batchSize)
		for i := 0; i < len(possiblyDeadKeysSlice); i += batchSize {
			isLastBatch := i+batchSize > len(possiblyDeadKeysSlice)
			batch, ids = batch[:0], ids[:0]

			for j := 0; (i*batchSize)+j < len(possiblyDeadKeysSlice) && j < batchSize; j++ {
				var item tsi1.DropSeriesItem

				// TODO(jeff): ugh reduce copies here
				key := possiblyDeadKeysSlice[(i*batchSize)+j]
				item.Key = []byte(key)
				item.Key, _ = SeriesAndFieldFromCompositeKey(item.Key)

				name, tags := models.ParseKeyBytes(item.Key)
				item.SeriesID = e.sfile.SeriesID(name, tags, buf)
				if item.SeriesID.IsZero() {
					continue
				}
				batch = append(batch, item)
				ids = append(ids, item.SeriesID)
			}

			// Remove from index & series file.
			if err := e.index.DropSeries(batch, isLastBatch); err != nil {
				return err
			} else if err := e.sfile.DeleteSeriesIDs(ids); err != nil {
				return err
			}
		}
		span.Finish()
	}

	return nil
}

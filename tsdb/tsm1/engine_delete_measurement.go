package tsm1

import (
	"bytes"
	"math"
	"sync"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/bytesutil"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
)

// DeleteBucket removes all TSM data belonging to a bucket, and removes all index
// and series file data associated with the bucket. The provided time range ensures
// that only bucket data for that range is removed.
func (e *Engine) DeleteBucket(name []byte, min, max int64) error {
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

	// Disable and abort running compactions so that tombstones added existing tsm
	// files don't get removed. This would cause deleted measurements/series to
	// re-appear once the compaction completed. We only disable the level compactions
	// so that snapshotting does not stop while writing out tombstones. If it is stopped,
	// and writing tombstones takes a long time, writes can get rejected due to the cache
	// filling up.
	e.disableLevelCompactions(true)
	defer e.enableLevelCompactions(true)

	e.sfile.DisableCompactions()
	defer e.sfile.EnableCompactions()
	e.sfile.Wait()

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
		return r.DeletePrefix(name, min, max, func(key []byte) {
			possiblyDead.Lock()
			possiblyDead.keys[string(key)] = struct{}{}
			possiblyDead.Unlock()
		})
	}); err != nil {
		return err
	}

	// TODO(jeff): add a DeletePrefix to the Cache and WAL.
	// TODO(jeff): add a Tombstone entry into the WAL for deletes.

	var deleteKeys [][]byte

	// ApplySerialEntryFn cannot return an error in this invocation.
	_ = e.Cache.ApplyEntryFn(func(k []byte, _ *entry) error {
		if bytes.HasPrefix(k, name) {
			if deleteKeys == nil {
				deleteKeys = make([][]byte, 0, 10000)
			}
			deleteKeys = append(deleteKeys, k)

			// we have to double check every key in the cache because maybe
			// it exists in the index but not yet on disk.
			possiblyDead.keys[string(k)] = struct{}{}
		}
		return nil
	})

	// Sort the series keys because ApplyEntryFn iterates over the keys randomly.
	bytesutil.Sort(deleteKeys)

	// Delete from the cache and WAL.
	e.Cache.DeleteRange(deleteKeys, min, max)
	if _, err := e.WAL.DeleteRange(deleteKeys, min, max); err != nil {
		return err
	}

	// Now that all of the data is purged, we need to find if some keys are fully deleted
	// and if so, remove them from the index.
	if err := e.FileStore.Apply(func(r TSMFile) error {
		possiblyDead.RLock()
		defer possiblyDead.RUnlock()

		iter := r.Iterator(name)
		for i := 0; iter.Next(); i++ {
			key := iter.Key()
			if !bytes.HasPrefix(key, name) {
				break
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

		return iter.Err()
	}); err != nil {
		return err
	}

	if len(possiblyDead.keys) > 0 {
		buf := make([]byte, 1024)

		// TODO(jeff): all of these methods have possible errors which opens us to partial
		// failure scenarios. we need to either ensure that partial errors here are ok or
		// do something to fix it.
		// TODO(jeff): it's also important that all of the deletes happen atomically with
		// the deletes of the data in the tsm files.

		// In this case the entire measurement (bucket) can be removed from the index.
		if min == math.MinInt64 && max == math.MaxInt64 {
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
			if err := e.index.DropMeasurement(name); err != nil {
				return err
			}

			// Iterate over the series ids we previously extracted from the index
			// and remove from the series file.
			set.ForEachNoLock(func(id tsdb.SeriesID) {
				if err = e.sfile.DeleteSeriesID(id); err != nil {
					return
				}
			})
			return err
		}

		// This is the slow path, when not dropping the entire bucket (measurement)
		for key := range possiblyDead.keys {
			// TODO(jeff): ugh reduce copies here
			keyb := []byte(key)
			keyb, _ = SeriesAndFieldFromCompositeKey(keyb)

			name, tags := models.ParseKeyBytes(keyb)
			sid := e.sfile.SeriesID(name, tags, buf)
			if sid.IsZero() {
				continue
			}

			if err := e.index.DropSeries(sid, keyb, true); err != nil {
				return err
			}

			if err := e.sfile.DeleteSeriesID(sid); err != nil {
				return err
			}
		}
	}

	return nil
}

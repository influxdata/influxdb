package tsm1

import (
	"bytes"
	"context"
	"sort"
	"strings"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/influxdata/influxdb/v2/tsdb/cursors"
	"github.com/influxdata/influxdb/v2/tsdb/seriesfile"
	"github.com/influxdata/influxql"
	"go.uber.org/zap"
)

// MeasurementNames returns an iterator which enumerates the measurements for the given
// bucket and limited to the time range [start, end].
//
// MeasurementNames will always return a StringIterator if there is no error.
//
// If the context is canceled before MeasurementNames has finished processing, a non-nil
// error will be returned along with statistics for the already scanned data.
func (e *Engine) MeasurementNames(ctx context.Context, orgID, bucketID influxdb.ID, start, end int64) (cursors.StringIterator, error) {
	orgBucket := tsdb.EncodeName(orgID, bucketID)
	// TODO(edd): we need to clean up how we're encoding the prefix so that we
	// don't have to remember to get it right everywhere we need to touch TSM data.
	prefix := models.EscapeMeasurement(orgBucket[:])

	var (
		tsmValues = make(map[string]struct{})
		stats     cursors.CursorStats
		canceled  bool
	)

	e.FileStore.ForEachFile(func(f TSMFile) bool {
		// Check the context before accessing each tsm file
		select {
		case <-ctx.Done():
			canceled = true
			return false
		default:
		}
		if f.OverlapsTimeRange(start, end) && f.OverlapsKeyPrefixRange(prefix, prefix) {
			iter := f.TimeRangeIterator(prefix, start, end)
			for i := 0; iter.Next(); i++ {
				sfkey := iter.Key()
				if !bytes.HasPrefix(sfkey, prefix) {
					// end of org+bucket
					break
				}

				key, _ := SeriesAndFieldFromCompositeKey(sfkey)
				name, err := models.ParseMeasurement(key)
				if err != nil {
					e.logger.Error("Invalid series key in TSM index", zap.Error(err), zap.Binary("key", key))
					continue
				}

				if _, ok := tsmValues[string(name)]; ok {
					continue
				}

				if iter.HasData() {
					tsmValues[string(name)] = struct{}{}
				}
			}
			stats.Add(iter.Stats())
		}
		return true
	})

	if canceled {
		return cursors.NewStringSliceIteratorWithStats(nil, stats), ctx.Err()
	}

	var ts cursors.TimestampArray

	// With performance in mind, we explicitly do not check the context
	// while scanning the entries in the cache.
	prefixStr := string(prefix)
	_ = e.Cache.ApplyEntryFn(func(sfkey string, entry *entry) error {
		if !strings.HasPrefix(sfkey, prefixStr) {
			return nil
		}

		// TODO(edd): consider the []byte() conversion here.
		key, _ := SeriesAndFieldFromCompositeKey([]byte(sfkey))
		name, err := models.ParseMeasurement(key)
		if err != nil {
			e.logger.Error("Invalid series key in cache", zap.Error(err), zap.Binary("key", key))
			return nil
		}

		if _, ok := tsmValues[string(name)]; ok {
			return nil
		}

		ts.Timestamps = entry.AppendTimestamps(ts.Timestamps[:0])
		if ts.Len() == 0 {
			return nil
		}

		sort.Sort(&ts)

		stats.ScannedValues += ts.Len()
		stats.ScannedBytes += ts.Len() * 8 // sizeof timestamp

		if ts.Contains(start, end) {
			tsmValues[string(name)] = struct{}{}
		}
		return nil
	})

	vals := make([]string, 0, len(tsmValues))
	for val := range tsmValues {
		vals = append(vals, val)
	}
	sort.Strings(vals)

	return cursors.NewStringSliceIteratorWithStats(vals, stats), nil
}

// MeasurementTagValues returns an iterator which enumerates the tag values for the given
// bucket, measurement and tag key, filtered using the optional the predicate and limited to the
// time range [start, end].
//
// MeasurementTagValues will always return a StringIterator if there is no error.
//
// If the context is canceled before TagValues has finished processing, a non-nil
// error will be returned along with statistics for the already scanned data.
func (e *Engine) MeasurementTagValues(ctx context.Context, orgID, bucketID influxdb.ID, measurement, tagKey string, start, end int64, predicate influxql.Expr) (cursors.StringIterator, error) {
	if predicate == nil {
		return e.tagValuesNoPredicate(ctx, orgID, bucketID, []byte(measurement), []byte(tagKey), start, end)
	}

	predicate = AddMeasurementToExpr(measurement, predicate)

	return e.tagValuesPredicate(ctx, orgID, bucketID, []byte(measurement), []byte(tagKey), start, end, predicate)

}

// MeasurementTagKeys returns an iterator which enumerates the tag keys for the given
// bucket and measurement, filtered using the optional the predicate and limited to the
//// time range [start, end].
//
// MeasurementTagKeys will always return a StringIterator if there is no error.
//
// If the context is canceled before MeasurementTagKeys has finished processing, a non-nil
// error will be returned along with statistics for the already scanned data.
func (e *Engine) MeasurementTagKeys(ctx context.Context, orgID, bucketID influxdb.ID, measurement string, start, end int64, predicate influxql.Expr) (cursors.StringIterator, error) {
	if predicate == nil {
		return e.tagKeysNoPredicate(ctx, orgID, bucketID, []byte(measurement), start, end)
	}

	predicate = AddMeasurementToExpr(measurement, predicate)

	return e.tagKeysPredicate(ctx, orgID, bucketID, []byte(measurement), start, end, predicate)
}

// MeasurementFields returns an iterator which enumerates the field schema for the given
// bucket and measurement, filtered using the optional the predicate and limited to the
//// time range [start, end].
//
// MeasurementFields will always return a MeasurementFieldsIterator if there is no error.
//
// If the context is canceled before MeasurementFields has finished processing, a non-nil
// error will be returned along with statistics for the already scanned data.
func (e *Engine) MeasurementFields(ctx context.Context, orgID, bucketID influxdb.ID, measurement string, start, end int64, predicate influxql.Expr) (cursors.MeasurementFieldsIterator, error) {
	if predicate == nil {
		return e.fieldsNoPredicate(ctx, orgID, bucketID, []byte(measurement), start, end)
	}

	predicate = AddMeasurementToExpr(measurement, predicate)

	return e.fieldsPredicate(ctx, orgID, bucketID, []byte(measurement), start, end, predicate)
}

type fieldTypeTime struct {
	typ cursors.FieldType
	max int64
}

func (e *Engine) fieldsPredicate(ctx context.Context, orgID influxdb.ID, bucketID influxdb.ID, measurement []byte, start int64, end int64, predicate influxql.Expr) (cursors.MeasurementFieldsIterator, error) {
	if err := ValidateTagPredicate(predicate); err != nil {
		return nil, err
	}

	orgBucket := tsdb.EncodeName(orgID, bucketID)

	keys, err := e.findCandidateKeys(ctx, orgBucket[:], predicate)
	if err != nil {
		return cursors.EmptyMeasurementFieldsIterator, err
	}

	if len(keys) == 0 {
		return cursors.EmptyMeasurementFieldsIterator, nil
	}

	var files []TSMFile
	defer func() {
		for _, f := range files {
			f.Unref()
		}
	}()
	var iters []*TimeRangeMaxTimeIterator

	// TODO(edd): we need to clean up how we're encoding the prefix so that we
	// don't have to remember to get it right everywhere we need to touch TSM data.
	orgBucketEsc := models.EscapeMeasurement(orgBucket[:])

	mt := models.Tags{models.NewTag(models.MeasurementTagKeyBytes, measurement)}
	tsmKeyPrefix := mt.AppendHashKey(orgBucketEsc)

	var canceled bool

	e.FileStore.ForEachFile(func(f TSMFile) bool {
		// Check the context before accessing each tsm file
		select {
		case <-ctx.Done():
			canceled = true
			return false
		default:
		}
		if f.OverlapsTimeRange(start, end) && f.OverlapsKeyPrefixRange(tsmKeyPrefix, tsmKeyPrefix) {
			f.Ref()
			files = append(files, f)
			iters = append(iters, f.TimeRangeMaxTimeIterator(tsmKeyPrefix, start, end))
		}
		return true
	})

	var stats cursors.CursorStats

	if canceled {
		stats = statsFromTimeRangeMaxTimeIters(stats, iters)
		return cursors.NewMeasurementFieldsSliceIteratorWithStats(nil, stats), ctx.Err()
	}

	tsmValues := make(map[string]fieldTypeTime)

	// reusable buffers
	var (
		tags   models.Tags
		keybuf []byte
		sfkey  []byte
		ts     cursors.TimestampArray
	)

	for i := range keys {
		// to keep cache scans fast, check context every 'cancelCheckInterval' iteratons
		if i%cancelCheckInterval == 0 {
			select {
			case <-ctx.Done():
				stats = statsFromTimeRangeMaxTimeIters(stats, iters)
				return cursors.NewMeasurementFieldsSliceIteratorWithStats(nil, stats), ctx.Err()
			default:
			}
		}

		_, tags = seriesfile.ParseSeriesKeyInto(keys[i], tags[:0])
		fieldKey := tags.Get(models.FieldKeyTagKeyBytes)
		keybuf = models.AppendMakeKey(keybuf[:0], orgBucketEsc, tags)
		sfkey = AppendSeriesFieldKeyBytes(sfkey[:0], keybuf, fieldKey)

		cur := fieldTypeTime{max: InvalidMinNanoTime}

		ts.Timestamps = e.Cache.AppendTimestamps(sfkey, ts.Timestamps[:0])
		if ts.Len() > 0 {
			sort.Sort(&ts)

			stats.ScannedValues += ts.Len()
			stats.ScannedBytes += ts.Len() * 8 // sizeof timestamp

			if ts.Contains(start, end) {
				max := ts.MaxTime()
				if max > cur.max {
					cur.max = max
					cur.typ = BlockTypeToFieldType(e.Cache.BlockType(sfkey))
				}
			}
		}

		for _, iter := range iters {
			if exact, _ := iter.Seek(sfkey); !exact {
				continue
			}

			max := iter.MaxTime()
			if max > cur.max {
				cur.max = max
				cur.typ = BlockTypeToFieldType(iter.Type())
			}
		}

		if cur.max != InvalidMinNanoTime {
			tsmValues[string(fieldKey)] = cur
		}
	}

	vals := make([]cursors.MeasurementField, 0, len(tsmValues))
	for key, val := range tsmValues {
		vals = append(vals, cursors.MeasurementField{Key: key, Type: val.typ})
	}

	return cursors.NewMeasurementFieldsSliceIteratorWithStats([]cursors.MeasurementFields{{Fields: vals}}, stats), nil
}

func (e *Engine) fieldsNoPredicate(ctx context.Context, orgID influxdb.ID, bucketID influxdb.ID, measurement []byte, start int64, end int64) (cursors.MeasurementFieldsIterator, error) {
	tsmValues := make(map[string]fieldTypeTime)
	orgBucket := tsdb.EncodeName(orgID, bucketID)

	// TODO(edd): we need to clean up how we're encoding the prefix so that we
	// don't have to remember to get it right everywhere we need to touch TSM data.
	orgBucketEsc := models.EscapeMeasurement(orgBucket[:])

	mt := models.Tags{models.NewTag(models.MeasurementTagKeyBytes, measurement)}
	tsmKeyPrefix := mt.AppendHashKey(orgBucketEsc)

	var stats cursors.CursorStats
	var canceled bool

	e.FileStore.ForEachFile(func(f TSMFile) bool {
		// Check the context before touching each tsm file
		select {
		case <-ctx.Done():
			canceled = true
			return false
		default:
		}
		if f.OverlapsTimeRange(start, end) && f.OverlapsKeyPrefixRange(tsmKeyPrefix, tsmKeyPrefix) {
			// TODO(sgc): create f.TimeRangeIterator(minKey, maxKey, start, end)
			iter := f.TimeRangeMaxTimeIterator(tsmKeyPrefix, start, end)
			for i := 0; iter.Next(); i++ {
				sfkey := iter.Key()
				if !bytes.HasPrefix(sfkey, tsmKeyPrefix) {
					// end of prefix
					break
				}

				max := iter.MaxTime()
				if max == InvalidMinNanoTime {
					continue
				}

				_, fieldKey := SeriesAndFieldFromCompositeKey(sfkey)
				v, ok := tsmValues[string(fieldKey)]
				if !ok || v.max < max {
					tsmValues[string(fieldKey)] = fieldTypeTime{
						typ: BlockTypeToFieldType(iter.Type()),
						max: max,
					}
				}
			}
			stats.Add(iter.Stats())
		}
		return true
	})

	if canceled {
		return cursors.NewMeasurementFieldsSliceIteratorWithStats(nil, stats), ctx.Err()
	}

	var ts cursors.TimestampArray

	// With performance in mind, we explicitly do not check the context
	// while scanning the entries in the cache.
	tsmKeyPrefixStr := string(tsmKeyPrefix)
	_ = e.Cache.ApplyEntryFn(func(sfkey string, entry *entry) error {
		if !strings.HasPrefix(sfkey, tsmKeyPrefixStr) {
			return nil
		}

		ts.Timestamps = entry.AppendTimestamps(ts.Timestamps[:0])
		if ts.Len() == 0 {
			return nil
		}

		sort.Sort(&ts)

		stats.ScannedValues += ts.Len()
		stats.ScannedBytes += ts.Len() * 8 // sizeof timestamp

		if !ts.Contains(start, end) {
			return nil
		}

		max := ts.MaxTime()

		// TODO(edd): consider the []byte() conversion here.
		_, fieldKey := SeriesAndFieldFromCompositeKey([]byte(sfkey))
		v, ok := tsmValues[string(fieldKey)]
		if !ok || v.max < max {
			tsmValues[string(fieldKey)] = fieldTypeTime{
				typ: BlockTypeToFieldType(entry.BlockType()),
				max: max,
			}
		}

		return nil
	})

	vals := make([]cursors.MeasurementField, 0, len(tsmValues))
	for key, val := range tsmValues {
		vals = append(vals, cursors.MeasurementField{Key: key, Type: val.typ})
	}

	return cursors.NewMeasurementFieldsSliceIteratorWithStats([]cursors.MeasurementFields{{Fields: vals}}, stats), nil
}

func AddMeasurementToExpr(measurement string, base influxql.Expr) influxql.Expr {
	// \x00 = '<measurement>'
	expr := &influxql.BinaryExpr{
		LHS: &influxql.VarRef{
			Val:  models.MeasurementTagKey,
			Type: influxql.Tag,
		},
		Op: influxql.EQ,
		RHS: &influxql.StringLiteral{
			Val: measurement,
		},
	}

	if base != nil {
		// \x00 = '<measurement>' AND (base)
		expr = &influxql.BinaryExpr{
			LHS: expr,
			Op:  influxql.AND,
			RHS: &influxql.ParenExpr{
				Expr: base,
			},
		}
	}

	return expr
}

func statsFromTimeRangeMaxTimeIters(stats cursors.CursorStats, iters []*TimeRangeMaxTimeIterator) cursors.CursorStats {
	for _, iter := range iters {
		stats.Add(iter.Stats())
	}
	return stats
}

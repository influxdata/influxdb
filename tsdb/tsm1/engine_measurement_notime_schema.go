package tsm1

import (
	"context"
	"sort"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/influxdata/influxdb/v2/tsdb/cursors"
	"github.com/influxdata/influxdb/v2/tsdb/seriesfile"
	"github.com/influxdata/influxql"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/log"
)

// MeasurementNamesNoTime returns an iterator which enumerates the measurements for the given
// bucket.
//
// MeasurementNamesNoTime will always return a StringIterator if there is no error.
//
// If the context is canceled before MeasurementNamesNoTime has finished processing, a non-nil
// error will be returned along with statistics for the already scanned data.
func (e *Engine) MeasurementNamesNoTime(ctx context.Context, orgID, bucketID influxdb.ID, predicate influxql.Expr) (cursors.StringIterator, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	return e.tagValuesNoTime(ctx, orgID, bucketID, models.MeasurementTagKeyBytes, predicate)
}

// MeasurementTagValuesNoTime returns an iterator which enumerates the tag values for the given
// bucket, measurement and tag key and filtered using the optional the predicate.
//
// MeasurementTagValuesNoTime will always return a StringIterator if there is no error.
//
// If the context is canceled before MeasurementTagValuesNoTime has finished processing, a non-nil
// error will be returned along with statistics for the already scanned data.
func (e *Engine) MeasurementTagValuesNoTime(ctx context.Context, orgID, bucketID influxdb.ID, measurement, tagKey string, predicate influxql.Expr) (cursors.StringIterator, error) {
	predicate = AddMeasurementToExpr(measurement, predicate)

	return e.tagValuesNoTime(ctx, orgID, bucketID, []byte(tagKey), predicate)
}

func (e *Engine) tagValuesNoTime(ctx context.Context, orgID, bucketID influxdb.ID, tagKeyBytes []byte, predicate influxql.Expr) (cursors.StringIterator, error) {
	if err := ValidateTagPredicate(predicate); err != nil {
		return nil, err
	}

	orgBucket := tsdb.EncodeName(orgID, bucketID)

	// fetch distinct values for tag key in bucket
	itr, err := e.index.TagValueIterator(orgBucket[:], tagKeyBytes)
	if err != nil {
		return nil, err
	} else if itr == nil {
		return cursors.NewStringSliceIterator(nil), err
	}
	defer itr.Close()

	var (
		vals = make([]string, 0, 128)
	)

	span := opentracing.SpanFromContext(ctx)
	if span != nil {
		defer func() {
			span.LogFields(
				log.Int("values_count", len(vals)),
			)
		}()
	}

	// reusable buffers
	var (
		tagKey = string(tagKeyBytes)
	)

	for i := 0; ; i++ {
		// to keep cache scans fast, check context every 'cancelCheckInterval' iterations
		if i%cancelCheckInterval == 0 {
			select {
			case <-ctx.Done():
				return cursors.NewStringSliceIterator(nil), ctx.Err()
			default:
			}
		}

		val, err := itr.Next()
		if err != nil {
			return cursors.NewStringSliceIterator(nil), err
		} else if len(val) == 0 {
			break
		}

		// <tagKey> = val
		var expr influxql.Expr = &influxql.BinaryExpr{
			LHS: &influxql.VarRef{Val: tagKey, Type: influxql.Tag},
			Op:  influxql.EQ,
			RHS: &influxql.StringLiteral{Val: string(val)},
		}

		if predicate != nil {
			// <tagKey> = val AND (expr)
			expr = &influxql.BinaryExpr{
				LHS: expr,
				Op:  influxql.AND,
				RHS: &influxql.ParenExpr{
					Expr: predicate,
				},
			}
		}

		if err := func() error {
			sitr, err := e.index.MeasurementSeriesByExprIterator(orgBucket[:], expr)
			if err != nil {
				return err
			}
			defer sitr.Close()

			if elem, err := sitr.Next(); err != nil {
				return err
			} else if !elem.SeriesID.IsZero() {
				vals = append(vals, string(val))
			}
			return nil
		}(); err != nil {
			return cursors.NewStringSliceIterator(nil), err
		}
	}

	sort.Strings(vals)
	return cursors.NewStringSliceIterator(vals), err
}

// MeasurementFieldsNoTime returns an iterator which enumerates the field schema for the given
// bucket and measurement, filtered using the optional the predicate.
//
// MeasurementFieldsNoTime will always return a MeasurementFieldsIterator if there is no error.
//
// If the context is canceled before MeasurementFieldsNoTime has finished processing, a non-nil
// error will be returned along with statistics for the already scanned data.
func (e *Engine) MeasurementFieldsNoTime(ctx context.Context, orgID, bucketID influxdb.ID, measurement string, predicate influxql.Expr) (cursors.MeasurementFieldsIterator, error) {
	predicate = AddMeasurementToExpr(measurement, predicate)

	return e.fieldsNoTime(ctx, orgID, bucketID, []byte(measurement), predicate)
}

func (e *Engine) fieldsNoTime(ctx context.Context, orgID, bucketID influxdb.ID, measurement []byte, predicate influxql.Expr) (cursors.MeasurementFieldsIterator, error) {
	type fieldKeyType struct {
		key []byte
		typ cursors.FieldType
	}

	if err := ValidateTagPredicate(predicate); err != nil {
		return nil, err
	}

	orgBucket := tsdb.EncodeName(orgID, bucketID)

	// fetch distinct values for field, which may be a superset of the measurement
	itr, err := e.index.TagValueIterator(orgBucket[:], models.FieldKeyTagKeyBytes)
	if err != nil {
		return nil, err
	}
	defer itr.Close()

	var (
		fieldTypes = make([]fieldKeyType, 0, 128)
	)

	span := opentracing.SpanFromContext(ctx)
	if span != nil {
		defer func() {
			span.LogFields(
				log.Int("values_count", len(fieldTypes)),
			)
		}()
	}

	for i := 0; ; i++ {
		// to keep cache scans fast, check context every 'cancelCheckInterval' iterations
		if i%cancelCheckInterval == 0 {
			select {
			case <-ctx.Done():
				return cursors.NewMeasurementFieldsSliceIterator(nil), ctx.Err()
			default:
			}
		}

		val, err := itr.Next()
		if err != nil {
			return cursors.NewMeasurementFieldsSliceIterator(nil), err
		} else if len(val) == 0 {
			break
		}

		// <tagKey> = val
		var expr influxql.Expr = &influxql.BinaryExpr{
			LHS: &influxql.VarRef{Val: models.FieldKeyTagKey, Type: influxql.Tag},
			Op:  influxql.EQ,
			RHS: &influxql.StringLiteral{Val: string(val)},
		}

		if predicate != nil {
			// <tagKey> = val AND (expr)
			expr = &influxql.BinaryExpr{
				LHS: expr,
				Op:  influxql.AND,
				RHS: &influxql.ParenExpr{
					Expr: predicate,
				},
			}
		}

		if err := func() error {
			sitr, err := e.index.MeasurementSeriesByExprIterator(orgBucket[:], expr)
			if err != nil {
				return err
			}
			defer sitr.Close()

			if elem, err := sitr.Next(); err != nil {
				return err
			} else if !elem.SeriesID.IsZero() {
				key := e.sfile.SeriesKey(elem.SeriesID)
				typedID := e.sfile.SeriesIDTypedBySeriesKey(key)
				fieldTypes = append(fieldTypes, fieldKeyType{key: val, typ: cursors.ModelsFieldTypeToFieldType(typedID.Type())})
			}
			return nil
		}(); err != nil {
			return cursors.NewMeasurementFieldsSliceIterator(nil), err
		}
	}

	vals := make([]cursors.MeasurementField, 0, len(fieldTypes))
	for i := range fieldTypes {
		val := &fieldTypes[i]
		vals = append(vals, cursors.MeasurementField{Key: string(val.key), Type: val.typ, Timestamp: 0})
	}

	return cursors.NewMeasurementFieldsSliceIterator([]cursors.MeasurementFields{{Fields: vals}}), nil
}

// MeasurementTagKeysNoTime returns an iterator which enumerates the tag keys
// for the given bucket, measurement and tag key and filtered using the optional
// the predicate.
//
// MeasurementTagKeysNoTime will always return a StringIterator if there is no error.
//
// If the context is canceled before MeasurementTagKeysNoTime has finished
// processing, a non-nil error will be returned along with statistics for the
// already scanned data.
func (e *Engine) MeasurementTagKeysNoTime(ctx context.Context, orgID, bucketID influxdb.ID, measurement string, predicate influxql.Expr) (cursors.StringIterator, error) {
	if measurement != "" {
		predicate = AddMeasurementToExpr(measurement, predicate)
	}
	return e.tagKeysNoTime(ctx, orgID, bucketID, predicate)
}

func (e *Engine) tagKeysNoTime(ctx context.Context, orgID, bucketID influxdb.ID, predicate influxql.Expr) (cursors.StringIterator, error) {
	if err := ValidateTagPredicate(predicate); err != nil {
		return nil, err
	}

	orgBucket := tsdb.EncodeName(orgID, bucketID)

	vals := make([]string, 0, 32)

	span := opentracing.SpanFromContext(ctx)
	if span != nil {
		defer func() {
			span.LogFields(
				log.Int("values_count", len(vals)),
			)
		}()
	}

	var (
		km   keyMerger
		keys = make([][]byte, 0, 32)
	)

	if err := func() error {
		sitr, err := e.index.MeasurementSeriesByExprIterator(orgBucket[:], predicate)
		if err != nil {
			return err
		}
		defer sitr.Close()

		for i := 0; ; i++ {
			// to keep cache scans fast, check context every 'cancelCheckInterval' iterations
			if i%cancelCheckInterval == 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
			}

			elem, err := sitr.Next()
			if err != nil {
				return err
			} else if elem.SeriesID.IsZero() {
				return nil
			}

			sf := e.index.SeriesFile()
			if sf == nil {
				return nil
			}

			skey := sf.SeriesKey(elem.SeriesID)
			if len(skey) == 0 {
				continue
			}

			keys = parseSeriesKeys(skey, keys)
			km.MergeKeys(keys)
		}
		return nil
	}(); err != nil {
		return cursors.NewStringSliceIterator(nil), err
	}

	for _, v := range km.Get() {
		vals = append(vals, string(v))
	}

	return cursors.NewStringSliceIterator(vals), nil
}

// parseSeriesKeys is adapted from seriesfile.ParseSeriesKeyInto. Instead of
// returning the full tag information, it only returns the keys.
func parseSeriesKeys(data []byte, dst [][]byte) [][]byte {
	_, data = seriesfile.ReadSeriesKeyLen(data)
	_, data = seriesfile.ReadSeriesKeyMeasurement(data)
	tagN, data := seriesfile.ReadSeriesKeyTagN(data)

	if cap(dst) < tagN {
		dst = make([][]byte, tagN)
	} else {
		dst = dst[:tagN]
	}

	for i := 0; i < tagN; i++ {
		dst[i], _, data = seriesfile.ReadSeriesKeyTag(data)
	}

	return dst
}

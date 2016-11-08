package tsi1

import (
	"bytes"
	"fmt"
	"regexp"
	"sort"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/estimator"
	"github.com/influxdata/influxdb/tsdb"
)

// Ensure index implements the interface.
var _ tsdb.Index = &Index{}

// Index represents a collection of layered index files and WAL.
type Index struct {
	logFiles   []*LogFile
	indexFiles IndexFiles
}

// Open opens the index.
func (i *Index) Open() error { panic("TODO") }

// Close closes the index.
func (i *Index) Close() error { panic("TODO") }

// SetLogFiles explicitly sets log files.
// TEMPORARY: For testing only.
func (i *Index) SetLogFiles(a ...*LogFile) { i.logFiles = a }

// SetIndexFiles explicitly sets index files
// TEMPORARY: For testing only.
func (i *Index) SetIndexFiles(a ...*IndexFile) { i.indexFiles = IndexFiles(a) }

// FileN returns the number of log and index files within the index.
func (i *Index) FileN() int { return len(i.logFiles) + len(i.indexFiles) }

func (i *Index) CreateMeasurementIndexIfNotExists(name []byte) (*tsdb.Measurement, error) {
	// FIXME(benbjohnson): Read lock log file during lookup.
	if mm := i.measurement(name); mm == nil {
		return mm, nil
	}
	return i.logFiles[0].CreateMeasurementIndexIfNotExists(name)
}

// Measurement retrieves a measurement by name.
func (i *Index) Measurement(name []byte) (*tsdb.Measurement, error) {
	return i.measurement(name), nil
}

func (i *Index) measurement(name []byte) *tsdb.Measurement {
	m := tsdb.NewMeasurement(string(name))

	// Iterate over measurement series.
	itr := i.MeasurementSeriesIterator(name)

	var id uint64 // TEMPORARY
	for e := itr.Next(); e != nil; e = itr.Next() {
		// TODO: Handle deleted series.

		// Append series to to measurement.
		// TODO: Remove concept of series ids.
		m.AddSeries(&tsdb.Series{
			ID:   id,
			Key:  string(e.Name()),
			Tags: models.CopyTags(e.Tags()),
		})

		// TEMPORARY: Increment ID.
		id++
	}

	if !m.HasSeries() {
		return nil
	}
	return m
}

// MeasurementSeriesIterator returns an iterator over all series in the index.
func (i *Index) MeasurementSeriesIterator(name []byte) SeriesIterator {
	a := make([]SeriesIterator, 0, i.FileN())
	for _, f := range i.logFiles {
		a = append(a, f.MeasurementSeriesIterator(name))
	}
	for _, f := range i.indexFiles {
		a = append(a, f.MeasurementSeriesIterator(name))
	}
	return MergeSeriesIterators(a...)
}

// Measurements returns a list of all measurements.
func (i *Index) Measurements() (tsdb.Measurements, error) {
	var mms tsdb.Measurements
	itr := i.MeasurementIterator()
	for e := itr.Next(); e != nil; e = itr.Next() {
		mms = append(mms, i.measurement(e.Name()))
	}
	return mms, nil
}

// MeasurementIterator returns an iterator over all measurements in the index.
func (i *Index) MeasurementIterator() MeasurementIterator {
	a := make([]MeasurementIterator, 0, i.FileN())
	for _, f := range i.logFiles {
		a = append(a, f.MeasurementIterator())
	}
	for _, f := range i.indexFiles {
		a = append(a, f.MeasurementIterator())
	}
	return MergeMeasurementIterators(a...)
}

func (i *Index) MeasurementsByExpr(expr influxql.Expr) (tsdb.Measurements, bool, error) {
	return i.measurementsByExpr(expr)
}

func (i *Index) measurementsByExpr(expr influxql.Expr) (tsdb.Measurements, bool, error) {
	if expr == nil {
		return nil, false, nil
	}

	switch e := expr.(type) {
	case *influxql.BinaryExpr:
		switch e.Op {
		case influxql.EQ, influxql.NEQ, influxql.EQREGEX, influxql.NEQREGEX:
			tag, ok := e.LHS.(*influxql.VarRef)
			if !ok {
				return nil, false, fmt.Errorf("left side of '%s' must be a tag key", e.Op.String())
			}

			// Retrieve value or regex expression from RHS.
			var value string
			var regex *regexp.Regexp
			if influxql.IsRegexOp(e.Op) {
				re, ok := e.RHS.(*influxql.RegexLiteral)
				if !ok {
					return nil, false, fmt.Errorf("right side of '%s' must be a regular expression", e.Op.String())
				}
				regex = re.Val
			} else {
				s, ok := e.RHS.(*influxql.StringLiteral)
				if !ok {
					return nil, false, fmt.Errorf("right side of '%s' must be a tag value string", e.Op.String())
				}
				value = s.Val
			}

			// Match on name, if specified.
			if tag.Val == "_name" {
				return i.measurementsByNameFilter(e.Op, value, regex), true, nil
			} else if influxql.IsSystemName(tag.Val) {
				return nil, false, nil
			}
			return i.measurementsByTagFilter(e.Op, tag.Val, value, regex), true, nil

		case influxql.OR, influxql.AND:
			lhsIDs, lhsOk, err := i.measurementsByExpr(e.LHS)
			if err != nil {
				return nil, false, err
			}

			rhsIDs, rhsOk, err := i.measurementsByExpr(e.RHS)
			if err != nil {
				return nil, false, err
			}

			if lhsOk && rhsOk {
				if e.Op == influxql.OR {
					return lhsIDs.Union(rhsIDs), true, nil
				}
				return lhsIDs.Intersect(rhsIDs), true, nil
			} else if lhsOk {
				return lhsIDs, true, nil
			} else if rhsOk {
				return rhsIDs, true, nil
			}
			return nil, false, nil

		default:
			return nil, false, fmt.Errorf("invalid tag comparison operator")
		}

	case *influxql.ParenExpr:
		return i.measurementsByExpr(e.Expr)
	default:
		return nil, false, fmt.Errorf("%#v", expr)
	}
}

// measurementsByNameFilter returns the sorted measurements matching a name.
func (i *Index) measurementsByNameFilter(op influxql.Token, val string, regex *regexp.Regexp) tsdb.Measurements {
	var mms tsdb.Measurements
	itr := i.MeasurementIterator()
	for e := itr.Next(); e != nil; e = itr.Next() {
		var matched bool
		switch op {
		case influxql.EQ:
			matched = string(e.Name()) == val
		case influxql.NEQ:
			matched = string(e.Name()) != val
		case influxql.EQREGEX:
			matched = regex.Match(e.Name())
		case influxql.NEQREGEX:
			matched = !regex.Match(e.Name())
		}

		if matched {
			mms = append(mms, i.measurement(e.Name()))
		}
	}
	sort.Sort(mms)
	return mms
}

func (i *Index) measurementsByTagFilter(op influxql.Token, key, val string, regex *regexp.Regexp) tsdb.Measurements {
	var mms tsdb.Measurements
	itr := i.MeasurementIterator()
	for e := itr.Next(); e != nil; e = itr.Next() {
		mm := i.measurement(e.Name())

		tagVals := mm.SeriesByTagKeyValue(key)
		if tagVals == nil {
			continue
		}

		// If the operator is non-regex, only check the specified value.
		var tagMatch bool
		if op == influxql.EQ || op == influxql.NEQ {
			if _, ok := tagVals[val]; ok {
				tagMatch = true
			}
		} else {
			// Else, the operator is a regex and we have to check all tag
			// values against the regular expression.
			for tagVal := range tagVals {
				if regex.MatchString(tagVal) {
					tagMatch = true
					break
				}
			}
		}

		//
		// XNOR gate
		//
		// tags match | operation is EQ | measurement matches
		// --------------------------------------------------
		//     True   |       True      |      True
		//     True   |       False     |      False
		//     False  |       True      |      False
		//     False  |       False     |      True
		if tagMatch == (op == influxql.EQ || op == influxql.EQREGEX) {
			mms = append(mms, mm)
			break
		}
	}

	sort.Sort(mms)
	return mms
}

func (i *Index) MeasurementsByName(names [][]byte) ([]*tsdb.Measurement, error) {
	itr := i.MeasurementIterator()
	mms := make([]*tsdb.Measurement, 0, len(names))
	for e := itr.Next(); e != nil; e = itr.Next() {
		for _, name := range names {
			if bytes.Equal(e.Name(), name) {
				mms = append(mms, i.measurement(e.Name()))
				break
			}
		}
	}
	return mms, nil
}

func (i *Index) MeasurementsByRegex(re *regexp.Regexp) (tsdb.Measurements, error) {
	itr := i.MeasurementIterator()
	var mms tsdb.Measurements
	for e := itr.Next(); e != nil; e = itr.Next() {
		if re.Match(e.Name()) {
			mms = append(mms, i.measurement(e.Name()))
		}
	}
	return mms, nil
}

func (i *Index) DropMeasurement(name []byte) error {
	panic("TODO: Requires WAL")
}

func (i *Index) CreateSeriesIndexIfNotExists(measurement []byte, series *tsdb.Series) (*tsdb.Series, error) {
	panic("TODO: Requires WAL")
}

func (i *Index) Series(key []byte) (*tsdb.Series, error) {
	panic("TODO")
}

func (i *Index) DropSeries(keys [][]byte) error {
	panic("TODO: Requires WAL")
}

func (i *Index) SeriesN() (n uint64, err error) {
	panic("TODO: Use sketches")

	// itr := i.file.MeasurementIterator()
	// for e := itr.Next(); e != nil; e = itr.Next() {
	// 	n += uint64(e.SeriesN)
	// }
	// return n, nil
}

func (i *Index) TagsForSeries(key []byte) (models.Tags, error) {
	ss, err := i.Series([]byte(key))
	if err != nil {
		return nil, err
	}
	return ss.Tags, nil
}

func (i *Index) SeriesSketches() (estimator.Sketch, estimator.Sketch, error) {
	panic("TODO")
}

func (i *Index) MeasurementsSketches() (estimator.Sketch, estimator.Sketch, error) {
	panic("TODO")
}

// Dereference is a nop.
func (i *Index) Dereference([]byte) {}

// TagKeySeriesIterator returns a series iterator for all values across a single key.
func (i *Index) TagKeySeriesIterator(name, key []byte) SeriesIterator {
	panic("TODO")
}

// TagValueSeriesIterator returns a series iterator for a single tag value.
func (i *Index) TagValueSeriesIterator(name, key, value []byte) SeriesIterator {
	panic("TODO")
}

// MatchTagValueSeriesIterator returns a series iterator for tags which match value.
// If matches is false, returns iterators which do not match value.
func (i *Index) MatchTagValueSeriesIterator(name, key []byte, value *regexp.Regexp, matches bool) SeriesIterator {
	panic("TODO")

	/*
		// Check if we match the empty string to see if we should include series
		// that are missing the tag.
		empty := value.MatchString("")

		// Gather the series that match the regex. If we should include the empty string,
		// start with the list of all series and reject series that don't match our condition.
		// If we should not include the empty string, include series that match our condition.
		if op == influxql.EQREGEX {

			if empty {
				// See comments above for EQ with a StringLiteral.
				seriesIDs := newEvictSeriesIDs(m.seriesIDs)
				for k := range tagVals {
					if !re.Val.MatchString(k) {
						seriesIDs.mark(tagVals[k])
					}
				}
				return seriesIDs.evict(), nil, nil
			}
			ids = make(SeriesIDs, 0, len(m.seriesIDs))
			for k := range tagVals {
				if re.Val.MatchString(k) {
					ids = append(ids, tagVals[k]...)
				}
			}
			sort.Sort(ids)
			return ids, nil, nil

		}

		// Compare not-equal to empty string.
		if empty {
			ids = make(SeriesIDs, 0, len(m.seriesIDs))
			for k := range tagVals {
				if !re.Val.MatchString(k) {
					ids = append(ids, tagVals[k]...)
				}
			}
			sort.Sort(ids)
			return ids, nil, nil
		}

		// Compare not-equal to empty string.
		seriesIDs := newEvictSeriesIDs(m.seriesIDs)
		for k := range tagVals {
			if re.Val.MatchString(k) {
				seriesIDs.mark(tagVals[k])
			}
		}

		return seriesIDs.evict(), nil, nil
	*/
}

// TagSets returns an ordered list of tag sets for a measurement by dimension
// and filtered by an optional conditional expression.
func (i *Index) TagSets(name []byte, dimensions []string, condition influxql.Expr) ([]*influxql.TagSet, error) {
	var tagSets []*influxql.TagSet
	// TODO(benbjohnson): Iterate over filtered series and build tag sets.
	return tagSets, nil
}

// MeasurementSeriesByExprIterator returns a series iterator for a measurement
// that is filtered by expr. If expr only contains time expressions then this
// call is equivalent to MeasurementSeriesIterator().
func (i *Index) MeasurementSeriesByExprIterator(name []byte, expr influxql.Expr) (SeriesIterator, error) {
	// Return all series for the measurement if there are no tag expressions.
	if expr == nil || influxql.OnlyTimeExpr(expr) {
		return i.MeasurementSeriesIterator(name), nil
	}
	return i.seriesByExprIterator(name, expr)
}

func (i *Index) seriesByExprIterator(name []byte, expr influxql.Expr) (SeriesIterator, error) {
	switch expr := expr.(type) {
	case *influxql.BinaryExpr:
		switch expr.Op {
		case influxql.AND, influxql.OR:
			// Get the series IDs and filter expressions for the LHS.
			litr, err := i.seriesByExprIterator(name, expr.LHS)
			if err != nil {
				return nil, err
			}

			// Get the series IDs and filter expressions for the RHS.
			ritr, err := i.seriesByExprIterator(name, expr.RHS)
			if err != nil {
				return nil, err
			}

			// Intersect iterators if expression is "AND".
			if expr.Op == influxql.AND {
				return IntersectSeriesIterators(litr, ritr), nil
			}

			// Union iterators if expression is "OR".
			return UnionSeriesIterators(litr, ritr), nil

		default:
			return i.seriesByBinaryExprIterator(name, expr)
		}

	case *influxql.ParenExpr:
		return i.seriesByExprIterator(name, expr.Expr)

	default:
		return nil, nil
	}
}

// seriesByBinaryExprIterator returns a series iterator and a filtering expression.
func (i *Index) seriesByBinaryExprIterator(name []byte, n *influxql.BinaryExpr) (SeriesIterator, error) {
	// If this binary expression has another binary expression, then this
	// is some expression math and we should just pass it to the underlying query.
	if _, ok := n.LHS.(*influxql.BinaryExpr); ok {
		return newSeriesExprIterator(i.MeasurementSeriesIterator(name), n), nil
	} else if _, ok := n.RHS.(*influxql.BinaryExpr); ok {
		return newSeriesExprIterator(i.MeasurementSeriesIterator(name), n), nil
	}

	// Retrieve the variable reference from the correct side of the expression.
	key, ok := n.LHS.(*influxql.VarRef)
	value := n.RHS
	if !ok {
		key, ok = n.RHS.(*influxql.VarRef)
		if !ok {
			return nil, fmt.Errorf("invalid expression: %s", n.String())
		}
		value = n.LHS
	}

	// For time literals, return all series and "true" as the filter.
	if _, ok := value.(*influxql.TimeLiteral); ok || key.Val == "time" {
		return newSeriesExprIterator(i.MeasurementSeriesIterator(name), &influxql.BooleanLiteral{Val: true}), nil
	}

	/*
		// For fields, return all series from this measurement.
		if key.Val != "_name" && ((key.Type == influxql.Unknown && i.hasField(key.Val)) || key.Type == influxql.AnyField || (key.Type != influxql.Tag && key.Type != influxql.Unknown)) {
			return newSeriesExprIterator(i.MeasurementSeriesIterator(name), n), nil
		} else if value, ok := value.(*influxql.VarRef); ok {
			// Check if the RHS is a variable and if it is a field.
			if value.Val != "_name" && ((value.Type == influxql.Unknown && i.hasField(value.Val)) || key.Type == influxql.AnyField || (value.Type != influxql.Tag && value.Type != influxql.Unknown)) {
				return newSeriesExprIterator(i.MeasurementSeriesIterator(name), n), nil
			}
		}
	*/

	// Create iterator based on value type.
	switch value := value.(type) {
	case *influxql.StringLiteral:
		return i.seriesByBinaryExprStringIterator(name, []byte(key.Val), []byte(value.Val), n.Op)
	case *influxql.RegexLiteral:
		return i.seriesByBinaryExprRegexIterator(name, []byte(key.Val), value.Val, n.Op)
	case *influxql.VarRef:
		return i.seriesByBinaryExprVarRefIterator(name, []byte(key.Val), value, n.Op)
	default:
		if n.Op == influxql.NEQ || n.Op == influxql.NEQREGEX {
			return i.MeasurementSeriesIterator(name), nil
		}
		return nil, nil
	}
}

func (i *Index) seriesByBinaryExprStringIterator(name, key, value []byte, op influxql.Token) (SeriesIterator, error) {
	// Special handling for "_name" to match measurement name.
	if bytes.Equal(key, []byte("_name")) {
		if (op == influxql.EQ && bytes.Equal(value, name)) || (op == influxql.NEQ && !bytes.Equal(value, name)) {
			return i.MeasurementSeriesIterator(name), nil
		}
		return nil, nil
	}

	if op == influxql.EQ {
		// Match a specific value.
		if len(value) != 0 {
			return i.TagValueSeriesIterator(name, key, value), nil
		}

		// Return all measurement series that have no values from this tag key.
		return DifferenceSeriesIterators(
			i.MeasurementSeriesIterator(name),
			i.TagKeySeriesIterator(name, key),
		), nil
	}

	// Return all measurement series without this tag value.
	if len(value) != 0 {
		return DifferenceSeriesIterators(
			i.MeasurementSeriesIterator(name),
			i.TagValueSeriesIterator(name, key, value),
		), nil
	}

	// Return all series across all values of this tag key.
	return i.TagKeySeriesIterator(name, key), nil
}

func (i *Index) seriesByBinaryExprRegexIterator(name, key []byte, value *regexp.Regexp, op influxql.Token) (SeriesIterator, error) {
	// Special handling for "_name" to match measurement name.
	if bytes.Equal(key, []byte("_name")) {
		match := value.Match(name)
		if (op == influxql.EQREGEX && match) || (op == influxql.NEQREGEX && !match) {
			return newSeriesExprIterator(i.MeasurementSeriesIterator(name), &influxql.BooleanLiteral{Val: true}), nil
		}
		return nil, nil
	}
	return i.MatchTagValueSeriesIterator(name, key, value, op == influxql.EQREGEX), nil
}

func (i *Index) seriesByBinaryExprVarRefIterator(name, key []byte, value *influxql.VarRef, op influxql.Token) (SeriesIterator, error) {
	if op == influxql.EQ {
		return IntersectSeriesIterators(
			i.TagKeySeriesIterator(name, key),
			i.TagKeySeriesIterator(name, []byte(value.Val)),
		), nil
	}

	return DifferenceSeriesIterators(
		i.TagKeySeriesIterator(name, key),
		i.TagKeySeriesIterator(name, []byte(value.Val)),
	), nil
}

// FilterExprs represents a map of series IDs to filter expressions.
type FilterExprs map[uint64]influxql.Expr

// DeleteBoolLiteralTrues deletes all elements whose filter expression is a boolean literal true.
func (fe FilterExprs) DeleteBoolLiteralTrues() {
	for id, expr := range fe {
		if e, ok := expr.(*influxql.BooleanLiteral); ok && e.Val == true {
			delete(fe, id)
		}
	}
}

// Len returns the number of elements.
func (fe FilterExprs) Len() int {
	if fe == nil {
		return 0
	}
	return len(fe)
}

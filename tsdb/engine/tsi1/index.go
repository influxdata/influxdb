package tsi1

import (
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
	file *IndexFile

	// TODO(benbjohnson): Use layered list of index files.

	// TODO(benbjohnson): Add write ahead log.
}

// Open opens the index.
func (i *Index) Open() error { panic("TODO") }

// Close closes the index.
func (i *Index) Close() error { panic("TODO") }

// SetFile explicitly sets a file in the index.
func (i *Index) SetFile(f *IndexFile) { i.file = f }

func (i *Index) CreateMeasurementIndexIfNotExists(name string) (*tsdb.Measurement, error) {
	panic("TODO: Requires WAL")
}

// Measurement retrieves a measurement by name.
func (i *Index) Measurement(name []byte) (*tsdb.Measurement, error) {
	return i.measurement(name), nil
}

func (i *Index) measurement(name []byte) *tsdb.Measurement {
	m := tsdb.NewMeasurement(string(name))

	// Iterate over measurement series.
	itr := i.file.MeasurementSeriesIterator(name)

	var id uint64 // TEMPORARY
	var sname []byte
	var tags models.Tags
	var deleted bool
	for {
		if itr.Next(&sname, &tags, &deleted); sname == nil {
			break
		}

		// TODO: Handle deleted series.

		// Append series to to measurement.
		// TODO: Remove concept of series ids.
		m.AddSeries(&tsdb.Series{
			ID:   id,
			Key:  string(sname),
			Tags: models.CopyTags(tags),
		})

		// TEMPORARY: Increment ID.
		id++
	}

	if !m.HasSeries() {
		return nil
	}
	return m
}

// Measurements returns a list of all measurements.
func (i *Index) Measurements() (tsdb.Measurements, error) {
	var mms tsdb.Measurements
	itr := i.file.MeasurementIterator()
	for e := itr.Next(); e != nil; e = itr.Next() {
		mms = append(mms, i.measurement(e.Name))
	}
	return mms, nil
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
	itr := i.file.MeasurementIterator()
	for e := itr.Next(); e != nil; e = itr.Next() {
		var matched bool
		switch op {
		case influxql.EQ:
			matched = string(e.Name) == val
		case influxql.NEQ:
			matched = string(e.Name) != val
		case influxql.EQREGEX:
			matched = regex.Match(e.Name)
		case influxql.NEQREGEX:
			matched = !regex.Match(e.Name)
		}

		if matched {
			mms = append(mms, i.measurement(e.Name))
		}
	}
	sort.Sort(mms)
	return mms
}

func (i *Index) measurementsByTagFilter(op influxql.Token, key, val string, regex *regexp.Regexp) tsdb.Measurements {
	var mms tsdb.Measurements
	itr := i.file.MeasurementIterator()
	for e := itr.Next(); e != nil; e = itr.Next() {
		mm := i.measurement(e.Name)

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

func (i *Index) MeasurementsByName(names []string) ([]*tsdb.Measurement, error) {
	itr := i.file.MeasurementIterator()
	mms := make([]*tsdb.Measurement, 0, len(names))
	for e := itr.Next(); e != nil; e = itr.Next() {
		for _, name := range names {
			if string(e.Name) == name {
				mms = append(mms, i.measurement(e.Name))
				break
			}
		}
	}
	return mms, nil
}

func (i *Index) MeasurementsByRegex(re *regexp.Regexp) (tsdb.Measurements, error) {
	itr := i.file.MeasurementIterator()
	var mms tsdb.Measurements
	for e := itr.Next(); e != nil; e = itr.Next() {
		if re.Match(e.Name) {
			mms = append(mms, i.measurement(e.Name))
		}
	}
	return mms, nil
}

func (i *Index) DropMeasurement(name []byte) error {
	panic("TODO: Requires WAL")
}

func (i *Index) CreateSeriesIndexIfNotExists(measurement string, series *tsdb.Series) (*tsdb.Series, error) {
	panic("TODO: Requires WAL")
}

func (i *Index) Series(key []byte) (*tsdb.Series, error) {
	panic("TODO")
}

func (i *Index) DropSeries(keys []string) error {
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

func (i *Index) TagsForSeries(key string) (models.Tags, error) {
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

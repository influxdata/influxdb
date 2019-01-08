package storage

import (
	"errors"
	"sort"
	"sync"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/tsi1"
	"github.com/influxdata/influxql"
)

type SeriesCursor interface {
	Close() error
	Next() (*SeriesCursorRow, error)
}

type SeriesCursorRequest struct {
	Measurements tsdb.MeasurementIterator
}

// seriesCursor is an implementation of SeriesCursor over an tsi1.Index.
type seriesCursor struct {
	once  sync.Once
	index *tsi1.Index
	mitr  tsdb.MeasurementIterator
	keys  [][]byte
	ofs   int
	row   SeriesCursorRow
	cond  influxql.Expr
}

type SeriesCursorRow struct {
	Name []byte
	Tags models.Tags
}

// newSeriesCursor returns a new instance of SeriesCursor.
func newSeriesCursor(req SeriesCursorRequest, index *tsi1.Index, cond influxql.Expr) (_ SeriesCursor, err error) {
	// Only equality operators are allowed.
	influxql.WalkFunc(cond, func(node influxql.Node) {
		switch n := node.(type) {
		case *influxql.BinaryExpr:
			switch n.Op {
			case influxql.EQ, influxql.NEQ, influxql.EQREGEX, influxql.NEQREGEX, influxql.OR, influxql.AND:
			default:
				err = errors.New("invalid tag comparison operator")
			}
		}
	})
	if err != nil {
		return nil, err
	}

	mitr := req.Measurements
	if mitr == nil {
		mitr, err = index.MeasurementIterator()
		if err != nil {
			return nil, err
		}
	}

	return &seriesCursor{
		index: index,
		mitr:  mitr,
		cond:  cond,
	}, nil
}

// Close closes the iterator.
func (cur *seriesCursor) Close() (err error) {
	cur.once.Do(func() {
		if cur.mitr != nil {
			err = cur.mitr.Close()
		}
	})
	return err
}

// Next emits the next point in the iterator.
func (cur *seriesCursor) Next() (*SeriesCursorRow, error) {
	for {
		// Read series keys for next measurement if no more keys remaining.
		// Exit if there are no measurements remaining.
		if cur.ofs == len(cur.keys) {
			m, err := cur.mitr.Next()
			if err != nil {
				return nil, err
			} else if m == nil {
				return nil, nil
			}

			if err := cur.readSeriesKeys(m); err != nil {
				return nil, err
			}
			continue
		}

		cur.row.Name, cur.row.Tags = tsdb.ParseSeriesKey(cur.keys[cur.ofs])
		cur.ofs++
		return &cur.row, nil
	}
}

func (cur *seriesCursor) readSeriesKeys(name []byte) error {
	sitr, err := cur.index.MeasurementSeriesByExprIterator(name, cur.cond)
	if err != nil {
		return err
	} else if sitr == nil {
		return nil
	}
	defer sitr.Close()

	// Slurp all series keys.
	cur.ofs = 0
	cur.keys = cur.keys[:0]
	for {
		elem, err := sitr.Next()
		if err != nil {
			return err
		} else if elem.SeriesID.IsZero() {
			break
		}

		key := cur.index.SeriesFile().SeriesKey(elem.SeriesID)
		if len(key) == 0 {
			continue
		}
		cur.keys = append(cur.keys, key)
	}

	// Sort keys.
	sort.Sort(seriesKeys(cur.keys))
	return nil
}

type seriesKeys [][]byte

func (a seriesKeys) Len() int      { return len(a) }
func (a seriesKeys) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a seriesKeys) Less(i, j int) bool {
	return tsdb.CompareSeriesKeys(a[i], a[j]) == -1
}

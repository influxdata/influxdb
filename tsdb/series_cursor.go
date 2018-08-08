package tsdb

import (
	"bytes"
	"errors"
	"sort"
	"sync"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxql"
)

type SeriesCursor interface {
	Close() error
	Next() (*SeriesCursorRow, error)
}

type SeriesCursorRequest struct {
	Measurements MeasurementIterator
}

// seriesCursor is an implementation of SeriesCursor over an IndexSet.
type seriesCursor struct {
	once     sync.Once
	indexSet IndexSet
	mitr     MeasurementIterator
	keys     [][]byte
	ofs      int
	row      SeriesCursorRow
	cond     influxql.Expr
}

type SeriesCursorRow struct {
	Name []byte
	Tags models.Tags
}

func (r *SeriesCursorRow) Compare(other *SeriesCursorRow) int {
	if r == other {
		return 0
	} else if r == nil {
		return -1
	} else if other == nil {
		return 1
	}
	cmp := bytes.Compare(r.Name, other.Name)
	if cmp != 0 {
		return cmp
	}
	return models.CompareTags(r.Tags, other.Tags)
}

// newSeriesCursor returns a new instance of SeriesCursor.
func newSeriesCursor(req SeriesCursorRequest, indexSet IndexSet, cond influxql.Expr) (_ SeriesCursor, err error) {
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
		mitr, err = indexSet.MeasurementIterator()
		if err != nil {
			return nil, err
		}
	}

	return &seriesCursor{
		indexSet: indexSet,
		mitr:     mitr,
		cond:     cond,
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

		cur.row.Name, cur.row.Tags = ParseSeriesKey(cur.keys[cur.ofs])
		cur.ofs++

		//if itr.opt.Authorizer != nil && !itr.opt.Authorizer.AuthorizeSeriesRead(itr.indexSet.Database(), name, tags) {
		//	continue
		//}

		return &cur.row, nil
	}
}

func (cur *seriesCursor) readSeriesKeys(name []byte) error {
	sitr, err := cur.indexSet.MeasurementSeriesByExprIterator(name, cur.cond)
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
		} else if elem.SeriesID == 0 {
			break
		}

		key := cur.indexSet.SeriesFile.SeriesKey(elem.SeriesID)
		if len(key) == 0 {
			continue
		}
		cur.keys = append(cur.keys, key)
	}

	// Sort keys.
	sort.Sort(seriesKeys(cur.keys))
	return nil
}

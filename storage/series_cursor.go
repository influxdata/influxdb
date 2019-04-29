package storage

import (
	"bytes"
	"errors"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/lifecycle"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/tsi1"
	"github.com/influxdata/influxql"
)

var (
	errUnexpectedOrg                   = errors.New("seriesCursor: unexpected org")
	errUnexpectedTagComparisonOperator = errors.New("seriesCursor: unexpected tag comparison operator")
)

type SeriesCursor interface {
	Close() error
	Next() (*SeriesCursorRow, error)
}

type SeriesCursorRequest struct {
	// Name contains the tsdb encoded org and bucket ID
	Name [influxdb.IDLength]byte
}

// seriesCursor is an implementation of SeriesCursor over an tsi1.Index.
type seriesCursor struct {
	index    *tsi1.Index
	indexref *lifecycle.Reference
	sfile    *tsdb.SeriesFile
	sfileref *lifecycle.Reference
	name     [influxdb.IDLength]byte
	keys     [][]byte
	ofs      int
	row      SeriesCursorRow
	cond     influxql.Expr
	init     bool
}

type SeriesCursorRow struct {
	Name []byte
	Tags models.Tags
}

// newSeriesCursor returns a new instance of SeriesCursor.
func newSeriesCursor(req SeriesCursorRequest, index *tsi1.Index, sfile *tsdb.SeriesFile, cond influxql.Expr) (SeriesCursor, error) {
	if cond != nil {
		var err error
		influxql.WalkFunc(cond, func(node influxql.Node) {
			switch n := node.(type) {
			case *influxql.BinaryExpr:
				switch n.Op {
				case influxql.EQ, influxql.NEQ, influxql.EQREGEX, influxql.NEQREGEX, influxql.OR, influxql.AND:
				default:
					err = errUnexpectedTagComparisonOperator
				}
			}
		})
		if err != nil {
			return nil, err
		}
	}

	indexref, err := index.Acquire()
	if err != nil {
		return nil, err
	}
	sfileref, err := sfile.Acquire()
	if err != nil {
		indexref.Release()
		return nil, err
	}

	return &seriesCursor{
		index:    index,
		indexref: indexref,
		sfile:    sfile,
		sfileref: sfileref,
		name:     req.Name,
		cond:     cond,
	}, nil
}

// Close closes the iterator. Safe to call multiple times.
func (cur *seriesCursor) Close() error {
	cur.sfileref.Release()
	cur.indexref.Release()
	return nil
}

// Next emits the next point in the iterator.
func (cur *seriesCursor) Next() (*SeriesCursorRow, error) {
	if !cur.init {
		if err := cur.readSeriesKeys(); err != nil {
			return nil, err
		}
		cur.init = true
	}

	if cur.ofs < len(cur.keys) {
		cur.row.Name, cur.row.Tags = tsdb.ParseSeriesKeyInto(cur.keys[cur.ofs], cur.row.Tags)
		if !bytes.HasPrefix(cur.row.Name, cur.name[:influxdb.OrgIDLength]) {
			return nil, errUnexpectedOrg
		}
		cur.ofs++
		return &cur.row, nil
	}

	return nil, nil
}

func (cur *seriesCursor) readSeriesKeys() error {
	sitr, err := cur.index.MeasurementSeriesByExprIterator(cur.name[:], cur.cond)
	if err != nil {
		return err
	} else if sitr == nil {
		return nil
	}
	defer sitr.Close()

	for {
		elem, err := sitr.Next()
		if err != nil {
			return err
		} else if elem.SeriesID.IsZero() {
			break
		}

		key := cur.sfile.SeriesKey(elem.SeriesID)
		if len(key) == 0 {
			continue
		}
		cur.keys = append(cur.keys, key)
	}

	return nil
}

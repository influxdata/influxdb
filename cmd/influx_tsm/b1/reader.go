package b1 // import "github.com/influxdata/influxdb/cmd/influx_tsm/b1"

import (
	"encoding/binary"
	"sort"
	"sync/atomic"
	"time"

	"github.com/boltdb/bolt"
	"github.com/influxdata/influxdb/cmd/influx_tsm/tsdb"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

const DefaultChunkSize = 1000

var NoFieldsFiltered uint64

// Reader is used to read all data from a b1 shard.
type Reader struct {
	path string
	db   *bolt.DB
	tx   *bolt.Tx

	cursors    []*cursor
	currCursor int

	keyBuf    string
	valuesBuf []tsm1.Value

	series map[string]*tsdb.Series
	fields map[string]*tsdb.MeasurementFields
	codecs map[string]*tsdb.FieldCodec

	ChunkSize int
}

// NewReader returns a reader for the b1 shard at path.
func NewReader(path string) *Reader {
	return &Reader{
		path:   path,
		series: make(map[string]*tsdb.Series),
		fields: make(map[string]*tsdb.MeasurementFields),
		codecs: make(map[string]*tsdb.FieldCodec),
	}
}

// Open opens the reader.
func (r *Reader) Open() error {
	// Open underlying storage.
	db, err := bolt.Open(r.path, 0666, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return err
	}
	r.db = db

	// Load fields.
	if err := r.db.View(func(tx *bolt.Tx) error {
		meta := tx.Bucket([]byte("fields"))
		c := meta.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			mf := &tsdb.MeasurementFields{}
			if err := mf.UnmarshalBinary(v); err != nil {
				return err
			}
			r.fields[string(k)] = mf
			r.codecs[string(k)] = tsdb.NewFieldCodec(mf.Fields)
		}
		return nil
	}); err != nil {
		return err
	}

	// Load series
	if err := r.db.View(func(tx *bolt.Tx) error {
		meta := tx.Bucket([]byte("series"))
		c := meta.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			series := &tsdb.Series{}
			if err := series.UnmarshalBinary(v); err != nil {
				return err
			}
			r.series[string(k)] = series
		}
		return nil
	}); err != nil {
		return err
	}

	// Create cursor for each field of each series.
	r.tx, err = r.db.Begin(false)
	if err != nil {
		return err
	}
	for s, _ := range r.series {
		if err != nil {
			return err
		}

		measurement := tsdb.MeasurementFromSeriesKey(s)
		fields := r.fields[tsdb.MeasurementFromSeriesKey(s)]
		if fields == nil {
			atomic.AddUint64(&NoFieldsFiltered, 1)
			continue
		}
		for _, f := range fields.Fields {
			c := newCursor(r.tx, s, f.Name, r.codecs[measurement])
			c.SeekTo(0)
			r.cursors = append(r.cursors, c)
		}
	}
	sort.Sort(cursors(r.cursors))

	return nil
}

// Next returns whether any data remains to be read. It must be called before
// the next call to Read().
func (r *Reader) Next() bool {
	for {
		if r.currCursor == len(r.cursors) {
			// All cursors drained. No more data remains.
			return false
		}

		cc := r.cursors[r.currCursor]
		k, v := cc.Next()
		if k == -1 {
			// Go to next cursor and try again.
			r.currCursor++
			if len(r.valuesBuf) == 0 {
				// The previous cursor had no data. Instead of returning
				// just go immediately to the next cursor.
				continue
			}
			// There is some data available. Indicate that it should be read.
			return true
		}

		r.keyBuf = tsm1.SeriesFieldKey(cc.series, cc.field)
		r.valuesBuf = append(r.valuesBuf, tsdb.ConvertToValue(k, v))
		if len(r.valuesBuf) == r.ChunkSize {
			return true
		}
	}

}

// Read returns the next chunk of data in the shard, converted to tsm1 values. Data is
// emitted completely for every field, in every series, before the next field is processed.
// Data from Read() adheres to the requirements for writing to tsm1 shards
func (r *Reader) Read() (string, []tsm1.Value, error) {
	defer func() {
		r.valuesBuf = nil
	}()

	return r.keyBuf, r.valuesBuf, nil
}

// Close closes the reader.
func (r *Reader) Close() error {
	return r.tx.Rollback()
}

// cursor provides ordered iteration across a series.
type cursor struct {
	// Bolt cursor and readahead buffer.
	cursor *bolt.Cursor
	keyBuf int64
	valBuf interface{}

	series string
	field  string
	dec    *tsdb.FieldCodec
}

// Cursor returns an iterator for a key over a single field.
func newCursor(tx *bolt.Tx, series string, field string, dec *tsdb.FieldCodec) *cursor {
	cur := &cursor{
		keyBuf: -2,
		series: series,
		field:  field,
		dec:    dec,
	}

	// Retrieve series bucket.
	b := tx.Bucket([]byte(series))
	if b != nil {
		cur.cursor = b.Cursor()
	}

	return cur
}

// Seek moves the cursor to a position.
func (c cursor) SeekTo(seek int64) {
	k, v := c.cursor.Seek(u64tob(uint64(seek)))
	c.keyBuf, c.valBuf = tsdb.DecodeKeyValue(c.field, c.dec, k, v)
}

// Next returns the next key/value pair from the cursor.
func (c *cursor) Next() (key int64, value interface{}) {
	for {
		k, v := func() (int64, interface{}) {
			if c.keyBuf != -2 {
				k, v := c.keyBuf, c.valBuf
				c.keyBuf = -2
				return k, v
			}

			k, v := c.cursor.Next()
			if k == nil {
				return -1, nil
			}
			return tsdb.DecodeKeyValue(c.field, c.dec, k, v)
		}()

		if k != -1 && v == nil {
			// There is a point in the series at the next timestamp,
			// but not for this cursor's field. Go to the next point.
			continue
		}
		return k, v
	}
}

// Sort b1 cursors in correct order for writing to TSM files.

type cursors []*cursor

func (a cursors) Len() int      { return len(a) }
func (a cursors) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a cursors) Less(i, j int) bool {
	return tsm1.SeriesFieldKey(a[i].series, a[i].field) < tsm1.SeriesFieldKey(a[j].series, a[j].field)
}

// u64tob converts a uint64 into an 8-byte slice.
func u64tob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// btou64 converts an 8-byte slice to a uint64.
func btou64(b []byte) uint64 { return binary.BigEndian.Uint64(b) }

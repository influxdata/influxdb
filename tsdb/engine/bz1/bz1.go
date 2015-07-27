package bz1

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/boltdb/bolt"
	"github.com/golang/snappy"
	"github.com/influxdb/influxdb/tsdb"
)

var (
	// ErrSeriesExists is returned when writing points to an existing series.
	ErrSeriesExists = errors.New("series exists")
)

// Format is the file format name of this engine.
const Format = "bz1"

func init() {
	tsdb.RegisterEngine(Format, NewEngine)
}

const (
	// DefaultBlockSize is the default size of uncompressed points blocks.
	DefaultBlockSize = 32 * 1024 // 32KB
)

// Ensure Engine implements the interface.
var _ tsdb.Engine = &Engine{}

// Engine represents a storage engine with compressed blocks.
type Engine struct {
	mu   sync.Mutex
	path string
	db   *bolt.DB

	// Size of uncompressed points to write to a block.
	BlockSize int
}

// NewEngine returns a new instance of Engine.
func NewEngine(path string, opt tsdb.EngineOptions) tsdb.Engine {
	return &Engine{
		path: path,

		BlockSize: DefaultBlockSize,
	}
}

// Path returns the path the engine was opened with.
func (e *Engine) Path() string { return e.path }

// Open opens and initializes the engine.
func (e *Engine) Open() error {
	if err := func() error {
		e.mu.Lock()
		defer e.mu.Unlock()

		// Open underlying storage.
		db, err := bolt.Open(e.path, 0666, &bolt.Options{Timeout: 1 * time.Second})
		if err != nil {
			return err
		}
		e.db = db

		// Initialize data file.
		if err := e.db.Update(func(tx *bolt.Tx) error {
			_, _ = tx.CreateBucketIfNotExists([]byte("series"))
			_, _ = tx.CreateBucketIfNotExists([]byte("fields"))
			_, _ = tx.CreateBucketIfNotExists([]byte("points"))

			// Set file format, if not set yet.
			b, _ := tx.CreateBucketIfNotExists([]byte("meta"))
			if v := b.Get([]byte("format")); v == nil {
				if err := b.Put([]byte("format"), []byte(Format)); err != nil {
					return fmt.Errorf("set format: %s", err)
				}
			}

			return nil
		}); err != nil {
			return fmt.Errorf("init: %s", err)
		}

		return nil
	}(); err != nil {
		e.close()
		return err
	}
	return nil
}

// Close closes the engine.
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.close()
}

func (e *Engine) close() error {
	if e.db != nil {
		return e.db.Close()
	}
	return nil
}

// SetLogOutput is a no-op.
func (e *Engine) SetLogOutput(w io.Writer) {}

// LoadMetadataIndex loads the shard metadata into memory.
func (e *Engine) LoadMetadataIndex(index *tsdb.DatabaseIndex, measurementFields map[string]*tsdb.MeasurementFields) error {
	return e.db.View(func(tx *bolt.Tx) error {
		// Load measurement metadata
		meta := tx.Bucket([]byte("fields"))
		c := meta.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			m := index.CreateMeasurementIndexIfNotExists(string(k))
			mf := &tsdb.MeasurementFields{}
			if err := mf.UnmarshalBinary(v); err != nil {
				return err
			}
			for name, _ := range mf.Fields {
				m.SetFieldName(name)
			}
			mf.Codec = tsdb.NewFieldCodec(mf.Fields)
			measurementFields[m.Name] = mf
		}

		// Load series metadata
		meta = tx.Bucket([]byte("series"))
		c = meta.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			series := &tsdb.Series{}
			if err := series.UnmarshalBinary(v); err != nil {
				return err
			}
			index.CreateSeriesIndexIfNotExists(tsdb.MeasurementFromSeriesKey(string(k)), series)
		}
		return nil
	})
}

// WritePoints writes metadata and point data into the engine.
// Returns an error if new points are added to an existing key.
func (e *Engine) WritePoints(points []tsdb.Point, measurementFieldsToSave map[string]*tsdb.MeasurementFields, seriesToCreate []*tsdb.SeriesCreate) error {
	if err := e.db.Update(func(tx *bolt.Tx) error {
		// Write series & field metadata.
		if err := e.writeSeries(tx, seriesToCreate); err != nil {
			return fmt.Errorf("write series: %s", err)
		}
		if err := e.writeFields(tx, measurementFieldsToSave); err != nil {
			return fmt.Errorf("write fields: %s", err)
		}

		// Group points together by key.
		for key, a := range groupPointsByKey(points) {
			if err := e.writePoints(tx, key, a); err != nil {
				return fmt.Errorf("write points: key=%s, err=%s", key, err)
			}
		}

		return nil
	}); err != nil {
		return err
	}

	return nil
}

// writeSeries writes a list of series to the metadata.
func (e *Engine) writeSeries(tx *bolt.Tx, a []*tsdb.SeriesCreate) error {
	// Ignore if there are no series.
	if len(a) == 0 {
		return nil
	}

	// Marshal and insert each series into the metadata.
	b := tx.Bucket([]byte("series"))
	for _, sc := range a {
		// Marshal series into bytes.
		data, err := sc.Series.MarshalBinary()
		if err != nil {
			return fmt.Errorf("marshal series: %s", err)
		}

		// Insert marshaled data into appropriate key.
		if err := b.Put([]byte(sc.Series.Key), data); err != nil {
			return fmt.Errorf("put: %s", err)
		}
	}

	return nil
}

// writeFields writes a list of measurement fields to the metadata.
func (e *Engine) writeFields(tx *bolt.Tx, m map[string]*tsdb.MeasurementFields) error {
	// Ignore if there are no fields to save.
	if len(m) == 0 {
		return nil
	}

	// Persist each measurement field in the map.
	b := tx.Bucket([]byte("fields"))
	for k, f := range m {
		// Marshal field into bytes.
		data, err := f.MarshalBinary()
		if err != nil {
			return fmt.Errorf("marshal measurement field: %s", err)
		}

		// Insert marshaled data into key.
		if err := b.Put([]byte(k), data); err != nil {
			return fmt.Errorf("put: %s", err)
		}
	}

	return nil
}

// writePoints writes a set of points for a single key.
func (e *Engine) writePoints(tx *bolt.Tx, key string, a []tsdb.Point) error {
	// Ignore if there are no points.
	if len(a) == 0 {
		return nil
	}

	// Ensure points are sorted.
	sort.Sort(tsdb.Points(a))

	// Group points into blocks by size.
	blocks := make([][]byte, 1)
	for _, p := range a {
		// Add a new block if the last one is too large.
		if len(blocks[len(blocks)-1]) > e.BlockSize {
			blocks = append(blocks, nil)
		}

		// Append point to the end of the block.
		buf := marshalEntry(p.UnixNano(), p.Data())
		blocks[len(blocks)-1] = append(blocks[len(blocks)-1], buf...)
	}

	// Create bucket for key.
	bkt, err := tx.Bucket([]byte("points")).CreateBucket([]byte(key))
	if err == bolt.ErrBucketExists {
		return ErrSeriesExists
	} else if err != nil {
		return fmt.Errorf("create series bucket: %s", err)
	}

	// Write blocks to the bucket.
	for _, block := range blocks {
		// Use the first timestamp as the key for the block.
		timestamp := block[0:8]

		// Compress block using Snappy.
		value := snappy.Encode(nil, block)

		// Write block to the bucket.
		if err := bkt.Put(timestamp, value); err != nil {
			return fmt.Errorf("write block: series=%s, ts=%x, err=%s", timestamp, key, err)
		}
	}

	return nil
}

// groupPointsByKey generates a map of points by key.
func groupPointsByKey(a []tsdb.Point) map[string][]tsdb.Point {
	m := make(map[string][]tsdb.Point)

	// Group points by key.
	for _, p := range a {
		key := string(p.Key())
		m[key] = append(m[key], p)
	}

	return m
}

// DeleteSeries deletes the series from the engine.
func (e *Engine) DeleteSeries(keys []string) error {
	return e.db.Update(func(tx *bolt.Tx) error {
		for _, k := range keys {
			if err := tx.Bucket([]byte("series")).Delete([]byte(k)); err != nil {
				return fmt.Errorf("delete series metadata: %s", err)
			}
			if err := tx.Bucket([]byte("points")).DeleteBucket([]byte(k)); err != nil && err != bolt.ErrBucketNotFound {
				return fmt.Errorf("delete series data: %s", err)
			}
		}
		return nil
	})
}

// DeleteMeasurement deletes a measurement and all related series.
func (e *Engine) DeleteMeasurement(name string, seriesKeys []string) error {
	return e.db.Update(func(tx *bolt.Tx) error {
		if err := tx.Bucket([]byte("fields")).Delete([]byte(name)); err != nil {
			return err
		}

		for _, k := range seriesKeys {
			if err := tx.Bucket([]byte("series")).Delete([]byte(k)); err != nil {
				return fmt.Errorf("delete series metadata: %s", err)
			}
			if err := tx.Bucket([]byte("points")).DeleteBucket([]byte(k)); err != nil && err != bolt.ErrBucketNotFound {
				return fmt.Errorf("delete series data: %s", err)
			}
		}

		return nil
	})
}

// SeriesCount returns the number of series buckets on the shard.
func (e *Engine) SeriesCount() (n int, err error) {
	err = e.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte("points")).Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			n++
		}
		return nil
	})
	return
}

// Begin starts a new transaction on the engine.
func (e *Engine) Begin(writable bool) (tsdb.Tx, error) {
	tx, err := e.db.Begin(writable)
	if err != nil {
		return nil, err
	}
	return &Tx{Tx: tx, engine: e}, nil
}

// Stats returns internal statistics for the engine.
func (e *Engine) Stats() (stats Stats, err error) {
	err = e.db.View(func(tx *bolt.Tx) error {
		stats.Size = tx.Size()
		return nil
	})
	return stats, err
}

// Stats represents internal engine statistics.
type Stats struct {
	Size int64 // BoltDB data size
}

// Tx represents a transaction.
type Tx struct {
	*bolt.Tx
	engine *Engine
}

// Cursor returns an iterator for a key.
func (tx *Tx) Cursor(key string) tsdb.Cursor {
	// Retrieve points bucket. Ignore if there is no bucket.
	b := tx.Bucket([]byte("points")).Bucket([]byte(key))
	if b == nil {
		return nil
	}
	return &Cursor{
		cursor: b.Cursor(),
		buf:    make([]byte, DefaultBlockSize),
	}
}

// Cursor provides ordered iteration across a series.
type Cursor struct {
	cursor *bolt.Cursor
	buf    []byte // uncompressed buffer
	off    int    // buffer offset
}

// Seek moves the cursor to a position and returns the closest key/value pair.
func (c *Cursor) Seek(seek []byte) (key, value []byte) {
	// Move cursor to appropriate block and set to buffer.
	_, v := c.cursor.Seek(seek)
	c.setBuf(v)

	// Read current block up to seek position.
	c.seekBuf(seek)

	// Return current entry.
	return c.read()
}

// seekBuf moves the cursor to a position within the current buffer.
func (c *Cursor) seekBuf(seek []byte) (key, value []byte) {
	for {
		// Slice off the current entry.
		buf := c.buf[c.off:]

		// Exit if current entry's timestamp is on or after the seek.
		if len(buf) == 0 || bytes.Compare(buf[0:8], seek) != -1 {
			return
		}

		// Otherwise skip ahead to the next entry.
		c.off += entryHeaderSize + entryDataSize(buf)
	}
}

// Next returns the next key/value pair from the cursor.
func (c *Cursor) Next() (key, value []byte) {
	// Ignore if there is no buffer.
	if len(c.buf) == 0 {
		return nil, nil
	}

	// Move forward to next entry.
	c.off += entryHeaderSize + entryDataSize(c.buf[c.off:])

	// If no items left then read first item from next block.
	if c.off >= len(c.buf) {
		_, v := c.cursor.Next()
		c.setBuf(v)
	}

	return c.read()
}

// setBuf saves a compressed block to the buffer.
func (c *Cursor) setBuf(block []byte) {
	// Clear if the block is empty.
	if len(block) == 0 {
		c.buf, c.off = c.buf[0:0], 0
		return
	}

	// Otherwise decode block into buffer.
	buf, err := snappy.Decode(c.buf, block)
	if err != nil {
		log.Printf("block decode error: %s", err)
	}
	c.buf, c.off = buf, 0
}

// read reads the current key and value from the current block.
func (c *Cursor) read() (key, value []byte) {
	// Return nil if the offset is at the end of the buffer.
	if c.off >= len(c.buf) {
		return nil, nil
	}

	// Otherwise read the current entry.
	buf := c.buf[c.off:]
	dataSize := entryDataSize(buf)
	return buf[0:8], buf[entryHeaderSize : entryHeaderSize+dataSize]
}

// marshalEntry encodes point data into a single byte slice.
//
// The format of the byte slice is:
//
//     uint64 timestamp
//     uint32 data length
//     []byte data
//
func marshalEntry(timestamp int64, data []byte) []byte {
	v := make([]byte, 8+4, 8+4+len(data))
	binary.BigEndian.PutUint64(v[0:8], uint64(timestamp))
	binary.BigEndian.PutUint32(v[8:12], uint32(len(data)))
	v = append(v, data...)
	return v
}

// unmarshalEntry decodes an entry into it's separate parts.
// Returns the timestamp, data and the number of bytes read.
// Returned byte slices point to the original slice.
func unmarshalEntry(v []byte) (timestamp int64, data []byte, n int) {
	timestamp = int64(binary.BigEndian.Uint64(v[0:8]))
	dataLen := binary.BigEndian.Uint32(v[8:12])
	data = v[12+dataLen:]
	return timestamp, data, 12 + int(dataLen)
}

// entryHeaderSize is the number of bytes required for the header.
const entryHeaderSize = 8 + 4

// entryDataSize returns the size of an entry's data field, in bytes.
func entryDataSize(v []byte) int { return int(binary.BigEndian.Uint32(v[8:12])) }

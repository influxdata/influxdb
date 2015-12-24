package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/boltdb/bolt"
	"github.com/golang/snappy"
	tsm "github.com/influxdb/influxdb/tsdb/engine/tsm1"
)

type BZ1Reader struct {
	path string
	db   *bolt.DB
	tx   *bolt.Tx

	cursors    []*BZ1Cursor
	currCursor int
	keyBuf     int64
	valBuf     interface{}

	series map[string]*Series
	fields map[string]*MeasurementFields
	codecs map[string]*FieldCodec
}

func NewBZ1Reader(path string) *BZ1Reader {
	return &BZ1Reader{
		path:   path,
		series: make(map[string]*Series),
		fields: make(map[string]*MeasurementFields),
		codecs: make(map[string]*FieldCodec),
	}
}

func (b *BZ1Reader) Open() error {
	// Open underlying storage.
	db, err := bolt.Open(b.path, 0666, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return err
	}
	b.db = db

	err = b.db.View(func(tx *bolt.Tx) error {
		var data []byte

		buf := tx.Bucket([]byte("meta")).Get([]byte("series"))
		if buf == nil {
			// No data in this shard.
			return nil
		}
		data, err = snappy.Decode(nil, buf)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &b.series); err != nil {
			return err
		}

		buf = tx.Bucket([]byte("meta")).Get([]byte("fields"))
		if buf == nil {
			// No data in this shard.
			return nil
		}

		data, err = snappy.Decode(nil, buf)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &b.fields); err != nil {
			return err
		}
		return nil
	})

	// Build the codec for each measurement.
	for k, v := range b.fields {
		b.codecs[k] = NewFieldCodec(v.Fields)
	}

	b.tx, err = b.db.Begin(false)
	if err != nil {
		return err
	}
	for s, _ := range b.series {
		if err != nil {
			return err
		}

		measurement := MeasurementFromSeriesKey(s)
		for _, f := range b.fields[MeasurementFromSeriesKey(s)].Fields {
			c := NewBZ1Cursor(b.tx, s, f.Name, b.codecs[measurement])
			c.SeekTo(0)
			b.cursors = append(b.cursors, c)
		}
	}
	sort.Sort(BZ1Cursors(b.cursors))

	return nil
}

func (b *BZ1Reader) Next() bool {
	for {
		if b.currCursor == len(b.cursors) {
			// No more cursors remain. We're finished.
			return false
		}

		b.keyBuf, b.valBuf = b.cursors[b.currCursor].Next()
		if b.keyBuf == -1 {
			// Go to next cursor and try again.
			b.currCursor++
			continue
		}
		return true
	}
}

func (b *BZ1Reader) Read() (string, []tsm.Value, error) {
	var values []tsm.Value

	// Add buffered point.
	values = append(values, convertToValue(b.keyBuf, b.valBuf))

	// Add up go the chunk size of points from the current cursor.
	cc := b.cursors[b.currCursor]
	for {
		if len(values) == 1000 {
			break
		}

		k, v := cc.Next()
		if k == -1 {
			break
		}
		values = append(values, convertToValue(k, v))
	}

	return tsm.SeriesFieldKey(cc.series, cc.field), values, nil
}

func (b *BZ1Reader) Close() error {
	b.tx.Rollback()
	return nil
}

// BZ1Cursor provides ordered iteration across a series.
type BZ1Cursor struct {
	cursor       *bolt.Cursor
	buf          []byte // uncompressed buffer
	off          int    // buffer offset
	fieldIndices []int
	index        int

	series string
	field  string
	dec    *FieldCodec
}

// Cursor returns an iterator for a key.
func NewBZ1Cursor(tx *bolt.Tx, series string, field string, dec *FieldCodec) *BZ1Cursor {

	// Retrieve points bucket. Ignore if there is no bucket.
	b := tx.Bucket([]byte("points")).Bucket([]byte(series))
	if b == nil {
		return nil
	}

	return &BZ1Cursor{
		cursor: b.Cursor(),
		series: series,
		field:  field,
		dec:    dec,
	}
}

// Seek moves the cursor to a position and returns the closest key/value pair.
func (c *BZ1Cursor) SeekTo(seek int64) (key int64, value interface{}) {
	seekBytes := u64tob(uint64(seek))

	// Move cursor to appropriate block and set to buffer.
	k, v := c.cursor.Seek(seekBytes)
	if v == nil { // get the last block, it might have this time
		_, v = c.cursor.Last()
	} else if seek < int64(btou64(k)) { // the seek key is less than this block, go back one and check
		_, v = c.cursor.Prev()

		// if the previous block max time is less than the seek value, reset to where we were originally
		if v == nil || seek > int64(btou64(v[0:8])) {
			_, v = c.cursor.Seek(seekBytes)
		}
	}
	c.setBuf(v)

	// Read current block up to seek position.
	c.seekBuf(seekBytes)

	// Return current entry.
	return c.read()
}

// seekBuf moves the cursor to a position within the current buffer.
func (c *BZ1Cursor) seekBuf(seek []byte) (key, value []byte) {
	for {
		// Slice off the current entry.
		buf := c.buf[c.off:]

		// Exit if current entry's timestamp is on or after the seek.
		if len(buf) == 0 {
			return
		}

		if bytes.Compare(buf[0:8], seek) != -1 {
			return
		}

		c.off += entryHeaderSize + entryDataSize(buf)
	}
}

// Next returns the next key/value pair from the cursor.
func (c *BZ1Cursor) Next() (key int64, value interface{}) {
	// Ignore if there is no buffer.
	if len(c.buf) == 0 {
		return -1, nil
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
func (c *BZ1Cursor) setBuf(block []byte) {
	// Clear if the block is empty.
	if len(block) == 0 {
		c.buf, c.off, c.fieldIndices, c.index = c.buf[0:0], 0, c.fieldIndices[0:0], 0
		return
	}

	// Otherwise decode block into buffer.
	// Skip over the first 8 bytes since they are the max timestamp.
	buf, err := snappy.Decode(nil, block[8:])
	if err != nil {
		c.buf = c.buf[0:0]
		fmt.Printf("block decode error: %s\n", err)
	}

	c.buf, c.off = buf, 0
}

// read reads the current key and value from the current block.
func (c *BZ1Cursor) read() (key int64, value interface{}) {
	// Return nil if the offset is at the end of the buffer.
	if c.off >= len(c.buf) {
		return -1, nil
	}

	// Otherwise read the current entry.
	buf := c.buf[c.off:]
	dataSize := entryDataSize(buf)

	return decodeKeyValue(c.field, c.dec, buf[0:8], buf[entryHeaderSize:entryHeaderSize+dataSize])
}

// Sort bz1 cursors in correct order for writing to TSM files.

type BZ1Cursors []*BZ1Cursor

func (b BZ1Cursors) Len() int      { return len(b) }
func (b BZ1Cursors) Swap(i, j int) { b[i], b[j] = b[j], b[i] }
func (b BZ1Cursors) Less(i, j int) bool {
	return tsm.SeriesFieldKey(b[i].series, b[i].field) < tsm.SeriesFieldKey(b[j].series, b[j].field)
}

func convertToValue(k int64, v interface{}) tsm.Value {
	var value tsm.Value

	switch v := v.(type) {
	case int64:
		value = &Int64Value{
			time:  time.Unix(0, k),
			value: v,
		}
	case float64:
		value = &FloatValue{
			time:  time.Unix(0, k),
			value: v,
		}
	case bool:
		value = &BoolValue{
			time:  time.Unix(0, k),
			value: v,
		}
	case string:
		value = &StringValue{
			time:  time.Unix(0, k),
			value: v,
		}
	default:
		panic("value unsupported for conversion")
	}

	return value
}

// entryHeaderSize is the number of bytes required for the header.
const entryHeaderSize = 8 + 4

// entryDataSize returns the size of an entry's data field, in bytes.
func entryDataSize(v []byte) int { return int(binary.BigEndian.Uint32(v[8:12])) }

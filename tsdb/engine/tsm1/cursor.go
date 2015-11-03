package tsm1

import (
	"math"

	"github.com/influxdb/influxdb/tsdb"
)

// combinedEngineCursor holds a cursor for the WAL and the index
// and will combine the two together. Any points in the WAL with
// identical timestamps from the index will be preferred over the
// index point
type combinedEngineCursor struct {
	walCursor      tsdb.Cursor
	engineCursor   tsdb.Cursor
	walKeyBuf      int64
	walValueBuf    interface{}
	engineKeyBuf   int64
	engineValueBuf interface{}
	ascending      bool
}

// NewCombinedEngineCursor returns a Cursor that joins wc and ec.
// Values from wc take precedence over ec when identical timestamps are returned.
func NewCombinedEngineCursor(wc, ec tsdb.Cursor, ascending bool) tsdb.Cursor {
	return &combinedEngineCursor{
		walCursor:    wc,
		engineCursor: ec,
		ascending:    ascending,
	}
}

// SeekTo will seek both the index and WAL cursor
func (c *combinedEngineCursor) SeekTo(seek int64) (key int64, value interface{}) {
	c.walKeyBuf, c.walValueBuf = c.walCursor.SeekTo(seek)
	c.engineKeyBuf, c.engineValueBuf = c.engineCursor.SeekTo(seek)
	return c.read()
}

// Next returns the next value in the cursor
func (c *combinedEngineCursor) Next() (int64, interface{}) {
	return c.read()
}

// Ascending returns true if the cursor is time ascending
func (c *combinedEngineCursor) Ascending() bool {
	return c.ascending
}

// read will return the buffer value that is next from either the
// WAL or index cursor and repopulate the buffer value with the
// appropriate cursor's next value
func (c *combinedEngineCursor) read() (key int64, value interface{}) {
	if c.walKeyBuf == tsdb.EOF && c.engineKeyBuf == tsdb.EOF {
		return tsdb.EOF, nil
	}

	// handle the case where they have the same point
	if c.walKeyBuf == c.engineKeyBuf {
		// keep the wal value since it will overwrite the engine value
		key = c.walKeyBuf
		value = c.walValueBuf
		c.walKeyBuf, c.walValueBuf = c.walCursor.Next()

		// overwrite the buffered engine values
		c.engineKeyBuf, c.engineValueBuf = c.engineCursor.Next()
		return
	}

	// ascending order
	if c.ascending {
		if c.walKeyBuf != tsdb.EOF && (c.walKeyBuf < c.engineKeyBuf || c.engineKeyBuf == tsdb.EOF) {
			key = c.walKeyBuf
			value = c.walValueBuf
			c.walKeyBuf, c.walValueBuf = c.walCursor.Next()
			return
		}

		key = c.engineKeyBuf
		value = c.engineValueBuf
		c.engineKeyBuf, c.engineValueBuf = c.engineCursor.Next()
		return
	}

	// descending order
	if c.walKeyBuf != tsdb.EOF && c.walKeyBuf > c.engineKeyBuf {
		key = c.walKeyBuf
		value = c.walValueBuf
		c.walKeyBuf, c.walValueBuf = c.walCursor.Next()
		return
	}

	key = c.engineKeyBuf
	value = c.engineValueBuf
	c.engineKeyBuf, c.engineValueBuf = c.engineCursor.Next()
	return
}

// multieFieldCursor wraps cursors for multiple fields on the same series
// key. Instead of returning a plain interface value in the call for Next(),
// it returns a map[string]interface{} for the field values
type multiFieldCursor struct {
	fields      []string
	cursors     []tsdb.Cursor
	ascending   bool
	keyBuffer   []int64
	valueBuffer []interface{}
}

// NewMultiFieldCursor returns an instance of Cursor that joins the results of cursors.
func NewMultiFieldCursor(fields []string, cursors []tsdb.Cursor, ascending bool) tsdb.Cursor {
	return &multiFieldCursor{
		fields:      fields,
		cursors:     cursors,
		ascending:   ascending,
		keyBuffer:   make([]int64, len(cursors)),
		valueBuffer: make([]interface{}, len(cursors)),
	}
}

func (m *multiFieldCursor) SeekTo(seek int64) (key int64, value interface{}) {
	for i, c := range m.cursors {
		m.keyBuffer[i], m.valueBuffer[i] = c.SeekTo(seek)
	}
	return m.read()
}

func (m *multiFieldCursor) Next() (int64, interface{}) {
	return m.read()
}

func (m *multiFieldCursor) Ascending() bool {
	return m.ascending
}

func (m *multiFieldCursor) read() (int64, interface{}) {
	t := int64(math.MaxInt64)
	if !m.ascending {
		t = int64(math.MinInt64)
	}

	// find the time we need to combine all fields
	for _, k := range m.keyBuffer {
		if k == tsdb.EOF {
			continue
		}
		if m.ascending && t > k {
			t = k
		} else if !m.ascending && t < k {
			t = k
		}
	}

	// get the value and advance each of the cursors that have the matching time
	if t == math.MinInt64 || t == math.MaxInt64 {
		return tsdb.EOF, nil
	}

	mm := make(map[string]interface{})
	for i, k := range m.keyBuffer {
		if k == t {
			mm[m.fields[i]] = m.valueBuffer[i]
			m.keyBuffer[i], m.valueBuffer[i] = m.cursors[i].Next()
		}
	}
	return t, mm
}

type emptyCursor struct {
	ascending bool
}

func (c *emptyCursor) Next() (int64, interface{})            { return tsdb.EOF, nil }
func (c *emptyCursor) SeekTo(key int64) (int64, interface{}) { return tsdb.EOF, nil }
func (c *emptyCursor) Ascending() bool                       { return c.ascending }

// cursor is a cursor for the data in the index
type cursor struct {
	// key for the series key and field
	key string

	// hashID is the hashed id for the key
	hashID uint64

	// hash is a function that can take a key and hash to its ID
	hash func(string) uint64

	// f is the current data file we're reading from
	f *dataFile

	// filesPos is the position in the files index we're reading from
	filesPos int // the index in the files slice we're looking at

	// pos is the position in the current data file we're reading
	pos uint32

	// vals is the current decoded block of Values we're iterating from
	vals []Value

	ascending bool

	// blockPositions is used for descending queries to keep track
	// of what positions in the current data file encoded blocks for
	// the id exist at
	blockPositions []uint32

	// time acending slice of read only data files
	files []*dataFile
}

func newCursor(hash func(string) uint64, key string, files []*dataFile, ascending bool) *cursor {
	return &cursor{
		key:       key,
		hashID:    hash(key),
		hash:      hash,
		ascending: ascending,
		files:     files,
		vals:      make([]Value, 0),
	}
}

func (c *cursor) SeekTo(seek int64) (int64, interface{}) {
	if len(c.files) == 0 {
		return tsdb.EOF, nil
	}

	if c.ascending {
		if seek <= c.files[0].MinTime() {
			c.filesPos = 0
			c.f = c.files[0]
		} else {
			for i, f := range c.files {
				if seek >= f.MinTime() && seek <= f.MaxTime() {
					c.filesPos = i
					c.f = f
					break
				}
			}
		}
	} else {
		if seek >= c.files[len(c.files)-1].MaxTime() {
			c.filesPos = len(c.files) - 1
			c.f = c.files[c.filesPos]
		} else if seek < c.files[0].MinTime() {
			return tsdb.EOF, nil
		} else {
			for i, f := range c.files {
				if seek >= f.MinTime() && seek <= f.MaxTime() {
					c.filesPos = i
					c.f = f
					break
				}
			}
		}
	}

	if c.f == nil {
		return tsdb.EOF, nil
	}

	// find the first file we need to check in
	for {
		if c.filesPos < 0 || c.filesPos >= len(c.files) {
			return tsdb.EOF, nil
		}
		c.f = c.files[c.filesPos]

		c.pos = c.f.StartingPositionForID(c.hashID)

		// if this id isn't in this file, move to next one or return
		if c.pos == 0 {
			if c.ascending {
				c.filesPos++
			} else {
				c.filesPos--
				c.blockPositions = nil
			}
			continue
		}

		// see if this key exists in the file
		c.pos = c.seekToKey()

		// if we don't have this key, move to the next file
		if c.pos == 0 {
			if c.ascending {
				c.filesPos++
			} else {
				c.filesPos--
				c.blockPositions = nil
			}
			continue
		}

		// handle seek for correct order
		k := tsdb.EOF
		var v interface{}

		if c.ascending {
			k, v = c.seekAscending(seek)
		} else {
			k, v = c.seekDescending(seek)
		}

		if k != tsdb.EOF {
			return k, v
		}

		if c.ascending {
			c.filesPos++
		} else {
			c.filesPos--
			c.blockPositions = nil
		}
	}
}

func (c *cursor) seekAscending(seek int64) (int64, interface{}) {
	// seek to the block and values we're looking for
	indexPosition := c.f.indexPosition()
	for {
		if c.pos >= indexPosition {
			return tsdb.EOF, nil
		}

		// if the time is between this block and the next,
		// decode this block and go, otherwise seek to next block
		key, block, nextPos := c.f.block(c.pos)
		c.pos = nextPos

		// if the next key is an empty string, the next block is of the same key, check its min time
		if nextPos < indexPosition {
			nextKey, nextBlock, _ := c.f.block(nextPos)
			if nextKey == "" && MinTime(nextBlock) <= seek {
				continue
			}
		}

		// it must be in this block or not at all
		id := c.hash(key)
		if id != c.hashID {
			return tsdb.EOF, nil
		}

		c.vals = c.vals[:0]
		_ = DecodeBlock(block, &c.vals)

		// see if we can find it in this block
		for i, v := range c.vals {
			if v.Time().UnixNano() >= seek {
				c.vals = c.vals[i+1:]
				return v.Time().UnixNano(), v.Value()
			}
		}
	}
}

func (c *cursor) seekDescending(seek int64) (int64, interface{}) {
	c.setBlockPositions()
	if len(c.blockPositions) == 0 {
		return tsdb.EOF, nil
	}

	for i := len(c.blockPositions) - 1; i >= 0; i-- {
		pos := c.blockPositions[i]
		_, block, _ := c.f.block(pos)
		if MinTime(block) > seek {
			continue
		}

		c.vals = c.vals[:0]
		_ = DecodeBlock(block, &c.vals)

		c.blockPositions = c.blockPositions[:i]

		for i := len(c.vals) - 1; i >= 0; i-- {
			val := c.vals[i]
			if seek >= val.UnixNano() {
				c.vals = c.vals[:i]
				return val.UnixNano(), val.Value()
			}
			if seek < val.UnixNano() {
				// we need to move to the next block
				if i == 0 {
					break
				}
				val := c.vals[i-1]
				c.vals = c.vals[:i-1]
				return val.UnixNano(), val.Value()
			}
		}
		c.blockPositions = c.blockPositions[:i]
	}

	return tsdb.EOF, nil
}

// setBlockPositions will read the positions of all
// blocks for the cursor id in the given data file
func (c *cursor) setBlockPositions() {
	pos := c.pos

	// the first postion was already read
	c.blockPositions = append(c.blockPositions, c.pos)

	_, _, pos = c.f.block(pos)

	indexPosition := c.f.indexPosition()
	for {
		if pos >= indexPosition {
			return
		}

		key, _, next := c.f.block(pos)

		// we've already done a seek to the first block for this key. once
		// we encounter one that isn't blank we know we're done
		if key != "" {
			return
		}

		c.blockPositions = append(c.blockPositions, pos)
		pos = next
	}
}

func (c *cursor) Next() (int64, interface{}) {
	if c.ascending {
		k, v := c.nextAscending()
		return k, v
	}
	return c.nextDescending()
}

func (c *cursor) nextAscending() (int64, interface{}) {
	if len(c.vals) > 0 {
		v := c.vals[0]
		c.vals = c.vals[1:]

		return v.Time().UnixNano(), v.Value()
	}

	// if we have a file set, see if the next block is for this key
	if c.f != nil && c.pos < c.f.indexPosition() {
		key, block, next := c.f.block(c.pos)
		if key == "" {
			c.vals = c.vals[:0]
			_ = DecodeBlock(block, &c.vals)
			c.pos = next
			return c.nextAscending()
		}
	}

	// loop through the files until we hit the next one that has this id
	for {
		c.filesPos++
		if c.filesPos >= len(c.files) {
			return tsdb.EOF, nil
		}
		c.f = c.files[c.filesPos]

		startingPos := c.f.StartingPositionForID(c.hashID)
		if startingPos == 0 {
			// move to next file because it isn't in this one
			continue
		}
		c.pos = startingPos

		// we have a block with this id, see if the key is there
		pos := c.seekToKey()
		if pos == 0 {
			continue
		}

		// we have a block matching this key, decode and recurse
		_, block, next := c.f.block(pos)
		c.pos = next
		c.vals = c.vals[:0]
		_ = DecodeBlock(block, &c.vals)
		return c.nextAscending()
	}
}

func (c *cursor) seekToKey() uint32 {
	indexPosition := c.f.indexPosition()
	pos := c.pos
	for {
		if c.pos >= indexPosition {
			return uint32(0)
		}

		key, _, next := c.f.block(pos)

		if key == c.key {
			return pos
		}

		// set the position to the start of the key for the next block
		pos = next

		// read until we get to a new key so we can check it
		if key == "" {
			continue
		}

		// check the ID, if different, we don't have this key in the file
		id := c.hash(key)
		if id != c.hashID {
			return uint32(0)
		}
	}
}

func (c *cursor) nextDescending() (int64, interface{}) {
	if len(c.vals) > 0 {
		v := c.vals[len(c.vals)-1]
		if len(c.vals) >= 1 {
			c.vals = c.vals[:len(c.vals)-1]
		} else {
			c.vals = nil
		}
		return v.UnixNano(), v.Value()
	}

	for i := len(c.blockPositions) - 1; i >= 0; i-- {
		_, block, _ := c.f.block(c.blockPositions[i])
		c.vals = c.vals[:0]
		_ = DecodeBlock(block, &c.vals)
		c.blockPositions = c.blockPositions[:i]
		if len(c.vals) == 0 {
			continue
		}
		val := c.vals[len(c.vals)-1]
		c.vals = c.vals[:len(c.vals)-1]
		return val.UnixNano(), val.Value()
	}

	return tsdb.EOF, nil
}

func (c *cursor) Ascending() bool { return c.ascending }

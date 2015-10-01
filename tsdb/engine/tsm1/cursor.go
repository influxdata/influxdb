package tsm1

import (
	"math"

	"github.com/influxdb/influxdb/tsdb"
)

type combinedEngineCursor struct {
	walCursor      tsdb.Cursor
	engineCursor   tsdb.Cursor
	walKeyBuf      int64
	walValueBuf    interface{}
	engineKeyBuf   int64
	engineValueBuf interface{}
	ascending      bool
}

func NewCombinedEngineCursor(wc, ec tsdb.Cursor, ascending bool) tsdb.Cursor {
	return &combinedEngineCursor{
		walCursor:    wc,
		engineCursor: ec,
		ascending:    ascending,
	}
}

func (c *combinedEngineCursor) SeekTo(seek int64) (key int64, value interface{}) {
	c.walKeyBuf, c.walValueBuf = c.walCursor.SeekTo(seek)
	c.engineKeyBuf, c.engineValueBuf = c.engineCursor.SeekTo(seek)
	return c.read()
}

func (c *combinedEngineCursor) Next() (int64, interface{}) {
	return c.read()
}

func (c *combinedEngineCursor) Ascending() bool {
	return c.ascending
}

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

type multiFieldCursor struct {
	fields      []string
	cursors     []tsdb.Cursor
	ascending   bool
	keyBuffer   []int64
	valueBuffer []interface{}
}

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

type cursor struct {
	id       uint64
	f        *dataFile
	filesPos int // the index in the files slice we're looking at
	pos      uint32
	vals     Values

	ascending bool

	blockPositions []uint32 // only used for descending queries

	// time acending list of data files
	files []*dataFile
}

func newCursor(id uint64, files []*dataFile, ascending bool) *cursor {
	return &cursor{
		id:        id,
		ascending: ascending,
		files:     files,
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

		c.pos = c.f.StartingPositionForID(c.id)

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
	for {
		// if the time is between this block and the next,
		// decode this block and go, otherwise seek to next block
		length := c.blockLength(c.pos)

		// if the next block has a time less than what we're seeking to,
		// skip decoding this block and continue on
		nextBlockPos := c.pos + blockHeaderSize + length
		if nextBlockPos < c.f.indexPosition() {
			nextBlockID := btou64(c.f.mmap[nextBlockPos : nextBlockPos+8])
			if nextBlockID == c.id {
				nextBlockTime := c.blockMinTime(nextBlockPos)
				if nextBlockTime <= seek {
					c.pos = nextBlockPos
					continue
				}
			}
		}

		// it must be in this block or not at all
		c.decodeBlock(c.pos)

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
		if c.blockMinTime(pos) > seek {
			continue
		}

		c.decodeBlock(pos)
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

func (c *cursor) blockMinTime(pos uint32) int64 {
	return int64(btou64(c.f.mmap[pos+12 : pos+20]))
}

func (c *cursor) setBlockPositions() {
	pos := c.pos

	for {
		if pos >= c.f.indexPosition() {
			return
		}

		length := c.blockLength(pos)
		id := btou64(c.f.mmap[pos : pos+8])

		if id != c.id {
			return
		}

		c.blockPositions = append(c.blockPositions, pos)
		pos += blockHeaderSize + length
	}
}

func (c *cursor) Next() (int64, interface{}) {
	if c.ascending {
		return c.nextAscending()
	}
	return c.nextDescending()
}

func (c *cursor) nextAscending() (int64, interface{}) {
	if len(c.vals) > 0 {
		v := c.vals[0]
		c.vals = c.vals[1:]

		return v.Time().UnixNano(), v.Value()
	}

	// if we have a file set, see if the next block is for this ID
	if c.f != nil && c.pos < c.f.indexPosition() {
		nextBlockID := btou64(c.f.mmap[c.pos : c.pos+8])
		if nextBlockID == c.id {
			c.decodeBlock(c.pos)
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

		startingPos := c.f.StartingPositionForID(c.id)
		if startingPos == 0 {
			// move to next file because it isn't in this one
			continue
		}

		// we have a block with this id, decode and return
		c.decodeBlock(startingPos)
		return c.nextAscending()
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
		c.decodeBlock(c.blockPositions[i])
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

func (c *cursor) blockLength(pos uint32) uint32 {
	return btou32(c.f.mmap[pos+8 : pos+12])
}

func (c *cursor) decodeBlock(position uint32) {
	length := c.blockLength(position)
	block := c.f.mmap[position+blockHeaderSize : position+blockHeaderSize+length]
	c.vals, _ = DecodeBlock(block)

	// only adavance the position if we're asceending.
	// Descending queries use the blockPositions
	if c.ascending {
		c.pos = position + blockHeaderSize + length
	}
}

func (c *cursor) Ascending() bool { return c.ascending }

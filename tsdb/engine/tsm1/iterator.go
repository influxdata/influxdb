package tsm1

/*
import (
	"github.com/influxdb/influxdb/influxql"
)

// NOTE: This file is being developed for the query engine in parallel with tsm1
// so it should be merged into the other tsm1 files once completed.

// CreateIterator returns an iterator based on the given options.
func (e *DevEngine) CreateIterator(opt influxql.IteratorOptions) (influxql.Iterator, error) {

}

// floatIterator represents an iterator for a field that combines the cache and tsm files.
type floatIterator struct {
	name string
	tags influxql.Tags

	expr *fieldFloatIterator
	aux  []*fieldFloatIterator

	// reusable point
	point influxql.FloatPoint
}

func (itr *floatIterator) Close() error { return nil }

func (itr *floatIterator) Next() *influxql.FloatPoint {
	join := tsdb.EOF

	// Read from main expression iterator first.
	if itr.expr != nil {
		t, v := itr.expr.next()
		if t == tsdb.EOF {
			return nil
		}
		itr.point.Time = t
		itr.point.Value = v

		// Join on main expression's timestamp.
		join = t
	} else {
		for i, aux := range itr.aux {
			t, _ := itr.expr.peek()
			if i == 0 || t
		}
	}

	// Then read all auxilary cursors that join on the same timestamp.
	for i, aux := range itr.aux {
		t, v := aux.next()

	}
}

// floatFieldIterator represents a iterator for traversing a field's timestamp & value.
type floatFieldIterator struct {
	cache struct {
		values Values
		pos    int
		buf    struct {
			key   int64
			value interface{}
		}
	}

	tsm struct {
		values Values
		pos    int
		buf    struct {
			key   int64
			value interface{}
		}
	}

	tsmKeyCursor *KeyCursor
	ascending    bool
}

// SeekTo positions the cursor at the timestamp specified by seek and returns the
// timestamp and value.
func (c *devCursor) SeekTo(seek int64) (int64, interface{}) {
	// Seek to position in cache.
	c.cacheKeyBuf, c.cacheValueBuf = func() (int64, interface{}) {
		// Seek to position in cache index.
		c.cachePos = sort.Search(len(c.cache), func(i int) bool {
			return c.cache[i].Time().UnixNano() >= seek
		})

		if c.cachePos < len(c.cache) {
			v := c.cache[c.cachePos]
			if v.UnixNano() == seek || c.ascending {
				// Exact seek found or, if ascending, next one is good.
				return v.UnixNano(), v.Value()
			}
			// Nothing available if descending.
			return tsdb.EOF, nil
		}

		// Ascending cursor, no match in the cache.
		if c.ascending {
			return tsdb.EOF, nil
		}

		// Descending cursor, go to previous value in cache, and return if it exists.
		c.cachePos--
		if c.cachePos < 0 {
			return tsdb.EOF, nil
		}
		return c.cache[c.cachePos].UnixNano(), c.cache[c.cachePos].Value()
	}()

	// Seek to position to tsm block.
	if c.ascending {
		c.tsmValues, _ = c.tsmKeyCursor.SeekTo(time.Unix(0, seek-1), c.ascending)
	} else {
		c.tsmValues, _ = c.tsmKeyCursor.SeekTo(time.Unix(0, seek+1), c.ascending)
	}

	c.tsmPos = sort.Search(len(c.tsmValues), func(i int) bool {
		return c.tsmValues[i].Time().UnixNano() >= seek
	})

	if !c.ascending {
		c.tsmPos--
	}

	if c.tsmPos >= 0 && c.tsmPos < len(c.tsmValues) {
		c.tsmKeyBuf = c.tsmValues[c.tsmPos].Time().UnixNano()
		c.tsmValueBuf = c.tsmValues[c.tsmPos].Value()
	} else {
		c.tsmKeyBuf = tsdb.EOF
		c.tsmKeyCursor.Close()
	}

	return c.read()
}

// Next returns the next value from the cursor.
func (c *devCursor) Next() (int64, interface{}) {
	return c.read()
}

// Ascending returns whether the cursor returns data in time-ascending order.
func (c *devCursor) Ascending() bool { return c.ascending }

// read returns the next value for the cursor.
func (c *devCursor) read() (int64, interface{}) {
	var key int64
	var value interface{}

	// Determine where the next datum should come from -- the cache or the TSM files.

	switch {
	// No more data in cache or in TSM files.
	case c.cacheKeyBuf == tsdb.EOF && c.tsmKeyBuf == tsdb.EOF:
		key = tsdb.EOF

	// Both cache and tsm files have the same key, cache takes precedence.
	case c.cacheKeyBuf == c.tsmKeyBuf:
		key = c.cacheKeyBuf
		value = c.cacheValueBuf
		c.cacheKeyBuf, c.cacheValueBuf = c.nextCache()
		c.tsmKeyBuf, c.tsmValueBuf = c.nextTSM()

	// Buffered cache key precedes that in TSM file.
	case c.ascending && (c.cacheKeyBuf != tsdb.EOF && (c.cacheKeyBuf < c.tsmKeyBuf || c.tsmKeyBuf == tsdb.EOF)),
		!c.ascending && (c.cacheKeyBuf != tsdb.EOF && (c.cacheKeyBuf > c.tsmKeyBuf || c.tsmKeyBuf == tsdb.EOF)):
		key = c.cacheKeyBuf
		value = c.cacheValueBuf
		c.cacheKeyBuf, c.cacheValueBuf = c.nextCache()

	// Buffered TSM key precedes that in cache.
	default:
		key = c.tsmKeyBuf
		value = c.tsmValueBuf
		c.tsmKeyBuf, c.tsmValueBuf = c.nextTSM()
	}

	return key, value
}

// nextCache returns the next value from the cache.
func (c *devCursor) nextCache() (int64, interface{}) {
	if c.ascending {
		c.cachePos++
		if c.cachePos >= len(c.cache) {
			return tsdb.EOF, nil
		}
		return c.cache[c.cachePos].UnixNano(), c.cache[c.cachePos].Value()
	} else {
		c.cachePos--
		if c.cachePos < 0 {
			return tsdb.EOF, nil
		}
		return c.cache[c.cachePos].UnixNano(), c.cache[c.cachePos].Value()
	}
}

// nextTSM returns the next value from the TSM files.
func (c *devCursor) nextTSM() (int64, interface{}) {
	if c.ascending {
		c.tsmPos++
		if c.tsmPos >= len(c.tsmValues) {
			c.tsmValues, _ = c.tsmKeyCursor.Next(c.ascending)
			if len(c.tsmValues) == 0 {
				c.tsmKeyCursor.Close()
				return tsdb.EOF, nil
			}
			c.tsmPos = 0
		}
		return c.tsmValues[c.tsmPos].UnixNano(), c.tsmValues[c.tsmPos].Value()
	} else {
		c.tsmPos--
		if c.tsmPos < 0 {
			c.tsmValues, _ = c.tsmKeyCursor.Next(c.ascending)
			if len(c.tsmValues) == 0 {
				c.tsmKeyCursor.Close()
				return tsdb.EOF, nil
			}
			c.tsmPos = len(c.tsmValues) - 1
		}
		return c.tsmValues[c.tsmPos].UnixNano(), c.tsmValues[c.tsmPos].Value()
	}
}
*/

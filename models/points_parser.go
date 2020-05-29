package models

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"time"
	"unsafe"
)

// Limits errors
var (
	// ErrLimitMaxLinesExceeded is the error returned by ParsePointsWithOptions when
	// the number of lines in the source buffer exceeds the specified limit.
	ErrLimitMaxLinesExceeded = errors.New("points: number of lines exceeded")

	// ErrLimitMaxValuesExceeded is the error returned by ParsePointsWithOptions when
	// the number of parsed values exceeds the specified limit.
	ErrLimitMaxValuesExceeded = errors.New("points: number of values exceeded")

	// ErrLimitMaxBytesExceeded is the error returned by ParsePointsWithOptions when
	// the number of allocated bytes to parse the source buffer exceeds the specified limit.
	ErrLimitMaxBytesExceeded = errors.New("points: number of allocated bytes exceeded")

	errLimit = errors.New("points: limit exceeded")
)

type ParserStats struct {
	// BytesN reports the number of bytes allocated to parse the request.
	BytesN int
}

type ParserOption func(*pointsParser)

// WithParserPrecision specifies the default precision for to use to truncate timestamps.
func WithParserPrecision(precision string) ParserOption {
	return func(pp *pointsParser) {
		pp.precision = precision
	}
}

// WithParserDefaultTime specifies the default time to assign to values when no timestamp is provided.
func WithParserDefaultTime(t time.Time) ParserOption {
	return func(pp *pointsParser) {
		pp.defaultTime = t
	}
}

// WithParserMaxBytes specifies the maximum number of bytes that may be allocated when processing a single request.
func WithParserMaxBytes(n int) ParserOption {
	return func(pp *pointsParser) {
		pp.maxBytes = n
	}
}

// WithParserMaxLines specifies the maximum number of lines that may be parsed when processing a single request.
func WithParserMaxLines(n int) ParserOption {
	return func(pp *pointsParser) {
		pp.maxLines = n
	}
}

// WithParserMaxValues specifies the maximum number of values that may be parsed when processing a single request.
func WithParserMaxValues(n int) ParserOption {
	return func(pp *pointsParser) {
		pp.maxValues = n
	}
}

// WithParserStats specifies that s will contain statistics about the parsed request.
func WithParserStats(s *ParserStats) ParserOption {
	return func(pp *pointsParser) {
		pp.stats = s
	}
}

type parserState int

const (
	parserStateOK parserState = iota
	parserStateBytesLimit
	parserStateValueLimit
)

type pointsParser struct {
	maxLines    int
	maxBytes    int
	maxValues   int
	bytesN      int
	orgBucket   []byte
	defaultTime time.Time // truncated time to assign to points which have no associated timestamp.
	precision   string
	points      []Point
	state       parserState
	stats       *ParserStats
}

func newPointsParser(orgBucket []byte, opts ...ParserOption) *pointsParser {
	pp := &pointsParser{
		orgBucket:   orgBucket,
		defaultTime: time.Now(),
		precision:   "ns",
		state:       parserStateOK,
	}

	for _, opt := range opts {
		opt(pp)
	}

	// truncate the time based in the specified precision
	pp.defaultTime = truncateTimeWithPrecision(pp.defaultTime, pp.precision)

	return pp
}

func (pp *pointsParser) parsePoints(buf []byte) (err error) {
	lineCount := bytes.Count(buf, []byte{'\n'})
	if pp.maxLines > 0 && lineCount > pp.maxLines {
		return ErrLimitMaxLinesExceeded
	}

	if !pp.checkAlloc(lineCount+1, int(unsafe.Sizeof(Point(nil)))) {
		return ErrLimitMaxBytesExceeded
	}

	pp.points = make([]Point, 0, lineCount+1)

	var (
		pos    int
		block  []byte
		failed []string
	)
	for pos < len(buf) && pp.state == parserStateOK {
		pos, block = scanLine(buf, pos)
		pos++

		if len(block) == 0 {
			continue
		}

		// lines which start with '#' are comments
		start := skipWhitespace(block, 0)

		// If line is all whitespace, just skip it
		if start >= len(block) {
			continue
		}

		if block[start] == '#' {
			continue
		}

		// strip the newline if one is present
		if lb := block[len(block)-1]; lb == '\n' || lb == '\r' {
			block = block[:len(block)-1]
		}

		err = pp.parsePointsAppend(block[start:])
		if err != nil {
			if errors.Is(err, errLimit) {
				break
			}

			if !pp.checkAlloc(1, len(block[start:])) {
				pp.state = parserStateBytesLimit
				break
			}

			failed = append(failed, fmt.Sprintf("unable to parse '%s': %v", string(block[start:]), err))
		}
	}

	if pp.stats != nil {
		pp.stats.BytesN = pp.bytesN
	}

	if pp.state != parserStateOK {
		switch pp.state {
		case parserStateBytesLimit:
			return ErrLimitMaxBytesExceeded
		case parserStateValueLimit:
			return ErrLimitMaxValuesExceeded
		default:
			panic("unreachable")
		}
	}

	if len(failed) > 0 {
		return fmt.Errorf("%s", strings.Join(failed, "\n"))
	}

	return nil
}

func (pp *pointsParser) parsePointsAppend(buf []byte) error {
	// scan the first block which is measurement[,tag1=value1,tag2=value=2...]
	pos, key, err := scanKey(buf, 0)
	if err != nil {
		return err
	}

	// measurement name is required
	if len(key) == 0 {
		return fmt.Errorf("missing measurement")
	}

	if len(key) > MaxKeyLength {
		return fmt.Errorf("max key length exceeded: %v > %v", len(key), MaxKeyLength)
	}

	// Since the measurement is converted to a tag and measurements & tags have
	// different escaping rules, we need to check if the measurement needs escaping.
	_, i, _ := scanMeasurement(key, 0)
	keyMeasurement := key[:i-1]
	if bytes.IndexByte(keyMeasurement, '=') != -1 {
		escapedKeyMeasurement := bytes.Replace(keyMeasurement, []byte("="), []byte(`\=`), -1)

		sz := len(escapedKeyMeasurement) + (len(key) - len(keyMeasurement))
		if !pp.checkAlloc(1, sz) {
			return errLimit
		}
		newKey := make([]byte, sz)
		copy(newKey, escapedKeyMeasurement)
		copy(newKey[len(escapedKeyMeasurement):], key[len(keyMeasurement):])
		key = newKey
	}

	// scan the second block is which is field1=value1[,field2=value2,...]
	// at least one field is required
	pos, fields, err := scanFields(buf, pos)
	if err != nil {
		return err
	} else if len(fields) == 0 {
		return fmt.Errorf("missing fields")
	}

	// scan the last block which is an optional integer timestamp
	pos, ts, err := scanTime(buf, pos)
	if err != nil {
		return err
	}

	// Build point with timestamp only.
	pt := point{}

	if len(ts) == 0 {
		pt.time = pp.defaultTime
	} else {
		ts, err := parseIntBytes(ts, 10, 64)
		if err != nil {
			return err
		}
		pt.time, err = SafeCalcTime(ts, pp.precision)
		if err != nil {
			return err
		}

		// Determine if there are illegal non-whitespace characters after the
		// timestamp block.
		for pos < len(buf) {
			if buf[pos] != ' ' {
				return ErrInvalidPoint
			}
			pos++
		}
	}

	// Loop over fields and split points while validating field.
	var walkFieldsErr error
	if err := walkFields(fields, func(k, v, fieldBuf []byte) bool {
		var newKey []byte
		newKey, walkFieldsErr = pp.newV2Key(key, k)
		if walkFieldsErr != nil {
			return false
		}

		walkFieldsErr = pp.append(point{time: pt.time, key: newKey, fields: fieldBuf})
		return walkFieldsErr == nil
	}); err != nil {
		return err
	} else if walkFieldsErr != nil {
		return walkFieldsErr
	}

	return nil
}

func (pp *pointsParser) append(p point) error {
	if pp.maxValues > 0 && len(pp.points) > pp.maxValues {
		pp.state = parserStateValueLimit
		return errLimit
	}
	if !pp.checkAlloc(1, int(unsafe.Sizeof(p))) {
		return errLimit
	}
	pp.points = append(pp.points, &p)
	return nil
}

func (pp *pointsParser) checkAlloc(n, size int) bool {
	newBytes := pp.bytesN + (n * size)
	if pp.maxBytes > 0 && newBytes > pp.maxBytes {
		pp.state = parserStateBytesLimit
		return false
	}
	pp.bytesN = newBytes
	return true
}

// newV2Key returns a new key by converting the old measurement & field into keys.
func (pp *pointsParser) newV2Key(oldKey, field []byte) ([]byte, error) {
	mm := pp.orgBucket
	if sz := seriesKeySizeV2(oldKey, mm, field); sz > MaxKeyLength {
		return nil, fmt.Errorf("max key length exceeded: %v > %v", sz, MaxKeyLength)
	}

	sz := len(mm) + 1 + len(MeasurementTagKey) + 1 + len(oldKey) + 1 + len(FieldKeyTagKey) + 1 + len(field)
	if !pp.checkAlloc(1, sz) {
		return nil, errLimit
	}
	newKey := make([]byte, sz)
	buf := newKey

	copy(buf, mm)
	buf = buf[len(mm):]

	buf[0], buf[1], buf[2], buf = ',', MeasurementTagKeyBytes[0], '=', buf[3:]
	copy(buf, oldKey)
	buf = buf[len(oldKey):]

	buf[0], buf[1], buf[2], buf = ',', FieldKeyTagKeyBytes[0], '=', buf[3:]
	copy(buf, field)

	return newKey, nil
}

func truncateTimeWithPrecision(t time.Time, precision string) time.Time {
	switch precision {
	case "us":
		return t.Truncate(time.Microsecond)
	case "ms":
		return t.Truncate(time.Millisecond)
	case "s":
		return t.Truncate(time.Second)
	default:
		return t
	}
}

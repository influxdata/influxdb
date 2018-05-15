package csv

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/ifql/query/execute"
	"github.com/influxdata/ifql/values"
	"github.com/influxdata/platform"
)

const (
	defaultMaxBufferCount = 1000
)

// ResultDecoder decodes a csv representation of a result.
type ResultDecoder struct {
	c ResultDecoderConfig
}

// NewResultDecoder creates a new ResultDecoder.
func NewResultDecoder(c ResultDecoderConfig) *ResultDecoder {
	if c.MaxBufferCount == 0 {
		c.MaxBufferCount = defaultMaxBufferCount
	}
	return &ResultDecoder{
		c: c,
	}
}

// ResultDecoderConfig are options that can be specified on the ResultDecoder.
type ResultDecoderConfig struct {
	// MaxBufferCount is the maximum number of rows that will be buffered when decoding.
	// If 0, then a value of 1000 will be used.
	MaxBufferCount int
}

func (d *ResultDecoder) Decode(r io.Reader) (execute.Result, error) {
	res := &resultDecoder{
		r: csv.NewReader(r),
		c: d.c,
	}
	return res, res.readHeader()
}

// MultiResultDecoder reads multiple results from a single csv file.
// Results are delimited by an empty line.
type MultiResultDecoder struct {
	c ResultDecoderConfig
	r io.Reader
}

// NewMultiResultDecoder creates a new MultiResultDecoder.
func NewMultiResultDecoder(c ResultDecoderConfig) *MultiResultDecoder {
	return &MultiResultDecoder{
		c: c,
	}
}

func (d *MultiResultDecoder) Decode(r io.Reader) (platform.ResultIterator, error) {
	return &resultIterator{
		c: d.c,
		r: r,
	}, nil
}

// resultIterator iterates through the results encoded in r.
type resultIterator struct {
	c    ResultDecoderConfig
	r    io.Reader
	next *resultDecoder
	err  error
}

func (r *resultIterator) More() bool {
	if r.next == nil || !r.next.eof {
		r.next = &resultDecoder{
			r: csv.NewReader(r.r),
			c: r.c,
		}
		r.err = r.next.readHeader()
		return r.err == nil
	}
	return false
}

func (r *resultIterator) Next() (string, execute.Result) {
	return "", r.next
}

func (r *resultIterator) Cancel() {
}

func (r *resultIterator) Err() error {
	return r.err
}

type resultDecoder struct {
	r *csv.Reader
	c ResultDecoderConfig

	cols []colMeta

	eof bool
}

func (r *resultDecoder) readHeader() error {
	// TODO: Check for #error header and report
	dataTypes, err := r.r.Read()
	if err != nil {
		if err == io.EOF {
			return errors.New("short read: expected #datatype header row")
		}
		return err
	}
	labels, err := r.r.Read()
	if err != nil {
		if err == io.EOF {
			return errors.New("short read: expected header row")
		}
		return err
	}
	cols, err := colsFromHeader(dataTypes, labels)
	if err != nil {
		return err
	}
	r.cols = cols
	return nil
}

func (r *resultDecoder) Blocks() execute.BlockIterator {
	return r
}

func (r *resultDecoder) Abort(error) {
	panic("not implemented")
}

func (r *resultDecoder) Do(f func(execute.Block) error) error {
	var extraLine []string
	for {
		b, err := newBlock(r.r, extraLine, r.cols, r.c)
		if err != nil {
			return err
		}
		if err := f(b); err != nil {
			return err
		}
		if b.err != nil {
			return b.err
		}
		// track whether we hit the EOF
		r.eof = b.eof
		// track any extra line that was read
		extraLine = b.extraLine

		// break if we have read the entire result
		if b.resultFinished {
			break
		}
	}
	return nil
}

type blockDecoder struct {
	r  *csv.Reader
	c  ResultDecoderConfig
	id string

	boundsSet bool

	cols    []colMeta
	builder *execute.ColListBlockBuilder

	finished       bool
	resultFinished bool
	eof            bool
	err            error
	extraLine      []string
}

const (
	//TODO(nathanielc): Determine these column indexes from the header
	blockDatatypeCol = 0
	blockIDCol       = 1
	blockStartCol    = 2
	blockStopCol     = 3

	blockRecordStart = 4

	datatypeLabel   = "#datatype"
	blockIDLabel    = "blkid"
	blockStartLabel = "_start"
	blockStopLabel  = "_stop"
)

func newBlock(r *csv.Reader, line []string, cols []colMeta, c ResultDecoderConfig) (*blockDecoder, error) {
	b := &blockDecoder{
		r:       r,
		c:       c,
		cols:    cols,
		builder: execute.NewColListBlockBuilder(newUnlimitedAllocator()),
	}
	for _, c := range cols[blockRecordStart:] {
		b.builder.AddCol(c.ColMeta)
	}
	if len(line) > 0 {
		b.appendLine(line)
	}
	b.advance()
	return b, b.err
}

func (b *blockDecoder) do(f func()) {
	// do f for initial advance call
	if b.builder.NRows() > 0 {
		f()
		b.builder.ClearData()
	}
	// while not finished advance and do f
	for !b.finished {
		b.advance()
		if b.err != nil {
			return
		}
		if b.builder.NRows() > 0 {
			f()
			b.builder.ClearData()
		}
	}
}

// advance reads the csv data until the end of the block or bufSize rows have been read.
func (b *blockDecoder) advance() {
	for b.builder.NRows() < b.c.MaxBufferCount {
		line, err := b.r.Read()
		if err != nil {
			b.finished = true
			if err == io.EOF {
				b.eof = true
				b.resultFinished = true
				return
			}
			b.err = err
			return
		}
		if len(line) == 0 {
			// result is done
			b.resultFinished = true
			b.finished = true
			return
		}
		if b.id == "" {
			b.id = line[blockIDCol]
		}

		if b.id != line[blockIDCol] {
			b.extraLine = line
			b.finished = true
			return
		}

		if err := b.appendLine(line); err != nil {
			b.err = err
			b.finished = true
			return
		}
	}
}

func (b *blockDecoder) processLine() {
}

func (b *blockDecoder) RefCount(n int) {}
func (b *blockDecoder) Bounds() execute.Bounds {
	return b.builder.Bounds()
}

func (b *blockDecoder) Tags() execute.Tags {
	return b.builder.Tags()
}

func (b *blockDecoder) Cols() []execute.ColMeta {
	return b.builder.Cols()
}

func (b *blockDecoder) Col(c int) execute.ValueIterator {
	return &valueIterator{
		col: c,
		b:   b,
	}
}

func (b *blockDecoder) Times() execute.ValueIterator {
	timeIdx := execute.TimeIdx(b.Cols())
	return b.Col(timeIdx)
}

func (b *blockDecoder) Values() (execute.ValueIterator, error) {
	valueIdx := execute.ValueIdx(b.Cols())
	if valueIdx == -1 {
		return nil, errors.New("no _value column")
	}
	return b.Col(valueIdx), nil
}

func (b *blockDecoder) setBounds(line []string) error {
	start, err := decodeTime(line[blockStartCol], b.cols[blockStartCol].fmt)
	if err != nil {
		return err
	}
	stop, err := decodeTime(line[blockStopCol], b.cols[blockStopCol].fmt)
	if err != nil {
		return err
	}
	b.builder.SetBounds(execute.Bounds{
		Start: start,
		Stop:  stop,
	})
	b.boundsSet = true
	return nil
}

func (b *blockDecoder) appendLine(line []string) error {
	if !b.boundsSet {
		err := b.setBounds(line)
		if err != nil {
			return err
		}
	}
	record := line[blockRecordStart:]
	colMeta := b.cols[blockRecordStart:]

	if len(b.builder.Cols()) != len(record) {
		return fmt.Errorf("record has incorrect number of columns %d != %d", len(b.builder.Cols()), len(record))
	}
	for j, c := range colMeta {
		if err := decodeValueInto(j, c, record[j], b.builder); err != nil {
			return err
		}
	}
	return nil
}

type valueIterator struct {
	col int
	b   *blockDecoder
}

func (v *valueIterator) DoBool(f func([]bool, execute.RowReader)) {
	v.b.do(func() { v.b.builder.RawBlock().Col(v.col).DoBool(f) })
}

func (v *valueIterator) DoInt(f func([]int64, execute.RowReader)) {
	v.b.do(func() { v.b.builder.RawBlock().Col(v.col).DoInt(f) })
}

func (v *valueIterator) DoUInt(f func([]uint64, execute.RowReader)) {
	v.b.do(func() { v.b.builder.RawBlock().Col(v.col).DoUInt(f) })
}

func (v *valueIterator) DoFloat(f func([]float64, execute.RowReader)) {
	v.b.do(func() { v.b.builder.RawBlock().Col(v.col).DoFloat(f) })
}

func (v *valueIterator) DoString(f func([]string, execute.RowReader)) {
	v.b.do(func() { v.b.builder.RawBlock().Col(v.col).DoString(f) })
}

func (v *valueIterator) DoTime(f func([]execute.Time, execute.RowReader)) {
	v.b.do(func() { v.b.builder.RawBlock().Col(v.col).DoTime(f) })
}

type colMeta struct {
	execute.ColMeta
	fmt string
}

func colsFromHeader(dataTypes, labels []string) ([]colMeta, error) {
	if len(dataTypes) != len(labels) {
		return nil, errors.New("mismatched datatypes and labels count")
	}
	cols := make([]colMeta, len(dataTypes))
	for i := range dataTypes {
		cols[i].Label = labels[i]
		split := strings.SplitN(dataTypes[i], ":", 2)
		var typ, colFmt string
		typ = split[0]
		if len(split) > 1 {
			colFmt = split[1]
		}
		switch typ {
		case "tag":
			cols[i].Kind = execute.TagColKind
			cols[i].Type = execute.TString
		case "dateTime", "time":
			cols[i].Kind = execute.TimeColKind
			cols[i].Type = execute.TTime
			switch colFmt {
			case "RFC3339":
				cols[i].fmt = time.RFC3339
			case "RFC3339Nano":
				cols[i].fmt = time.RFC3339Nano
			default:
				cols[i].fmt = colFmt
			}
		case "double":
			cols[i].Kind = execute.ValueColKind
			cols[i].Type = execute.TFloat
		case "string":
			cols[i].Kind = execute.ValueColKind
			cols[i].Type = execute.TString
		case "boolean":
			cols[i].Kind = execute.ValueColKind
			cols[i].Type = execute.TBool
		case "long":
			cols[i].Kind = execute.ValueColKind
			cols[i].Type = execute.TInt
		case "unsignedLong":
			cols[i].Kind = execute.ValueColKind
			cols[i].Type = execute.TUInt
		case "#datatype":
			// Do nothing
		default:
			return nil, fmt.Errorf("unsupported column type %q", typ)
		}
	}
	return cols, nil
}

func newUnlimitedAllocator() *execute.Allocator {
	return &execute.Allocator{Limit: math.MaxInt64}
}

type ResultEncoder struct {
}

func NewResultEncoder() *ResultEncoder {
	return new(ResultEncoder)
}

func (e *ResultEncoder) Encode(w io.Writer, result execute.Result) error {
	blkid := 0
	var row []string
	cols := []colMeta{
		{ColMeta: execute.ColMeta{Label: "", Type: execute.TInvalid}},
		{ColMeta: execute.ColMeta{Label: blockIDLabel, Type: execute.TInt}},
		{ColMeta: execute.ColMeta{Label: blockStartLabel, Type: execute.TTime}, fmt: time.RFC3339Nano},
		{ColMeta: execute.ColMeta{Label: blockStopLabel, Type: execute.TTime}, fmt: time.RFC3339Nano},
	}
	writer := csv.NewWriter(w)
	writer.Comma = ','
	writer.UseCRLF = true

	return result.Blocks().Do(func(b execute.Block) error {
		if blkid == 0 {
			// Update cols with block cols
			for _, c := range b.Cols() {
				cm := colMeta{ColMeta: c}
				if c.Type == execute.TTime {
					cm.fmt = time.RFC3339Nano
				}
				cols = append(cols, cm)
			}
			// pre-allocate row slice
			row = make([]string, len(cols))

			// Write dataType header
			for j, c := range cols {
				switch c.Type {
				case execute.TInvalid:
					row[j] = datatypeLabel
				case execute.TBool:
					row[j] = "boolean"
				case execute.TInt:
					row[j] = "long"
				case execute.TUInt:
					row[j] = "unsignedLong"
				case execute.TFloat:
					row[j] = "double"
				case execute.TString:
					row[j] = "string"
				case execute.TTime:
					row[j] = "dateTime:RFC3339Nano"
				}
			}
			writer.Write(row)
			// Write labels header
			for j, c := range cols {
				row[j] = c.Label
			}
			writer.Write(row)
		}

		// Update row with block metadata
		row[blockIDCol] = strconv.Itoa(blkid)
		row[blockStartCol] = encodeTime(b.Bounds().Start, cols[blockStartCol].fmt)
		row[blockStopCol] = encodeTime(b.Bounds().Stop, cols[blockStopCol].fmt)

		b.Times().DoTime(func(ts []execute.Time, rr execute.RowReader) {
			record := row[blockRecordStart:]
			for i := range ts {
				for j, c := range cols[blockRecordStart:] {
					record[j] = encodeValue(i, j, c, rr)
				}
				writer.Write(row)
			}
			writer.Flush()
		})
		blkid++
		return writer.Error()
	})
}

func decodeValueInto(j int, c colMeta, value string, builder execute.BlockBuilder) error {
	switch c.Type {
	case execute.TBool:
		v, err := strconv.ParseBool(value)
		if err != nil {
			return err
		}
		builder.AppendBool(j, v)
	case execute.TInt:
		v, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return err
		}
		builder.AppendInt(j, v)
	case execute.TUInt:
		v, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return err
		}
		builder.AppendUInt(j, v)
	case execute.TFloat:
		v, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return err
		}
		builder.AppendFloat(j, v)
	case execute.TString:
		builder.AppendString(j, value)
	case execute.TTime:
		t, err := decodeTime(value, c.fmt)
		if err != nil {
			return err
		}
		builder.AppendTime(j, t)
	default:
		return fmt.Errorf("unsupported type %v", c.Type)
	}
	return nil
}

func encodeValue(i, j int, c colMeta, rr execute.RowReader) string {
	switch c.Type {
	case execute.TBool:
		return strconv.FormatBool(rr.AtBool(i, j))
	case execute.TInt:
		return strconv.FormatInt(rr.AtInt(i, j), 10)
	case execute.TUInt:
		return strconv.FormatUint(rr.AtUInt(i, j), 10)
	case execute.TFloat:
		return strconv.FormatFloat(rr.AtFloat(i, j), 'f', -1, 64)
	case execute.TString:
		return rr.AtString(i, j)
	case execute.TTime:
		return encodeTime(rr.AtTime(i, j), c.fmt)
	default:
		return ""
	}
}

func decodeTime(t string, fmt string) (execute.Time, error) {
	v, err := time.Parse(fmt, t)
	if err != nil {
		return 0, err
	}
	return values.ConvertTime(v), nil
}

func encodeTime(t execute.Time, fmt string) string {
	return t.Time().Format(fmt)
}

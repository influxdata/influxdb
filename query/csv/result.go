package csv

import (
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/ifql/query/execute"
	"github.com/influxdata/ifql/values"
	"github.com/influxdata/platform"
	"github.com/pkg/errors"
)

const (
	defaultMaxBufferCount = 1000

	annotationIdx = 0
	resultIdx     = 1
	tableIdx      = 2

	recordStartIdx = 3

	datatypeAnnotation  = "datatype"
	partitionAnnotation = "partition"
	defaultAnnotation   = "default"

	resultLabel = "result"
	tableLabel  = "table"

	commentPrefix = "#"

	stringDatatype = "string"
	timeDatatype   = "dateTime"
	floatDatatype  = "double"
	boolDatatype   = "boolean"
	intDatatype    = "long"
	uintDatatype   = "unsignedLong"

	timeDataTypeWithFmt = "dateTime:RFC3339"
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
	// NoHeader indicates that the CSV data will not have a header row.
	NoHeader bool
	// MaxBufferCount is the maximum number of rows that will be buffered when decoding.
	// If 0, then a value of 1000 will be used.
	MaxBufferCount int
}

func (d *ResultDecoder) Decode(r io.Reader) (execute.Result, error) {
	return newResultDecoder(r, d.c, nil), nil
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
		var extraMeta *tableMetadata
		if r.next != nil {
			extraMeta = r.next.extraMeta
		}
		r.next = newResultDecoder(r.r, r.c, extraMeta)
		return true
	}
	return false
}

func (r *resultIterator) Next() (string, execute.Result) {
	return r.next.id, r.next
}

func (r *resultIterator) Cancel() {
}

func (r *resultIterator) Err() error {
	return r.err
}

type resultDecoder struct {
	id string
	r  io.Reader
	c  ResultDecoderConfig

	extraMeta *tableMetadata

	eof bool
}

func newResultDecoder(r io.Reader, c ResultDecoderConfig, extraMeta *tableMetadata) *resultDecoder {
	return &resultDecoder{
		r:         r,
		c:         c,
		extraMeta: extraMeta,
	}
}

func newCSVReader(r io.Reader) *csv.Reader {
	csvr := csv.NewReader(r)
	csvr.ReuseRecord = true
	// Do not check record size
	csvr.FieldsPerRecord = -1
	return csvr
}

func (r *resultDecoder) Blocks() execute.BlockIterator {
	return r
}

func (r *resultDecoder) Abort(error) {
	panic("not implemented")
}

func (r *resultDecoder) Do(f func(execute.Block) error) error {
	cr := newCSVReader(r.r)

	var extraLine []string
	var meta tableMetadata

	newMeta := true
	for !r.eof {
		if newMeta {
			if r.extraMeta != nil {
				meta = *r.extraMeta
				r.extraMeta = nil
			} else {
				tm, err := readMetadata(cr, r.c, extraLine)
				if err != nil {
					if err == io.EOF {
						r.eof = true
						return nil
					}
					return errors.Wrap(err, "failed to read meta data")
				}
				meta = tm
				extraLine = nil
			}
			if r.id == "" {
				r.id = meta.ResultID
			}

			if meta.ResultID != r.id {
				r.extraMeta = &meta
				return nil
			}
		}

		// create new block
		b, err := newBlock(cr, r.c, meta, extraLine)
		if err != nil {
			return err
		}
		if err := f(b); err != nil {
			return err
		}
		// track whether we hit the EOF
		r.eof = b.eof
		// track any extra line that was read
		extraLine = b.extraLine
		if len(extraLine) > 0 {
			newMeta = extraLine[annotationIdx] != ""
		}
	}
	return nil
}

type tableMetadata struct {
	ResultID   string
	TableID    string
	Cols       []colMeta
	Partitions []bool
	Defaults   []interface{}
	NumFields  int
}

// readMetadata reads the table annotations and header.
func readMetadata(r *csv.Reader, c ResultDecoderConfig, extraLine []string) (tableMetadata, error) {
	n := -1
	var resultID, tableID string
	var datatypes, partitions, defaults []string
	for datatypes == nil || partitions == nil || defaults == nil {
		var line []string
		if len(extraLine) > 0 {
			line = extraLine
			extraLine = nil
		} else {
			l, err := r.Read()
			if err != nil {
				if err == io.EOF {
					switch {
					case datatypes == nil:
						return tableMetadata{}, fmt.Errorf("missing expected annotation datatype")
					case partitions == nil:
						return tableMetadata{}, fmt.Errorf("missing expected annotation partition")
					case defaults == nil:
						return tableMetadata{}, fmt.Errorf("missing expected annotation default")
					default:
						// No, just pass the EOF up
						return tableMetadata{}, err
					}
				}
				return tableMetadata{}, err
			}
			line = l
		}
		if n == -1 {
			n = len(line)
		}
		if n != len(line) {
			return tableMetadata{}, errors.Wrap(csv.ErrFieldCount, "failed to read annotations")
		}
		switch annotation := strings.TrimPrefix(line[annotationIdx], commentPrefix); annotation {
		case datatypeAnnotation:
			datatypes = copyLine(line[recordStartIdx:])
		case partitionAnnotation:
			partitions = copyLine(line[recordStartIdx:])
		case defaultAnnotation:
			resultID = line[resultIdx]
			tableID = line[tableIdx]
			defaults = copyLine(line[recordStartIdx:])
		default:
			if annotation == "" {
				switch {
				case datatypes == nil:
					return tableMetadata{}, fmt.Errorf("missing expected annotation datatype")
				case partitions == nil:
					return tableMetadata{}, fmt.Errorf("missing expected annotation partition")
				case defaults == nil:
					return tableMetadata{}, fmt.Errorf("missing expected annotation default")
				}
			}
			// Skip extra annotation
		}
	}

	// Determine column labels
	var labels []string
	if c.NoHeader {
		labels := make([]string, len(datatypes))
		for i := range labels {
			labels[i] = fmt.Sprintf("col%d", i)
		}
	} else {
		// Read header row
		line, err := r.Read()
		if err != nil {
			if err == io.EOF {
				return tableMetadata{}, errors.New("missing expected header row")
			}
			return tableMetadata{}, err
		}
		if n != len(line) {
			return tableMetadata{}, errors.Wrap(csv.ErrFieldCount, "failed to read header row")
		}
		labels = line[recordStartIdx:]
	}

	cols := make([]colMeta, len(labels))
	defaultValues := make([]interface{}, len(labels))
	partitionValues := make([]bool, len(labels))

	for j, label := range labels {
		t, desc, err := decodeType(datatypes[j])
		if err != nil {
			return tableMetadata{}, errors.Wrapf(err, "column %q has invalid datatype", label)
		}
		cols[j].ColMeta.Label = label
		cols[j].ColMeta.Type = t
		if t == execute.TTime {
			switch desc {
			case "RFC3339":
				cols[j].fmt = time.RFC3339
			case "RFC3339Nano":
				cols[j].fmt = time.RFC3339Nano
			default:
				cols[j].fmt = desc
			}
		}
		if defaults[j] != "" {
			v, err := decodeValue(defaults[j], cols[j])
			if err != nil {
				return tableMetadata{}, errors.Wrapf(err, "column %q has invalid default value", label)
			}
			defaultValues[j] = v
		}
		partitionValues[j] = partitions[j] == "true"
	}

	return tableMetadata{
		ResultID:   resultID,
		TableID:    tableID,
		Cols:       cols,
		Partitions: partitionValues,
		Defaults:   defaultValues,
		NumFields:  n,
	}, nil
}

type blockDecoder struct {
	r *csv.Reader
	c ResultDecoderConfig

	defaults []interface{}

	meta tableMetadata

	initialized bool
	id          string
	key         execute.PartitionKey
	cols        []colMeta

	builder *execute.ColListBlockBuilder

	more bool

	eof       bool
	extraLine []string
}

func newBlock(
	r *csv.Reader,
	c ResultDecoderConfig,
	meta tableMetadata,
	extraLine []string,
) (*blockDecoder, error) {
	b := &blockDecoder{
		r:    r,
		c:    c,
		meta: meta,
	}
	var err error
	b.more, err = b.advance(extraLine)
	if err != nil {
		return nil, err
	}
	if !b.initialized {
		return b, b.init(nil)
	}
	return b, nil
}

func (b *blockDecoder) Do(f func(execute.ColReader) error) (err error) {
	// Send off first batch from first advance call.
	err = f(b.builder.RawBlock())
	if err != nil {
		return
	}
	b.builder.ClearData()

	for b.more {
		b.more, err = b.advance(nil)
		if err != nil {
			return
		}
		err = f(b.builder.RawBlock())
		if err != nil {
			return
		}
		b.builder.ClearData()
	}
	return
}

// advance reads the csv data until the end of the block or bufSize rows have been read.
// Advance returns whether there is more data and any error.
func (b *blockDecoder) advance(extraLine []string) (bool, error) {
	var line, record []string
	var err error
	for !b.initialized || b.builder.NRows() < b.c.MaxBufferCount {
		if len(extraLine) > 0 {
			line = extraLine
			extraLine = nil
		} else {
			l, err := b.r.Read()
			if err != nil {
				if err == io.EOF {
					b.eof = true
					return false, nil
				}
				return false, err
			}
			line = l
		}
		if len(line) != b.meta.NumFields {
			if len(line) > annotationIdx && line[annotationIdx] == "" {
				return false, csv.ErrFieldCount
			}
			goto DONE
		}

		// Check for new annotation
		if line[annotationIdx] != "" {
			goto DONE
		}

		if !b.initialized {
			if err := b.init(line); err != nil {
				return false, err
			}
			b.initialized = true
		}

		// check if we have tableID that is now different
		if line[tableIdx] != "" && line[tableIdx] != b.id {
			goto DONE
		}

		record = line[recordStartIdx:]
		err = b.appendRecord(record)
		if err != nil {
			return false, err
		}
	}
	return true, nil

DONE:
	// table is done
	b.extraLine = line
	if !b.initialized {
		return false, errors.New("table was not initialized, missing partition key data")
	}
	return false, nil
}

func (b *blockDecoder) init(line []string) error {
	if b.meta.TableID != "" {
		b.id = b.meta.TableID
	} else if len(line) != 0 {
		b.id = line[tableIdx]
	} else {
		return errors.New("missing table ID")
	}
	var record []string
	if len(line) != 0 {
		record = line[recordStartIdx:]
	}
	keyCols := make([]execute.ColMeta, 0, len(b.meta.Cols))
	keyValues := make([]interface{}, 0, len(b.meta.Cols))
	for j, c := range b.meta.Cols {
		if b.meta.Partitions[j] {
			var value interface{}
			if b.meta.Defaults[j] != nil {
				value = b.meta.Defaults[j]
			} else if record != nil {
				v, err := decodeValue(record[j], c)
				if err != nil {
					return err
				}
				value = v
			} else {
				return fmt.Errorf("missing value for partition key column %q", c.Label)
			}
			keyCols = append(keyCols, c.ColMeta)
			keyValues = append(keyValues, value)
		}
	}

	key := execute.NewPartitionKey(keyCols, keyValues)
	b.builder = execute.NewColListBlockBuilder(key, newUnlimitedAllocator())
	for _, c := range b.meta.Cols {
		b.builder.AddCol(c.ColMeta)
	}

	return nil
}

func (b *blockDecoder) appendRecord(record []string) error {
	for j, c := range b.meta.Cols {
		if record[j] == "" && b.meta.Defaults[j] != nil {
			switch c.Type {
			case execute.TBool:
				v := b.meta.Defaults[j].(bool)
				b.builder.AppendBool(j, v)
			case execute.TInt:
				v := b.meta.Defaults[j].(int64)
				b.builder.AppendInt(j, v)
			case execute.TUInt:
				v := b.meta.Defaults[j].(uint64)
				b.builder.AppendUInt(j, v)
			case execute.TFloat:
				v := b.meta.Defaults[j].(float64)
				b.builder.AppendFloat(j, v)
			case execute.TString:
				v := b.meta.Defaults[j].(string)
				b.builder.AppendString(j, v)
			case execute.TTime:
				v := b.meta.Defaults[j].(execute.Time)
				b.builder.AppendTime(j, v)
			default:
				return fmt.Errorf("unsupported column type %v", c.Type)
			}
			continue
		}
		if err := decodeValueInto(j, c, record[j], b.builder); err != nil {
			return err
		}
	}
	return nil
}

func (b *blockDecoder) RefCount(n int) {}

func (b *blockDecoder) Key() execute.PartitionKey {
	return b.builder.Key()
}

func (b *blockDecoder) Cols() []execute.ColMeta {
	return b.builder.Cols()
}

type colMeta struct {
	execute.ColMeta
	fmt string
}

func newUnlimitedAllocator() *execute.Allocator {
	return &execute.Allocator{Limit: math.MaxInt64}
}

type ResultEncoder struct {
	c ResultEncoderConfig
}

// ResultEncoderConfig are options that can be specified on the ResultEncoder.
type ResultEncoderConfig struct {
	// Annotations is a list of annotations to include.
	Annotations []string

	// NoHeader indicates whether a header row should be added.
	NoHeader bool

	// Delimiter is the character to delimite columns.
	// It must not be \r, \n, or the Unicode replacement character (0xFFFD).
	Delimiter rune
}

func DefaultEncoderConfig() ResultEncoderConfig {
	return ResultEncoderConfig{
		Annotations: []string{datatypeAnnotation, partitionAnnotation, defaultAnnotation},
	}
}

// NewResultEncoder creates a new encoder with the provided configuration.
func NewResultEncoder(c ResultEncoderConfig) *ResultEncoder {
	return &ResultEncoder{
		c: c,
	}
}

func (e *ResultEncoder) Encode(w io.Writer, result execute.Result) error {
	tableID := 0
	metaCols := []colMeta{
		{ColMeta: execute.ColMeta{Label: "", Type: execute.TInvalid}},
		{ColMeta: execute.ColMeta{Label: resultLabel, Type: execute.TString}},
		{ColMeta: execute.ColMeta{Label: tableLabel, Type: execute.TInt}},
	}
	writer := csv.NewWriter(w)
	if e.c.Delimiter != 0 {
		writer.Comma = e.c.Delimiter
	}
	writer.UseCRLF = true

	var lastCols []colMeta

	return result.Blocks().Do(func(b execute.Block) error {
		// Update cols with block cols
		cols := metaCols
		for _, c := range b.Cols() {
			cm := colMeta{ColMeta: c}
			if c.Type == execute.TTime {
				cm.fmt = time.RFC3339Nano
			}
			cols = append(cols, cm)
		}
		// pre-allocate row slice
		row := make([]string, len(cols))

		schemaChanged := !equalCols(cols, lastCols)

		if schemaChanged {
			if len(lastCols) > 0 {
				// Write out empty line if not first block
				writer.Write(nil)
			}

			if err := writeSchema(writer, &e.c, row, cols, false, b.Key()); err != nil {
				return err
			}
		}

		if execute.ContainsStr(e.c.Annotations, defaultAnnotation) {
			for j := range cols {
				switch j {
				case annotationIdx:
					row[j] = ""
				case resultIdx:
					row[j] = ""
				case tableIdx:
					row[j] = strconv.Itoa(tableID)
				default:
					row[j] = ""
				}
			}
		}

		count := 0
		err := b.Do(func(cr execute.ColReader) error {
			record := row[recordStartIdx:]
			l := cr.Len()
			count += l
			for i := 0; i < l; i++ {
				for j, c := range cols[recordStartIdx:] {
					v, err := encodeValueFrom(i, j, c, cr)
					if err != nil {
						return err
					}
					record[j] = v
				}
				writer.Write(row)
			}
			writer.Flush()
			return writer.Error()
		})
		if err != nil {
			return err
		}

		// If the table was empty add its own schema
		if !schemaChanged && count == 0 {
			if len(lastCols) > 0 {
				// Write out empty line if not first block
				writer.Write(nil)
			}
			if err := writeSchema(writer, &e.c, row, cols, true, b.Key()); err != nil {
				return err
			}
		}

		tableID++
		lastCols = cols
		return writer.Error()
	})
}

func writeSchema(writer *csv.Writer, c *ResultEncoderConfig, row []string, cols []colMeta, useKeyDefaults bool, key execute.PartitionKey) error {
	defaults := make([]string, len(row))
	for j, c := range cols {
		switch j {
		case annotationIdx:
		case resultIdx:
			// TODO use real result name
			defaults[j] = "_result"
		case tableIdx:
			defaults[j] = ""
		default:
			if useKeyDefaults {
				kj := execute.ColIdx(c.Label, key.Cols())
				if kj >= 0 {
					v, err := encodeValue(key.Value(kj), c)
					if err != nil {
						return err
					}
					defaults[j] = v
				} else {
					defaults[j] = ""
				}
			} else {
				defaults[j] = ""
			}
		}
	}
	// TODO: use real result name
	if err := writeAnnotations(writer, c.Annotations, row, defaults, cols, key); err != nil {
		return err
	}

	if !c.NoHeader {
		// Write labels header
		for j, c := range cols {
			row[j] = c.Label
		}
		writer.Write(row)
	}
	return writer.Error()
}

func writeAnnotations(writer *csv.Writer, annotations []string, row, defaults []string, cols []colMeta, key execute.PartitionKey) error {
	for _, annotation := range annotations {
		switch annotation {
		case datatypeAnnotation:
			if err := writeDatatypes(writer, row, cols); err != nil {
				return err
			}
		case partitionAnnotation:
			if err := writePartitions(writer, row, cols, key); err != nil {
				return err
			}
		case defaultAnnotation:
			if err := writeDefaults(writer, row, defaults); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported annotation %q", annotation)
		}
	}
	return writer.Error()
}

func writeDatatypes(writer *csv.Writer, row []string, cols []colMeta) error {
	for j, c := range cols {
		if j == annotationIdx {
			row[j] = commentPrefix + datatypeAnnotation
			continue
		}
		switch c.Type {
		case execute.TBool:
			row[j] = boolDatatype
		case execute.TInt:
			row[j] = intDatatype
		case execute.TUInt:
			row[j] = uintDatatype
		case execute.TFloat:
			row[j] = floatDatatype
		case execute.TString:
			row[j] = stringDatatype
		case execute.TTime:
			row[j] = timeDataTypeWithFmt
		default:
			return fmt.Errorf("unknown column type %v", c.Type)
		}
	}
	return writer.Write(row)
}

func writePartitions(writer *csv.Writer, row []string, cols []colMeta, key execute.PartitionKey) error {
	for j, c := range cols {
		if j == annotationIdx {
			row[j] = commentPrefix + partitionAnnotation
			continue
		}
		row[j] = strconv.FormatBool(key.HasCol(c.Label))
	}
	return writer.Write(row)
}

func writeDefaults(writer *csv.Writer, row, defaults []string) error {
	for j := range defaults {
		switch j {
		case annotationIdx:
			row[j] = commentPrefix + defaultAnnotation
		default:
			row[j] = defaults[j]
		}
	}
	return writer.Write(row)
}

func decodeValue(value string, c colMeta) (v interface{}, err error) {
	switch c.Type {
	case execute.TBool:
		v, err = strconv.ParseBool(value)
	case execute.TInt:
		v, err = strconv.ParseInt(value, 10, 64)
	case execute.TUInt:
		v, err = strconv.ParseUint(value, 10, 64)
	case execute.TFloat:
		v, err = strconv.ParseFloat(value, 64)
	case execute.TString:
		v = value
	case execute.TTime:
		v, err = decodeTime(value, c.fmt)
	default:
		return nil, fmt.Errorf("unsupported type %v", c.Type)
	}
	return
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

func encodeValue(value interface{}, c colMeta) (string, error) {
	switch c.Type {
	case execute.TBool:
		return strconv.FormatBool(value.(bool)), nil
	case execute.TInt:
		return strconv.FormatInt(value.(int64), 10), nil
	case execute.TUInt:
		return strconv.FormatUint(value.(uint64), 10), nil
	case execute.TFloat:
		return strconv.FormatFloat(value.(float64), 'f', -1, 64), nil
	case execute.TString:
		return value.(string), nil
	case execute.TTime:
		return encodeTime(value.(execute.Time), c.fmt), nil
	default:
		return "", fmt.Errorf("unknown type %v", c.Type)
	}
}

func encodeValueFrom(i, j int, c colMeta, cr execute.ColReader) (string, error) {
	switch c.Type {
	case execute.TBool:
		return strconv.FormatBool(cr.Bools(j)[i]), nil
	case execute.TInt:
		return strconv.FormatInt(cr.Ints(j)[i], 10), nil
	case execute.TUInt:
		return strconv.FormatUint(cr.UInts(j)[i], 10), nil
	case execute.TFloat:
		return strconv.FormatFloat(cr.Floats(j)[i], 'f', -1, 64), nil
	case execute.TString:
		return cr.Strings(j)[i], nil
	case execute.TTime:
		return encodeTime(cr.Times(j)[i], c.fmt), nil
	default:
		return "", fmt.Errorf("unknown type %v", c.Type)
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

func copyLine(line []string) []string {
	cpy := make([]string, len(line))
	copy(cpy, line)
	return cpy
}

// decodeType returns the execute.DataType and any additional format description.
func decodeType(datatype string) (t execute.DataType, desc string, err error) {
	split := strings.SplitN(datatype, ":", 2)
	if len(split) > 1 {
		desc = split[1]
	}
	typ := split[0]
	switch typ {
	case boolDatatype:
		t = execute.TBool
	case intDatatype:
		t = execute.TInt
	case uintDatatype:
		t = execute.TUInt
	case floatDatatype:
		t = execute.TFloat
	case stringDatatype:
		t = execute.TString
	case timeDatatype:
		t = execute.TTime
	default:
		err = fmt.Errorf("unsupported data type %q", typ)
	}
	return
}

func equalCols(a, b []colMeta) bool {
	if len(a) != len(b) {
		return false
	}
	for j := range a {
		if a[j] != b[j] {
			return false
		}
	}
	return true
}

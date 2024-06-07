package exporter

import (
	"context"
	"io"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"
	"github.com/influxdata/influxdb/cmd/influx_inspect/export/parquet/models"
	"github.com/influxdata/influxdb/cmd/influx_inspect/export/parquet/table"
	"github.com/pkg/errors"
)

type options struct {
	writerProps []parquet.WriterProperty
}

func (o *options) apply(fns ...OptionFn) {
	for _, fn := range fns {
		fn(o)
	}
}

type OptionFn func(*options)

func WithParquetWriterProperties(opts ...parquet.WriterProperty) OptionFn {
	return func(o *options) {
		o.writerProps = opts
	}
}

type Exporter struct {
	table  *table.Table
	schema *arrow.Schema

	writerProps      *parquet.WriterProperties
	arrowWriterProps pqarrow.ArrowWriterProperties
}

func New(t *table.Table, fns ...OptionFn) *Exporter {
	var opts options
	opts.apply(fns...)

	columns := t.Columns()
	fields := make([]arrow.Field, 0, len(columns))
	fields = append(fields, arrow.Field{
		Name:     "time",
		Type:     &arrow.TimestampType{Unit: arrow.Nanosecond},
		Nullable: false,
	})

	// skip timestamp column
	for _, col := range columns[1:] {
		fields = append(fields, arrow.Field{
			Name:     col.Name(),
			Type:     col.DataType(),
			Nullable: true,
		})
	}
	schema := arrow.NewSchema(fields, nil)

	writerOptions := opts.writerProps

	// override subset of options to ensure consistency
	writerOptions = append(writerOptions,
		parquet.WithDictionaryDefault(false),
		parquet.WithDataPageVersion(parquet.DataPageV2),
		parquet.WithVersion(parquet.V2_LATEST),
	)
	writerProps := parquet.NewWriterProperties(writerOptions...)

	return &Exporter{
		table:       t,
		schema:      schema,
		writerProps: writerProps,
		// no reason to override these defaults
		arrowWriterProps: pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema()),
	}
}

func (e *Exporter) WriteChunk(ctx context.Context, parquet io.Writer, metaW io.Writer, maxSize int) (done bool, err error) {
	if e.table.Err() != nil {
		return false, e.table.Err()
	}

	parquetWriter := writeCounter{Writer: parquet}

	var fw *pqarrow.FileWriter
	fw, err = pqarrow.NewFileWriter(e.schema, &parquetWriter, e.writerProps, e.arrowWriterProps)
	if err != nil {
		return false, err
	}
	defer func() {
		closeErr := fw.Close()
		if err == nil {
			// we only care if the file fails to close when there was no
			// previous error
			err = closeErr
		}
	}()

	var (
		firstSeriesKey     models.Escaped
		readInfo           table.ReadInfo
		firstTimestamp     arrow.Timestamp
		lastTimestamp      arrow.Timestamp
		seenFirstSeriesKey = false
	)
	defer func() {
		if err == nil {
			err = writeMeta(Meta{
				FirstSeriesKey: firstSeriesKey,
				FirstTimestamp: int64(firstTimestamp),
				LastSeriesKey:  readInfo.LastGroupKey,
				LastTimestamp:  int64(lastTimestamp),
			}, metaW)
		}
	}()

	for parquetWriter.Count() < maxSize {
		var cols []arrow.Array
		cols, readInfo = e.table.Next(ctx)
		if len(cols) == 0 {
			return true, errors.Wrap(e.table.Err(), "reading table")
		}

		if !seenFirstSeriesKey {
			firstSeriesKey = readInfo.FirstGroupKey.Clone()
			firstTimestamp = cols[0].(*array.Timestamp).Value(0)
			seenFirstSeriesKey = true
		}

		if cols[0].Len() > 0 {
			lastTimestamp = cols[0].(*array.Timestamp).Value(cols[0].Len() - 1)

			rec := array.NewRecord(e.schema, cols, int64(cols[0].Len()))
			err = fw.Write(rec)
			rec.Release()
			if err != nil {
				return false, errors.Wrap(err, "writing Parquet")
			}
		}

		for _, col := range cols {
			col.Release()
		}
	}

	return
}

// writeCounter counts the number of bytes written to an io.Writer
type writeCounter struct {
	io.Writer
	count int
}

func (c *writeCounter) Write(buf []byte) (n int, err error) {
	defer func() { c.count += n }()
	return c.Writer.Write(buf)
}

// Count function return counted bytes
func (c *writeCounter) Count() int {
	return c.count
}

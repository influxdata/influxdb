package export

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/apache/arrow/go/v16/parquet"
	"github.com/apache/arrow/go/v16/parquet/compress"
	pqexport "github.com/influxdata/influxdb/cmd/influx_inspect/export/parquet/exporter"
	"github.com/influxdata/influxdb/cmd/influx_inspect/export/parquet/index"
	"github.com/influxdata/influxdb/cmd/influx_inspect/export/parquet/models"
	"github.com/influxdata/influxdb/cmd/influx_inspect/export/parquet/resultset"
	"github.com/influxdata/influxdb/cmd/influx_inspect/export/parquet/table"
	tsdb "github.com/influxdata/influxdb/cmd/influx_inspect/export/parquet/tsm1"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
)

//
// Export to Parquet file(s) is done per each TSM file. The files are apparently not sorted.
// Therefore, neither are output files. So for example `table-00001.parquet` may contain older data
// than `table-00000.parquet`.
// If the data should be sorted then
//  1. writeValuesParquet must collect multiple chunks of field values, and they should be sorted later
//  2. exporter must call exportDone after all files were processed
//

// sequence is used for the suffix of generated parquet files
var sequence int

// vc is the key:field:values collection of exported values
var vc map[string]map[string][]tsm1.Value

func (cmd *Command) writeValuesParquet(_ io.Writer, seriesKey []byte, field string, values []tsm1.Value) error {
	if vc == nil {
		vc = make(map[string]map[string][]tsm1.Value)
	}

	key := string(seriesKey)
	fields, ok := vc[key]
	if !ok {
		fields = make(map[string][]tsm1.Value)
		vc[key] = fields
	}
	fields[field] = values
	return nil
}

func (cmd *Command) exportDoneParquet(_ string) error {
	defer func() {
		vc = nil
	}()

	var schema *index.MeasurementSchema

	for key, fields := range vc {
		// since code from v2 exporter is used, we need v2 series key
		seriesKeyEx := append([]byte(models.MeasurementTagKey+"="), []byte(key)...)
		// get tags
		tags := models.ParseTags(models.Escaped{B: seriesKeyEx})
		tagSet := make(map[string]struct{}, len(tags))
		for _, tag := range tags {
			tagSet[string(tag.Key)] = struct{}{}
		}
		// get fields
		fieldSet := make(map[index.MeasurementField]struct{}, len(fields))
		for field, values := range fields {
			bt, err := tsdb.BlockTypeForType(values[0].Value())
			if err != nil {
				return err
			}
			mf := index.MeasurementField{
				Name: field,
				Type: bt,
			}
			fieldSet[mf] = struct{}{}
		}
		// populate schema
		schema = &index.MeasurementSchema{
			TagSet:   tagSet,
			FieldSet: fieldSet,
		}
		// schema does not change in a table
		break
	}

	if err := cmd.writeToParquet(newResultSet(vc), schema); err != nil {
		return err
	}

	return nil
}

func (cmd *Command) writeToParquet(rs resultset.ResultSet, schema *index.MeasurementSchema) error {
	cfg := zap.NewDevelopmentConfig()
	if cmd.usingStdOut() {
		cfg.OutputPaths = []string{
			"stdout",
		}
	}
	log, _ := cfg.Build()

	log.Info("Generating parquet", zap.Int("chunk_size", cmd.pqChunkSize))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t, err := table.New(rs, schema)
	if err != nil {
		return err
	}

	exporter := pqexport.New(t,
		pqexport.WithParquetWriterProperties(
			parquet.WithCompression(compress.Codecs.Snappy)))

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		start := time.Now()
		var (
			lastSeriesCount int64
			lastRowCount    int64
		)
		for {
			ticker := time.NewTicker(30 * time.Second)
			select {
			case <-ctx.Done():
				stats := t.Stats()
				log.Info("Total Processed", zap.Int64("series_count", stats.SeriesCount), zap.Int64("rows", stats.RowCount), zap.Duration("build_time", time.Since(start)))
				return
			case <-ticker.C:
				stats := t.Stats()
				var (
					lastSeries int64
					lastRows   int64
				)
				lastSeries, lastSeriesCount = stats.SeriesCount-lastSeriesCount, stats.SeriesCount
				lastRows, lastRowCount = stats.RowCount-lastRowCount, stats.RowCount
				log.Info("Processed",
					zap.Int64("total_series", stats.SeriesCount),
					zap.Int64("total_rows", stats.RowCount),
					zap.Int64("last_series", lastSeries),
					zap.Int64("last_rows", lastRows),
				)
			}
		}
	}()

	log.Info("Schema info",
		zap.Int("tag_count", len(schema.TagSet)),
		zap.Int("field_count", len(schema.FieldSet)))

	var done bool
	for !done && err == nil {
		done, err = func() (done bool, err error) {
			defer func() { sequence++ }()

			var parquetFile *os.File
			{
				fileName := filepath.Join(cmd.out, fmt.Sprintf("table-%05d.parquet", sequence))
				parquetFile, err = os.Create(fileName)
				if err != nil {
					return false, err
				}
				defer func() {
					closeErr := parquetFile.Close()
					if err == nil {
						// we only care if the file fails to close when there was no
						// previous error
						err = closeErr
					}
				}()
			}

			var metaFile *os.File
			{
				fileName := filepath.Join(cmd.out, fmt.Sprintf("table-%05d.meta.json", sequence))
				metaFile, err = os.Create(fileName)
				if err != nil {
					return false, err
				}
				defer func() {
					closeErr := metaFile.Close()
					if err == nil {
						// we only care if the file fails to close when there was no
						// previous error
						err = closeErr
					}
				}()
			}

			stats := t.Stats()
			log.Info("Starting Parquet file.",
				zap.Int64("total_series", stats.SeriesCount),
				zap.Int64("total_rows", stats.RowCount),
				zap.String("parquet", parquetFile.Name()),
				zap.String("meta", metaFile.Name()),
			)

			defer func() {
				stats := t.Stats()
				log.Info("Closing Parquet file.",
					zap.Int64("total_series", stats.SeriesCount),
					zap.Int64("total_rows", stats.RowCount),
					zap.String("parquet", parquetFile.Name()),
					zap.String("meta", metaFile.Name()),
				)
			}()

			return exporter.WriteChunk(ctx, parquetFile, metaFile, cmd.pqChunkSize)
		}()
	}

	cancel()
	wg.Wait()

	if err != nil {
		log.Error("Failed to write table.", zap.Error(err))
	} else {
		log.Info("Finished")
	}

	return nil
}

// resultSet implements resultset.ResultSet over exported TSM data
type resultSet struct {
	x          map[string]map[string][]tsm1.Value
	keys       []string
	keyIndex   int
	fields     []string
	fieldIndex int
}

func newResultSet(x map[string]map[string][]tsm1.Value) resultset.ResultSet {
	return &resultSet{
		x:    x,
		keys: maps.Keys(x),
	}
}

func (r *resultSet) Next(ctx context.Context) resultset.SeriesCursor {
	if r.keyIndex >= len(r.keys) {
		return nil
	}

	if r.fields == nil {
		r.fields = maps.Keys(r.x[r.keys[r.keyIndex]])
	}

	if r.fieldIndex >= len(r.fields) {
		r.fields = nil
		r.fieldIndex = 0
		r.keyIndex++
		return r.Next(ctx)
	}

	defer func() {
		r.fieldIndex++
	}()

	groupKey := r.keys[r.keyIndex]
	fieldName := r.fields[r.fieldIndex]
	// since code from v2 exporter is used, we need v2 series key
	seriesKeyEx := append([]byte(groupKey), []byte(","+models.FieldKeyTagKey+"="+fieldName)...)
	values := r.x[groupKey][fieldName]

	switch values[0].Value().(type) {
	case int64:
		return newExportedCursor[int64](seriesKeyEx, values)
	case float64:
		return newExportedCursor[float64](seriesKeyEx, values)
	case string:
		return newExportedCursor[string](seriesKeyEx, values)
	case uint64:
		return newExportedCursor[uint64](seriesKeyEx, values)
	case bool:
		return newExportedCursor[bool](seriesKeyEx, values)
	}

	return nil
}

func (r *resultSet) Close() {
}

func (r *resultSet) Err() error {
	return nil
}

// exportedCursor implements resultset.TypedCursor for exported TSM data
type exportedCursor[T resultset.BlockType] struct {
	seriesKey  []byte
	chunks     []*resultset.TimeArray[T]
	chunkIndex int
}

func newExportedCursor[T resultset.BlockType](seriesKey []byte, values []tsm1.Value) *exportedCursor[T] {
	c := &exportedCursor[T]{
		seriesKey: seriesKey,
		chunks: []*resultset.TimeArray[T]{
			&resultset.TimeArray[T]{},
		},
	}
	c.chunks[0].Timestamps = make([]int64, len(values))
	c.chunks[0].Values = make([]T, len(values))
	for i, value := range values {
		c.chunks[0].Timestamps[i] = value.UnixNano()
		c.chunks[0].Values[i] = value.(TsmValue).Value().(T)
	}
	return c
}

func (c *exportedCursor[T]) SeriesKey() models.Escaped {
	return models.Escaped{B: c.seriesKey}
}

func (c *exportedCursor[T]) Close() {
}

func (c *exportedCursor[T]) Err() error {
	return nil
}

func (c *exportedCursor[T]) Next() *resultset.TimeArray[T] {
	if c.chunkIndex >= len(c.chunks) {
		return nil
	}
	defer func() {
		c.chunkIndex++
	}()
	return c.chunks[c.chunkIndex]
}

// TsmValue is helper for accessing value of tsm1.Value
type TsmValue interface {
	Value() interface{}
}

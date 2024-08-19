package export

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/apache/arrow/go/v16/parquet"
	"github.com/apache/arrow/go/v16/parquet/compress"
	pqexport "github.com/influxdata/influxdb/cmd/influx_tools/parquet/exporter/parquet/exporter"
	"github.com/influxdata/influxdb/cmd/influx_tools/parquet/exporter/parquet/index"
	"github.com/influxdata/influxdb/cmd/influx_tools/parquet/exporter/parquet/resultset"
	"github.com/influxdata/influxdb/cmd/influx_tools/parquet/exporter/parquet/table"
	"github.com/influxdata/influxdb/pkg/errors"
	"go.uber.org/zap"
)

func New(pqChunkSize int) *Exporter {
	return &Exporter{
		pqChunkSize: pqChunkSize,
	}
}

type Exporter struct {
	// sequence is used for the suffix of generated parquet files
	sequence int

	// pqChunkSize chunk size
	pqChunkSize int
}

func (cmd *Exporter) WriteTo(out string, rs resultset.ResultSet, schema *index.MeasurementSchema) error {
	cfg := zap.NewDevelopmentConfig()
	cfg.OutputPaths = []string{
		"stdout",
	}

	log, err := cfg.Build()
	if err != nil {
		return fmt.Errorf("failed creating logger instance: %w", err)
	}

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
				log.Info("Total Processed",
					zap.Int64("series_count", stats.SeriesCount),
					zap.Int64("rows", stats.RowCount),
					zap.Duration("build_time", time.Since(start)))
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
			defer func() { cmd.sequence++ }()

			var parquetFile *os.File
			{
				fileName := filepath.Join(out, fmt.Sprintf("table-%05d.parquet", cmd.sequence))
				parquetFile, err = os.Create(fileName)
				if err != nil {
					return false, fmt.Errorf("failed creating parquet file %s for export: %w", fileName, err)
				}
				defer errors.Capture(&err, parquetFile.Close)
			}

			var metaFile *os.File
			{
				fileName := filepath.Join(out, fmt.Sprintf("table-%05d.meta.json", cmd.sequence))
				metaFile, err = os.Create(fileName)
				if err != nil {
					return false, fmt.Errorf("failed creating parquet meta file %s: %w", fileName, err)
				}
				defer errors.Capture(&err, metaFile.Close)
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

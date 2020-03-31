package generate

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/influxdata/influxdb/cmd/influxd/generate/internal/shard"
	"github.com/influxdata/influxdb/kit/errors"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/data/gen"
	"github.com/influxdata/influxdb/pkg/limiter"
	"github.com/influxdata/influxdb/storage"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/seriesfile"
	"github.com/influxdata/influxdb/tsdb/tsi1"
	"github.com/influxdata/influxdb/tsdb/tsm1"
)

type Generator struct {
	sfile *seriesfile.SeriesFile

	// Clean specifies whether to clean any of the data related files
	Clean CleanLevel
}

func (g *Generator) Run(ctx context.Context, path string, gen gen.SeriesGenerator) ([]string, error) {
	path = filepath.Join(path, "engine")
	config := storage.NewConfig()

	switch g.Clean {
	case CleanLevelTSM:
		if err := os.RemoveAll(path); err != nil {
			return nil, err
		}

	case CleanLevelAll:
		if err := os.RemoveAll(path); err != nil {
			return nil, err
		}
	}

	g.sfile = seriesfile.NewSeriesFile(config.GetSeriesFilePath(path))
	if err := g.sfile.Open(ctx); err != nil {
		return nil, err
	}
	defer g.sfile.Close()
	g.sfile.DisableCompactions()

	ti := tsi1.NewIndex(g.sfile, config.Index, tsi1.WithPath(config.GetIndexPath(path)))
	if err := ti.Open(ctx); err != nil {
		return nil, fmt.Errorf("error opening TSI1 index: %s", err.Error())
	}

	files, err := g.writeShard(ti, gen, config.GetEnginePath(path))
	if err != nil {
		return nil, fmt.Errorf("error writing data: %s", err.Error())
	}

	ti.Compact()
	ti.Wait()
	if err := ti.Close(); err != nil {
		return nil, fmt.Errorf("error compacting TSI1 index: %s", err.Error())
	}

	var (
		wg   sync.WaitGroup
		errs errors.List
	)

	parts := g.sfile.Partitions()
	wg.Add(len(parts))
	ch := make(chan error, len(parts))
	limit := limiter.NewFixed(runtime.NumCPU())

	for i := range parts {
		go func(n int) {
			limit.Take()
			defer func() {
				wg.Done()
				limit.Release()
			}()

			p := parts[n]
			c := seriesfile.NewSeriesPartitionCompactor()
			if _, err := c.Compact(p); err != nil {
				ch <- fmt.Errorf("error compacting series partition %d: %s", n, err.Error())
			}
		}(i)
	}
	wg.Wait()

	close(ch)
	for e := range ch {
		errs.Append(e)
	}

	if err := errs.Err(); err != nil {
		return nil, err
	}

	return files, nil
}

// seriesBatchSize specifies the number of series keys passed to the index.
const seriesBatchSize = 1000

func (g *Generator) writeShard(idx *tsi1.Index, sg gen.SeriesGenerator, path string) ([]string, error) {
	if err := os.MkdirAll(path, 0777); err != nil {
		return nil, err
	}

	sw, err := shard.NewWriter(path, shard.AutoNumber())
	if err != nil {
		return nil, err
	}
	defer sw.Close()

	coll := &tsdb.SeriesCollection{
		Keys:  make([][]byte, 0, seriesBatchSize),
		Names: make([][]byte, 0, seriesBatchSize),
		Tags:  make([]models.Tags, 0, seriesBatchSize),
		Types: make([]models.FieldType, 0, seriesBatchSize),
	}

	for sg.Next() {
		seriesKey := sg.Key()
		coll.Keys = append(coll.Keys, seriesKey)
		coll.Names = append(coll.Names, sg.ID())
		coll.Tags = append(coll.Tags, sg.Tags())
		coll.Types = append(coll.Types, sg.FieldType())

		if coll.Length() == seriesBatchSize {
			if err := idx.CreateSeriesListIfNotExists(coll); err != nil {
				return nil, err
			}
			coll.Truncate(0)
		}

		vg := sg.TimeValuesGenerator()

		key := tsm1.SeriesFieldKeyBytes(string(seriesKey), string(sg.Field()))
		for vg.Next() {
			sw.WriteV(key, vg.Values())
		}

		if err := sw.Err(); err != nil {
			return nil, err
		}
	}

	if coll.Length() > 0 {
		if err := idx.CreateSeriesListIfNotExists(coll); err != nil {
			return nil, err
		}
	}

	return sw.Files(), nil
}

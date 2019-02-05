package exec

import (
	"context"
	"fmt"
	"path"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/influxdata/influxdb/cmd/influx_tools/internal/errlist"
	"github.com/influxdata/influxdb/cmd/influx_tools/internal/shard"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/data/gen"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"github.com/influxdata/influxdb/tsdb/index/tsi1"
)

type Generator struct {
	Concurrency int
	BuildTSI    bool

	sfile *tsdb.SeriesFile
}

func (g *Generator) Run(ctx context.Context, database, shardPath string, groups []meta.ShardGroupInfo, gens []gen.SeriesGenerator) (err error) {
	limit := make(chan struct{}, g.Concurrency)
	for i := 0; i < g.Concurrency; i++ {
		limit <- struct{}{}
	}

	var (
		wg   sync.WaitGroup
		errs errlist.ErrorList
		ch   = make(chan error, len(groups))
	)

	dbPath := path.Dir(shardPath)
	g.sfile = tsdb.NewSeriesFile(filepath.Join(dbPath, tsdb.SeriesFileDirectory))
	if err := g.sfile.Open(); err != nil {
		return err
	}
	defer g.sfile.Close()
	g.sfile.DisableCompactions()

	wg.Add(len(groups))
	for i := 0; i < len(groups); i++ {
		go func(n int) {
			<-limit
			defer func() {
				wg.Done()
				limit <- struct{}{}
			}()

			sgi := &groups[n]
			if len(sgi.Shards) > 1 {
				ch <- fmt.Errorf("multiple shards for the same owner %v", sgi.Shards[0].Owners)
				return
			}

			id := sgi.Shards[0].ID

			var (
				idx seriesIndex
				ti  *tsi1.Index
			)
			if g.BuildTSI {
				ti = tsi1.NewIndex(g.sfile, database, tsi1.WithPath(filepath.Join(shardPath, strconv.Itoa(int(id)), "index")))
				if err := ti.Open(); err != nil {
					ch <- fmt.Errorf("error opening TSI1 index %d: %s", id, err.Error())
					return
				}
				idx = ti
			} else {
				idx = &seriesFileAdapter{sf: g.sfile, buf: make([]byte, 0, 2048)}
			}

			if err := g.writeShard(idx, gens[n], id, shardPath); err != nil {
				ch <- fmt.Errorf("error writing shard %d: %s", id, err.Error())
			}

			if ti != nil {
				ti.Compact()
				ti.Wait()
				if err := ti.Close(); err != nil {
					ch <- fmt.Errorf("error compacting TSI1 index %d: %s", id, err.Error())
				}
			}
		}(i)
	}
	wg.Wait()

	close(ch)
	for e := range ch {
		errs.Add(e)
	}

	parts := g.sfile.Partitions()
	wg.Add(len(parts))
	ch = make(chan error, len(parts))
	for i := range parts {
		go func(n int) {
			<-limit
			defer func() {
				wg.Done()
				limit <- struct{}{}
			}()

			p := parts[n]
			c := tsdb.NewSeriesPartitionCompactor()
			if err := c.Compact(p); err != nil {
				ch <- fmt.Errorf("error compacting series partition %d: %s", n, err.Error())
			}
		}(i)
	}
	wg.Wait()

	close(ch)
	for e := range ch {
		errs.Add(e)
	}

	return errs.Err()
}

// seriesBatchSize specifies the number of series keys passed to the index.
const seriesBatchSize = 1000

func (g *Generator) writeShard(idx seriesIndex, sg gen.SeriesGenerator, id uint64, path string) error {
	sw := shard.NewWriter(id, path)
	defer sw.Close()

	var (
		keys  [][]byte
		names [][]byte
		tags  []models.Tags
	)

	for sg.Next() {
		seriesKey := sg.Key()
		keys = append(keys, seriesKey)
		names = append(names, sg.Name())
		tags = append(tags, sg.Tags())

		if len(keys) == seriesBatchSize {
			if err := idx.CreateSeriesListIfNotExists(keys, names, tags); err != nil {
				return err
			}
			keys = keys[:0]
			names = names[:0]
			tags = tags[:0]
		}

		vg := sg.TimeValuesGenerator()

		key := tsm1.SeriesFieldKeyBytes(string(seriesKey), string(sg.Field()))
		for vg.Next() {
			sw.WriteV(key, vg.Values())
		}

		if err := sw.Err(); err != nil {
			return err
		}
	}

	if len(keys) > seriesBatchSize {
		if err := idx.CreateSeriesListIfNotExists(keys, names, tags); err != nil {
			return err
		}
	}
	return nil
}

type seriesIndex interface {
	CreateSeriesListIfNotExists(keys [][]byte, names [][]byte, tagsSlice []models.Tags) error
}

type seriesFileAdapter struct {
	sf  *tsdb.SeriesFile
	buf []byte
}

func (s *seriesFileAdapter) CreateSeriesListIfNotExists(keys [][]byte, names [][]byte, tagsSlice []models.Tags) (err error) {
	_, err = s.sf.CreateSeriesListIfNotExists(names, tagsSlice)
	return
}

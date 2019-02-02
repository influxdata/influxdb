package importer

import (
	"fmt"
	"path/filepath"
	"strconv"

	"github.com/influxdata/influxdb/cmd/influx_tools/internal/errlist"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"github.com/influxdata/influxdb/tsdb/index/tsi1"
)

type seriesWriter struct {
	keys            [][]byte
	names           [][]byte
	tags            []models.Tags
	seriesBatchSize int
	sfile           *tsdb.SeriesFile
	idx             seriesIndex
}

func newInMemSeriesWriter(sfile *tsdb.SeriesFile, db string, dataPath string, shardPath string, shardID int, buf []byte) (*seriesWriter, error) {
	return &seriesWriter{seriesBatchSize: seriesBatchSize, sfile: sfile, idx: &seriesFileAdapter{sf: sfile, buf: buf}}, nil
}

func newTSI1SeriesWriter(sfile *tsdb.SeriesFile, db string, dataPath string, shardPath string, shardID int) (*seriesWriter, error) {
	ti := tsi1.NewIndex(sfile, db, tsi1.WithPath(filepath.Join(shardPath, strconv.Itoa(shardID), "index")))
	if err := ti.Open(); err != nil {
		return nil, fmt.Errorf("error opening TSI1 index %d: %s", shardID, err.Error())
	}

	return &seriesWriter{seriesBatchSize: seriesBatchSize, sfile: sfile, idx: &tsi1Adapter{ti: ti}}, nil
}

func (sw *seriesWriter) AddSeries(key []byte) error {
	seriesKey, _ := tsm1.SeriesAndFieldFromCompositeKey(key)
	sw.keys = append(sw.keys, seriesKey)

	name, tag := models.ParseKeyBytes(seriesKey)
	sw.names = append(sw.names, name)
	sw.tags = append(sw.tags, tag)

	if len(sw.keys) == sw.seriesBatchSize {
		if err := sw.idx.CreateSeriesListIfNotExists(sw.keys, sw.names, sw.tags); err != nil {
			return err
		}
		sw.keys = sw.keys[:0]
		sw.names = sw.names[:0]
		sw.tags = sw.tags[:0]
	}

	return nil
}

func (sw *seriesWriter) Close() error {
	el := errlist.NewErrorList()
	el.Add(sw.idx.CreateSeriesListIfNotExists(sw.keys, sw.names, sw.tags))
	el.Add(sw.idx.Compact())
	el.Add(sw.idx.Close())
	el.Add(sw.sfile.Close())
	return el.Err()
}

type seriesIndex interface {
	CreateSeriesListIfNotExists(keys [][]byte, names [][]byte, tagsSlice []models.Tags) (err error)
	Compact() error
	Close() error
}

type seriesFileAdapter struct {
	sf  *tsdb.SeriesFile
	buf []byte
}

func (s *seriesFileAdapter) CreateSeriesListIfNotExists(keys [][]byte, names [][]byte, tagsSlice []models.Tags) (err error) {
	_, err = s.sf.CreateSeriesListIfNotExists(names, tagsSlice)
	return err
}

func (s *seriesFileAdapter) Compact() error {
	parts := s.sf.Partitions()
	for i, p := range parts {
		c := tsdb.NewSeriesPartitionCompactor()
		if err := c.Compact(p); err != nil {
			return fmt.Errorf("error compacting series partition %d: %s", i, err.Error())
		}
	}

	return nil
}

func (s *seriesFileAdapter) Close() error {
	return nil
}

type tsi1Adapter struct {
	ti *tsi1.Index
}

func (t *tsi1Adapter) CreateSeriesListIfNotExists(keys [][]byte, names [][]byte, tagsSlice []models.Tags) (err error) {
	return t.ti.CreateSeriesListIfNotExists(keys, names, tagsSlice)
}

func (t *tsi1Adapter) Compact() error {
	t.ti.Compact()
	t.ti.Wait()
	return nil
}

func (t *tsi1Adapter) Close() error {
	return t.ti.Close()
}

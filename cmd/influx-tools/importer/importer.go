package importer

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/influxdata/influxdb/cmd/influx-tools/internal/errlist"
	"github.com/influxdata/influxdb/cmd/influx-tools/server"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"go.uber.org/zap"
)

type importer struct {
	MetaClient server.MetaClient
	store      *tsdb.Store
	db         string
	dataDir    string
	replace    bool

	rpi       *meta.RetentionPolicyInfo
	log       *zap.Logger
	sh        *shardWriter
	sfile     *tsdb.SeriesFile
	sw        *seriesWriter
	buildTsi  bool
	seriesBuf []byte
}

const seriesBatchSize = 1000

func newImporter(server server.Interface, db string, rp string, replace bool, buildTsi bool, log *zap.Logger) *importer {
	i := &importer{MetaClient: server.MetaClient(), db: db, dataDir: server.TSDBConfig().Dir, replace: replace, buildTsi: buildTsi, log: log}

	if replace {
		store := tsdb.NewStore(server.TSDBConfig().Dir)
		store.WithLogger(server.Logger())
		store.EngineOptions.Config = server.TSDBConfig()
		store.EngineOptions.EngineVersion = server.TSDBConfig().Engine
		store.EngineOptions.IndexVersion = server.TSDBConfig().Index
		store.EngineOptions.DatabaseFilter = func(database string) bool {
			return database == db
		}
		store.EngineOptions.RetentionPolicyFilter = func(_, r string) bool {
			return r == rp
		}
		store.EngineOptions.ShardFilter = func(_, _ string, _ uint64) bool {
			return false
		}
		i.store = store
	}

	if !buildTsi {
		i.seriesBuf = make([]byte, 0, 2048)
	}
	return i
}

func (i *importer) Open() error {
	if i.replace {
		return i.store.Open()
	}
	return nil
}

func (i *importer) Close() error {
	el := errlist.NewErrorList()
	if i.sh != nil {
		el.Add(i.CloseShardGroup())
	}
	if i.replace {
		el.Add(i.store.Close())
	}
	return el.Err()
}

func (i *importer) CreateDatabase(rp *meta.RetentionPolicySpec) error {
	var rpi *meta.RetentionPolicyInfo
	dbInfo := i.MetaClient.Database(i.db)
	if dbInfo == nil {
		return i.createDatabaseWithRetentionPolicy(rp)
	}

	rpi, err := i.MetaClient.RetentionPolicy(i.db, rp.Name)
	if err != nil {
		return err
	}

	updateRp := (rpi != nil) && i.replace &&
		((rp.Duration != nil && rpi.Duration != *rp.Duration) || (rpi.ShardGroupDuration != rp.ShardGroupDuration))
	if updateRp {
		err = i.updateRetentionPolicy(rpi, rp)
		if err != nil {
			return err
		}
	} else {
		_, err = i.MetaClient.CreateRetentionPolicy(i.db, rp, false)
		if err != nil {
			return err
		}
	}

	return i.createDatabaseWithRetentionPolicy(rp)
}

func (i *importer) updateRetentionPolicy(oldRpi *meta.RetentionPolicyInfo, newRp *meta.RetentionPolicySpec) error {
	for _, shardGroup := range oldRpi.ShardGroups {
		err := i.removeShardGroup(&shardGroup)
		if err != nil {
			return err
		}
	}

	rpu := &meta.RetentionPolicyUpdate{Name: &newRp.Name, Duration: newRp.Duration, ShardGroupDuration: &newRp.ShardGroupDuration}

	return i.MetaClient.UpdateRetentionPolicy(i.db, newRp.Name, rpu, false)
}

func (i *importer) createDatabaseWithRetentionPolicy(rp *meta.RetentionPolicySpec) error {
	var err error
	var dbInfo *meta.DatabaseInfo
	if len(rp.Name) == 0 {
		dbInfo, err = i.MetaClient.CreateDatabase(i.db)
	} else {
		dbInfo, err = i.MetaClient.CreateDatabaseWithRetentionPolicy(i.db, rp)
	}
	if err != nil {
		return err
	}
	i.rpi = dbInfo.RetentionPolicy(rp.Name)
	return nil
}

func (i *importer) StartShardGroup(start int64, end int64) error {
	sgi := i.rpi.ShardGroupByTimestamp(time.Unix(0, start))
	if sgi != nil {
		if i.replace {
			err := i.removeShardGroup(sgi)
			if err != nil {
				return err
			}
		} else {
			return fmt.Errorf("shard already exists for start time %v", start)
		}
	}

	sgi, err := i.MetaClient.CreateShardGroup(i.db, i.rpi.Name, time.Unix(0, start))
	if err != nil {
		return err
	}

	shardPath := filepath.Join(i.dataDir, i.db, i.rpi.Name)
	if err = os.MkdirAll(filepath.Join(shardPath, strconv.Itoa(int(sgi.ID))), 0777); err != nil {
		return err
	}

	i.sh = newShardWriter(sgi.ID, shardPath)

	i.startSeriesFile()
	return nil
}

func (i *importer) removeShardGroup(sgi *meta.ShardGroupInfo) error {
	err := i.MetaClient.DeleteShardGroup(i.db, i.rpi.Name, sgi.ID)
	if err != nil {
		return err
	}

	i.store.EngineOptions.ShardFilter = func(_, _ string, id uint64) bool {
		for _, shard := range sgi.Shards {
			if id == shard.ID {
				return true
			}
		}
		return false
	}

	for _, shard := range sgi.Shards {
		err = i.store.DeleteShard(shard.ID)
		if err != nil {
			return err
		}
	}

	return nil
}

func (i *importer) Write(key []byte, values tsm1.Values) error {
	if i.sh == nil {
		return errors.New("importer not currently writing a shard")
	}
	i.sh.Write(key, values)
	if i.sh.err != nil {
		return i.sh.err
	}
	return nil
}

func (i *importer) CloseShardGroup() error {
	el := errlist.NewErrorList()
	el.Add(i.closeSeriesFile())
	i.sh.Close()
	if i.sh.err != nil {
		el.Add(i.sh.err)
	}
	i.sh = nil
	return el.Err()
}

func (i *importer) startSeriesFile() error {
	dataPath := filepath.Join(i.dataDir, i.db)
	shardPath := filepath.Join(i.dataDir, i.db, i.rpi.Name)

	i.sfile = tsdb.NewSeriesFile(filepath.Join(dataPath, tsdb.SeriesFileDirectory))
	if err := i.sfile.Open(); err != nil {
		return err
	}

	var err error
	if i.buildTsi {
		i.sw, err = newTSI1SeriesWriter(i.sfile, i.db, dataPath, shardPath, int(i.sh.id))
	} else {
		i.sw, err = newInMemSeriesWriter(i.sfile, i.db, dataPath, shardPath, int(i.sh.id), i.seriesBuf)
	}

	if err != nil {
		return err
	}
	return nil
}

func (i *importer) AddSeries(seriesKey []byte) error {
	return i.sw.AddSeries(seriesKey)
}

func (i *importer) closeSeriesFile() error {
	return i.sw.Close()
}

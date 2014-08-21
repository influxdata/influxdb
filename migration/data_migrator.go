package migration

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/influxdb/influxdb/cluster"
	"github.com/influxdb/influxdb/configuration"
	"github.com/influxdb/influxdb/coordinator"
	"github.com/influxdb/influxdb/engine"
	"github.com/influxdb/influxdb/metastore"
	"github.com/influxdb/influxdb/parser"
	"github.com/influxdb/influxdb/protocol"

	log "code.google.com/p/log4go"
	// "github.com/BurntSushi/toml"
	"github.com/jmhodges/levigo"
)

// Used for migrating data from old versions of influx.
type DataMigrator struct {
	baseDbDir     string
	dbDir         string
	metaStore     *metastore.Store
	config        *configuration.Configuration
	clusterConfig *cluster.ClusterConfiguration
	coord         *coordinator.CoordinatorImpl
	pauseTime     time.Duration
}

const (
	MIGRATED_MARKER      = "MIGRATED"
	OLD_SHARD_DIR        = "shard_db"
	POINT_COUNT_TO_PAUSE = 10000
)

var (
	endStreamResponse = protocol.Response_END_STREAM
)

func NewDataMigrator(coord *coordinator.CoordinatorImpl, clusterConfig *cluster.ClusterConfiguration, config *configuration.Configuration, baseDbDir, newSubDir string, metaStore *metastore.Store, pauseTime time.Duration) *DataMigrator {
	return &DataMigrator{
		baseDbDir:     baseDbDir,
		dbDir:         filepath.Join(baseDbDir, OLD_SHARD_DIR),
		metaStore:     metaStore,
		config:        config,
		clusterConfig: clusterConfig,
		coord:         coord,
		pauseTime:     pauseTime,
	}
}

func (dm *DataMigrator) Migrate() {
	log.Info("Migrating from dir %s", dm.dbDir)
	infos, err := ioutil.ReadDir(dm.dbDir)
	if err != nil {
		log.Error("Error Migrating: ", err)
		return
	}
	names := make([]string, 0)
	for _, info := range infos {
		if info.IsDir() {
			names = append(names, info.Name())
		}
	}
	sort.Strings(names)
	//  go through in reverse order so most recently created shards will be migrated first
	for i := len(names) - 1; i >= 0; i-- {
		dm.migrateDir(names[i])
	}
}

func (dm *DataMigrator) migrateDir(name string) {
	migrateMarkerFile := filepath.Join(dm.shardDir(name), MIGRATED_MARKER)
	if _, err := os.Stat(migrateMarkerFile); err == nil {
		log.Info("Already migrated %s. Skipping", name)
		return
	}
	log.Info("Migrating %s", name)
	shard, err := dm.getShard(name)
	if err != nil {
		log.Error("Migration error getting shard: %s", err.Error())
		return
	}
	defer shard.Close()
	databases := dm.clusterConfig.GetDatabases()
	for _, database := range databases {
		err := dm.migrateDatabaseInShard(database.Name, shard)
		if err != nil {
			log.Error("Error migrating database %s: %s", database.Name, err.Error())
			return
		}
	}
	err = ioutil.WriteFile(migrateMarkerFile, []byte("done.\n"), 0644)
	if err != nil {
		log.Error("Problem writing migration marker for shard %s: %s", name, err.Error())
	}
}

func (dm *DataMigrator) migrateDatabaseInShard(database string, shard *LevelDbShard) error {
	log.Info("Migrating database %s for shard", database)
	seriesNames := shard.GetSeriesForDatabase(database)
	log.Info("Migrating %d series", len(seriesNames))

	admin := dm.clusterConfig.GetClusterAdmin(dm.clusterConfig.GetClusterAdmins()[0])
	pointCount := 0
	for _, series := range seriesNames {
		q, err := parser.ParseQuery(fmt.Sprintf("select * from \"%s\"", series))
		if err != nil {
			log.Error("Problem migrating series %s", series)
			continue
		}
		query := q[0]
		seriesChan := make(chan *protocol.Response)
		queryEngine := engine.NewPassthroughEngine(seriesChan, 2000)
		querySpec := parser.NewQuerySpec(admin, database, query)
		go func() {
			err := shard.Query(querySpec, queryEngine)
			if err != nil {
				log.Error("Error migrating %s", err.Error())
			}
			queryEngine.Close()
			seriesChan <- &protocol.Response{Type: &endStreamResponse}
		}()
		for {
			response := <-seriesChan
			if *response.Type == endStreamResponse {
				break
			}
			err := dm.coord.WriteSeriesData(admin, database, []*protocol.Series{response.Series})
			if err != nil {
				log.Error("Writing Series data: %s", err.Error())
			}
			pointCount += len(response.Series.Points)
			if pointCount > POINT_COUNT_TO_PAUSE {
				pointCount = 0
				time.Sleep(dm.pauseTime)
			}
		}
	}
	log.Info("Done migrating %s for shard", database)
	return nil
}

func (dm *DataMigrator) shardDir(name string) string {
	return filepath.Join(dm.baseDbDir, OLD_SHARD_DIR, name)
}

func (dm *DataMigrator) getShard(name string) (*LevelDbShard, error) {
	dbDir := dm.shardDir(name)
	cache := levigo.NewLRUCache(int(2000))
	opts := levigo.NewOptions()
	opts.SetCache(cache)
	opts.SetCreateIfMissing(true)
	opts.SetMaxOpenFiles(1000)
	ldb, err := levigo.Open(dbDir, opts)
	if err != nil {
		return nil, err
	}

	return NewLevelDbShard(ldb, dm.config.StoragePointBatchSize, dm.config.StorageWriteBatchSize)
}

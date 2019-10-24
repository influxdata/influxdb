package importer

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/influxdata/influxdb/cmd/influx_tools/internal/errlist"
	"github.com/influxdata/influxdb/cmd/influx_tools/internal/shard"
	"github.com/influxdata/influxdb/cmd/influx_tools/server"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"go.uber.org/zap"
)

type importer struct {
	MetaClient server.MetaClient
	db         string
	dataDir    string
	replace    bool

	rpi          *meta.RetentionPolicyInfo
	log          *zap.Logger
	skipShard    bool
	currentShard uint64
	sh           *shard.Writer
	sfile        *tsdb.SeriesFile
	sw           *seriesWriter
	buildTsi     bool
	seriesBuf    []byte
}

const seriesBatchSize = 1000

func newImporter(server server.Interface, db string, rp string, replace bool, buildTsi bool, log *zap.Logger) *importer {
	i := &importer{MetaClient: server.MetaClient(), db: db, dataDir: server.TSDBConfig().Dir, replace: replace, buildTsi: buildTsi, log: log, skipShard: false}

	if !buildTsi {
		i.seriesBuf = make([]byte, 0, 2048)
	}
	return i
}

func (i *importer) Close() error {
	el := errlist.NewErrorList()
	if i.sh != nil {
		el.Add(i.CloseShardGroup())
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

	nonmatchingRp := (rpi != nil) && ((rp.Duration != nil && rpi.Duration != *rp.Duration) ||
		(rp.ReplicaN != nil && rpi.ReplicaN != *rp.ReplicaN) ||
		(rpi.ShardGroupDuration != rp.ShardGroupDuration))
	if nonmatchingRp {
		return fmt.Errorf("retention policy %v already exists with different parameters", rp.Name)
	} else {
		if _, err := i.MetaClient.CreateRetentionPolicy(i.db, rp, false); err != nil {
			return err
		}
	}

	i.rpi, err = i.MetaClient.RetentionPolicy(i.db, rp.Name)
	return err
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
	existingSg, err := i.MetaClient.NodeShardGroupsByTimeRange(i.db, i.rpi.Name, time.Unix(0, start), time.Unix(0, end))
	if err != nil {
		return err
	}

	var sgi *meta.ShardGroupInfo
	var shardID uint64

	shardsPath := i.shardPath(i.rpi.Name)
	var shardPath string
	if len(existingSg) > 0 {
		sgi = &existingSg[0]
		if len(sgi.Shards) > 1 {
			return fmt.Errorf("multiple shards for the same owner %v and time range %v to %v", sgi.Shards[0].Owners, start, end)
		}

		shardID = sgi.Shards[0].ID

		shardPath = filepath.Join(shardsPath, strconv.Itoa(int(shardID)))
		_, err = os.Stat(shardPath)
		if err != nil {
			if !os.IsNotExist(err) {
				return err
			}
		} else {
			if i.replace {
				if err := os.RemoveAll(shardPath); err != nil {
					return err
				}
			} else {
				if i.log != nil {
					i.log.Error(fmt.Sprintf("shard %d already exists, skipping over new shard data", sgi.ID))
				}
				i.skipShard = true
				return nil
			}
		}
	} else {
		sgi, err = i.MetaClient.CreateShardGroup(i.db, i.rpi.Name, time.Unix(0, start))
		if err != nil {
			return err
		}
		shardID = sgi.Shards[0].ID
	}

	shardPath = filepath.Join(shardsPath, strconv.Itoa(int(shardID)))
	if err = os.MkdirAll(shardPath, 0777); err != nil {
		return err
	}

	i.skipShard = false
	i.sh = shard.NewWriter(shardID, shardsPath)
	i.currentShard = shardID

	i.startSeriesFile()
	return nil
}

func (i *importer) shardPath(rp string) string {
	return filepath.Join(i.dataDir, i.db, rp)
}

func (i *importer) removeShardGroup(rp string, shardID uint64) error {
	shardPath := i.shardPath(rp)
	err := os.RemoveAll(filepath.Join(shardPath, strconv.Itoa(int(shardID))))
	return err
}

func (i *importer) Write(key []byte, values tsm1.Values) error {
	if i.skipShard {
		return nil
	}
	if i.sh == nil {
		return errors.New("importer not currently writing a shard")
	}
	i.sh.Write(key, values)
	if i.sh.Err() != nil {
		el := errlist.NewErrorList()
		el.Add(i.sh.Err())
		el.Add(i.CloseShardGroup())
		el.Add(i.removeShardGroup(i.rpi.Name, i.currentShard))
		i.sh = nil
		i.currentShard = 0
		return el.Err()
	}
	return nil
}

func (i *importer) CloseShardGroup() error {
	if i.skipShard {
		i.skipShard = false
		return nil
	}
	el := errlist.NewErrorList()
	el.Add(i.closeSeriesFile())
	i.sh.Close()
	if i.sh.Err() != nil {
		el.Add(i.sh.Err())
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
		i.sw, err = newTSI1SeriesWriter(i.sfile, i.db, dataPath, shardPath, int(i.sh.ShardID()))
	} else {
		i.sw, err = newInMemSeriesWriter(i.sfile, i.db, dataPath, shardPath, int(i.sh.ShardID()), i.seriesBuf)
	}

	if err != nil {
		return err
	}
	return nil
}

func (i *importer) AddSeries(seriesKey []byte) error {
	if i.skipShard {
		return nil
	}
	return i.sw.AddSeries(seriesKey)
}

func (i *importer) closeSeriesFile() error {
	return i.sw.Close()
}

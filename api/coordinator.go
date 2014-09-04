package api

import (
	"github.com/influxdb/influxdb/cluster"
	cmn "github.com/influxdb/influxdb/common"
	"github.com/influxdb/influxdb/engine"
	"github.com/influxdb/influxdb/protocol"
)

// The following are the api that is accessed by any api

type Coordinator interface {
	// This is only used in the force compaction http endpoint
	ForceCompaction(cmn.User) error

	// Data related api
	RunQuery(cmn.User, string, string, engine.Processor) error
	WriteSeriesData(cmn.User, string, []*protocol.Series) error

	// Administration related api
	CreateDatabase(cmn.User, string) error
	ListDatabases(cmn.User) ([]*cluster.Database, error)
	DropDatabase(cmn.User, string) error
}

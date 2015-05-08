package data

import (
	"github.com/influxdb/influxdb"
	"github.com/influxdb/influxdb/influxql"
)

type SeriesWriter interface {
	Write(database, retentionPolicy string, points []influxdb.Point) error
}

type Querier interface {
	Query(query *influxql.Query, databaseName string, user *influxdb.User, chunkeSize int) (chan *influxdb.Result, error)
}

type Authenticater interface {
	Authenticate(string, string) (*influxdb.User, error)
}

type Node struct{}

func (n Node) Write(database, retentionPolicy string, points []influxdb.Point) error {

	return nil
}

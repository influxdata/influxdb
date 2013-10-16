package coordinator

import (
	"datastore"
	"parser"
	"protocol"
)

type CoordinatorImpl struct {
	clusterConfiguration *ClusterConfiguration
	raftServer           *RaftServer
	datastore            datastore.Datastore
}

func NewCoordinatorImpl(datastore datastore.Datastore, raftServer *RaftServer, clusterConfiguration *ClusterConfiguration) Coordinator {
	return &CoordinatorImpl{clusterConfiguration, raftServer, datastore}
}

func (self *CoordinatorImpl) DistributeQuery(db string, query *parser.Query, yield func(*protocol.Series) error) error {
	return self.datastore.ExecuteQuery(db, query, yield)
}

func (self *CoordinatorImpl) WriteSeriesData(db string, series *protocol.Series) error {
	return self.datastore.WriteSeriesData(db, series)
}

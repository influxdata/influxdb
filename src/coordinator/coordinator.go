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

func (self *CoordinatorImpl) DistributeQuery(query *parser.Query, yield func(*protocol.Series) error) error {
	return self.datastore.ExecuteQuery(0, query, yield)
}

func (self *CoordinatorImpl) WriteSeriesData(series *protocol.Series) error {
	return self.datastore.WriteSeriesData(0, series)
}

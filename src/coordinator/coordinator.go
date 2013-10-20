package coordinator

import (
	"common"
	"datastore"
	"parser"
	"protocol"
	"sync"
)

type CoordinatorImpl struct {
	clusterConfiguration  *ClusterConfiguration
	raftServer            *RaftServer
	datastore             datastore.Datastore
	currentSequenceNumber uint32
	sequenceNumberLock    sync.Mutex
}

func NewCoordinatorImpl(datastore datastore.Datastore, raftServer *RaftServer, clusterConfiguration *ClusterConfiguration) Coordinator {
	return &CoordinatorImpl{clusterConfiguration: clusterConfiguration, raftServer: raftServer, datastore: datastore}
}

func (self *CoordinatorImpl) DistributeQuery(db string, query *parser.Query, yield func(*protocol.Series) error) error {
	return self.datastore.ExecuteQuery(db, query, yield)
}

func (self *CoordinatorImpl) WriteSeriesData(db string, series *protocol.Series) error {
	now := common.CurrentTime()
	for _, p := range series.Points {
		if p.Timestamp == nil {
			p.Timestamp = &now
			self.sequenceNumberLock.Lock()
			self.currentSequenceNumber += 1
			n := self.currentSequenceNumber
			self.sequenceNumberLock.Unlock()
			p.SequenceNumber = &n
		} else if p.SequenceNumber == nil {
			self.sequenceNumberLock.Lock()
			self.currentSequenceNumber += 1
			n := self.currentSequenceNumber
			self.sequenceNumberLock.Unlock()
			p.SequenceNumber = &n
		}
	}
	return self.datastore.WriteSeriesData(db, series)
}

func (self *CoordinatorImpl) CreateDatabase(db, initialApiKey, requestingApiKey string) error {
	err := self.raftServer.CreateDatabase(db)
	if err != nil {
		return err
	}
	err = self.raftServer.AddReadApiKey(db, initialApiKey)
	if err != nil {
		return err
	}
	err = self.raftServer.AddWriteApiKey(db, initialApiKey)
	return err
}

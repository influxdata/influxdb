package coordinator

import (
	"bytes"
	log "code.google.com/p/log4go"
	"common"
	"datastore"
	"encoding/binary"
	"errors"
	"net"
	"parser"
	"protocol"
)

type ProtobufRequestHandler struct {
	db            datastore.Datastore
	coordinator   Coordinator
	clusterConfig *ClusterConfiguration
	writeOk       protocol.Response_Type
}

var replayReplicationEnd = protocol.Response_REPLICATION_REPLAY_END
var responseReplicationReplay = protocol.Response_REPLICATION_REPLAY

func NewProtobufRequestHandler(db datastore.Datastore, coordinator Coordinator, clusterConfig *ClusterConfiguration) *ProtobufRequestHandler {
	return &ProtobufRequestHandler{db: db, coordinator: coordinator, writeOk: protocol.Response_WRITE_OK, clusterConfig: clusterConfig}
}

func (self *ProtobufRequestHandler) HandleRequest(request *protocol.Request, conn net.Conn) error {
	if *request.Type == protocol.Request_PROXY_WRITE {
		response := &protocol.Response{RequestId: request.Id, Type: &self.writeOk}

		location := common.RingLocation(request.Database, request.Series.Name, request.Series.Points[0].Timestamp)
		ownerId := self.clusterConfig.GetOwnerIdByLocation(&location)
		request.OriginatingServerId = &self.clusterConfig.localServerId
		// TODO: make request logging and datastore write atomic
		replicationFactor := self.clusterConfig.GetReplicationFactor(request.Database)
		err := self.db.LogRequestAndAssignSequenceNumber(request, &replicationFactor, ownerId)
		if err != nil {
			return err
		}
		err = self.db.WriteSeriesData(*request.Database, request.Series)
		if err != nil {
			return err
		}
		err = self.WriteResponse(conn, response)
		// TODO: add quorum writes?
		self.coordinator.ReplicateWrite(request)
		return err
	} else if *request.Type == protocol.Request_PROXY_DELETE {
		response := &protocol.Response{RequestId: request.Id, Type: &self.writeOk}

		request.OriginatingServerId = &self.clusterConfig.localServerId
		// TODO: make request logging and datastore write atomic
		replicationFactor := self.clusterConfig.GetReplicationFactor(request.Database)
		err := self.db.LogRequestAndAssignSequenceNumber(request, &replicationFactor, request.OwnerServerId)
		if err != nil {
			return err
		}
		query, _ := parser.ParseQuery(*request.Query)
		err = self.db.DeleteSeriesData(*request.Database, query[0].DeleteQuery)
		if err != nil {
			return err
		}
		err = self.WriteResponse(conn, response)
		// TODO: add quorum writes?
		self.coordinator.ReplicateDelete(request)
		return err
	} else if *request.Type == protocol.Request_REPLICATION_WRITE {
		replicationFactor := self.clusterConfig.GetReplicationFactor(request.Database)
		// TODO: make request logging and datastore write atomic
		err := self.db.LogRequestAndAssignSequenceNumber(request, &replicationFactor, request.OwnerServerId)
		if err != nil {
			switch err := err.(type) {
			case datastore.SequenceMissingRequestsError:
				go self.coordinator.ReplayReplication(request, &replicationFactor, request.OwnerServerId, &err.LastKnownRequestSequence)
				return nil
			default:
				return err
			}
		}
		self.db.WriteSeriesData(*request.Database, request.Series)
		return nil
	} else if *request.Type == protocol.Request_REPLICATION_DELETE {
		replicationFactor := self.clusterConfig.GetReplicationFactor(request.Database)
		// TODO: make request logging and datastore write atomic
		err := self.db.LogRequestAndAssignSequenceNumber(request, &replicationFactor, request.OwnerServerId)
		if err != nil {
			switch err := err.(type) {
			case datastore.SequenceMissingRequestsError:
				go self.coordinator.ReplayReplication(request, &replicationFactor, request.OwnerServerId, &err.LastKnownRequestSequence)
				return nil
			default:
				return err
			}
		}
		query, _ := parser.ParseQuery(*request.Query)
		return self.db.DeleteSeriesData(*request.Database, query[0].DeleteQuery)
	} else if *request.Type == protocol.Request_QUERY {
		go self.handleQuery(request, conn)
	} else if *request.Type == protocol.Request_LIST_SERIES {
		go self.handleListSeries(request, conn)
	} else if *request.Type == protocol.Request_REPLICATION_REPLAY {
		self.handleReplay(request, conn)
	} else {
		log.Error("unknown request type: %v", request)
		return errors.New("Unknown request type")
	}
	return nil
}

func (self *ProtobufRequestHandler) handleReplay(request *protocol.Request, conn net.Conn) {
	sendRequest := func(loggedRequestData *[]byte) error {
		var response *protocol.Response
		if loggedRequestData != nil {
			loggedRequest, err := protocol.DecodeRequest(bytes.NewBuffer(*loggedRequestData))
			if err != nil {
				return err
			}
			response = &protocol.Response{Type: &responseReplicationReplay, Request: loggedRequest, RequestId: request.Id}
		} else {
			response = &protocol.Response{Type: &replayReplicationEnd, RequestId: request.Id}
		}
		return self.WriteResponse(conn, response)
	}
	replicationFactor8 := uint8(*request.ReplicationFactor)
	err := self.db.ReplayRequestsFromSequenceNumber(
		request.ClusterVersion,
		request.OriginatingServerId,
		request.OwnerServerId,
		&replicationFactor8,
		request.LastKnownSequenceNumber,
		sendRequest)
	if err != nil {
		log.Error("REPLAY ERROR: %s", err)
	}
}

func (self *ProtobufRequestHandler) handleQuery(request *protocol.Request, conn net.Conn) {
	nextPointMap := make(map[string]*protocol.Point)
	assignNextPointTimesAndSend := func(series *protocol.Series) error {
		pointCount := len(series.Points)
		if pointCount <= 1 {
			if nextPoint := nextPointMap[*series.Name]; nextPoint != nil {
				series.Points = append(series.Points, nextPoint)
			}
			response := &protocol.Response{Type: &queryResponse, Series: series, RequestId: request.Id}

			self.WriteResponse(conn, response)
			return nil
		}
		oldNextPoint := nextPointMap[*series.Name]
		nextPoint := series.Points[pointCount-1]
		series.Points[pointCount-1] = nil
		if oldNextPoint != nil {
			copy(series.Points[1:], series.Points[0:])
			series.Points[0] = oldNextPoint
		} else {
			series.Points = series.Points[:len(series.Points)-1]
		}

		response := &protocol.Response{Series: series, Type: &queryResponse, RequestId: request.Id}
		if nextPoint != nil {
			response.NextPointTime = nextPoint.Timestamp
			nextPointMap[*series.Name] = nextPoint
		}
		err := self.WriteResponse(conn, response)
		return err
	}
	// the query should always parse correctly since it was parsed at the originating server.
	query, _ := parser.ParseSelectQuery(*request.Query)
	user := self.clusterConfig.GetDbUser(*request.Database, *request.UserName)

	var ringFilter func(database, series *string, time *int64) bool
	if request.RingLocationsToQuery != nil {
		ringFilter = self.clusterConfig.GetRingFilterFunction(*request.Database, *request.RingLocationsToQuery)
	}
	self.db.ExecuteQuery(user, *request.Database, query, assignNextPointTimesAndSend, ringFilter)

	response := &protocol.Response{Type: &endStreamResponse, RequestId: request.Id}
	self.WriteResponse(conn, response)
}

func (self *ProtobufRequestHandler) handleListSeries(request *protocol.Request, conn net.Conn) {
	dbs := []string{}
	self.db.GetSeriesForDatabase(*request.Database, func(db string) error {
		dbs = append(dbs, db)
		return nil
	})

	seriesArray := seriesFromListSeries(dbs)
	for _, series := range seriesArray {
		response := &protocol.Response{RequestId: request.Id, Type: &listSeriesResponse, Series: series}
		self.WriteResponse(conn, response)
	}
	response := &protocol.Response{RequestId: request.Id, Type: &endStreamResponse}
	self.WriteResponse(conn, response)
}

func (self *ProtobufRequestHandler) WriteResponse(conn net.Conn, response *protocol.Response) error {
	data, err := response.Encode()
	if err != nil {
		log.Error("error encoding response: %s", err)
		return err
	}
	err = binary.Write(conn, binary.LittleEndian, uint32(len(data)))
	if err != nil {
		log.Error("error writing response length: %s", err)
		return err
	}
	_, err = conn.Write(data)
	return err
}

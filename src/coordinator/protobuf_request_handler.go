package coordinator

import (
	"datastore"
	"encoding/binary"
	"errors"
	"log"
	"net"
	"protocol"
)

type ProtobufRequestHandler struct {
	db               datastore.Datastore
	clusterConsensus ClusterConsensus
	writeOk          protocol.Response_Type
}

func NewProtobufRequestHandler(db datastore.Datastore, clusterConsensus ClusterConsensus) *ProtobufRequestHandler {
	return &ProtobufRequestHandler{db: db, clusterConsensus: clusterConsensus, writeOk: protocol.Response_WRITE_OK}
}

func (self *ProtobufRequestHandler) HandleRequest(request *protocol.Request, conn net.Conn) error {
	if *request.Type == protocol.Request_PROXY_WRITE {
		response := &protocol.Response{RequestId: request.Id, Type: &self.writeOk}

		self.db.(*datastore.LevelDbDatastore).LogRequestAndAssignId(request)
		err := self.db.WriteSeriesData(*request.Database, request.Series)
		if err != nil {
			return err
		}
		err = self.WriteResponse(conn, response)
		// TODO: add quorum writes?
		self.clusterConsensus.ReplicateWrite(request)
		return err
	} else if *request.Type == protocol.Request_PROXY_DELETE {

	} else if *request.Type == protocol.Request_REPLICATION_WRITE {
		// TODO: check the request id and server and make sure it's next (+1 from last one from the server).
		//       If so, write. If not, request replay.
		// TODO: log replication writes so the can be retrieved from other servers
		self.db.WriteSeriesData(*request.Database, request.Series)
		return nil
	} else if *request.Type == protocol.Request_REPLICATION_DELETE {

	} else if *request.Type == protocol.Request_QUERY {
	} else {
		log.Println("unknown request type: ", request)
		return errors.New("Unknown request type")
	}
	return nil
}

func (self *ProtobufRequestHandler) WriteResponse(conn net.Conn, response *protocol.Response) error {
	data, err := response.Encode()
	if err != nil {
		return err
	}
	err = binary.Write(conn, binary.LittleEndian, uint32(len(data)))
	if err != nil {
		return err
	}
	_, err = conn.Write(data)
	return err
}

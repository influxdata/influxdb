package coordinator

import (
	"datastore"
	"encoding/binary"
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
		err := self.db.WriteSeriesData(*request.Database, request.Series)
		if err != nil {
			return err
		}
		response := &protocol.Response{RequestId: request.Id, Type: &self.writeOk}
		err = self.WriteResponse(conn, response)
		// TODO: add quorum writes?
		self.clusterConsensus.ReplicateWrite(request)
		return err
	} else if *request.Type == protocol.Request_PROXY_DELETE {

	} else if *request.Type == protocol.Request_REPLICATION_WRITE {

	} else if *request.Type == protocol.Request_REPLICATION_DELETE {

	} else if *request.Type == protocol.Request_QUERY {
	} else {
		log.Println("unknown request type: ", request)
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

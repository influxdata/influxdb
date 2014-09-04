package coordinator

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"net"

	log "code.google.com/p/log4go"
	"github.com/influxdb/influxdb/cluster"
	"github.com/influxdb/influxdb/common"
	"github.com/influxdb/influxdb/parser"
	"github.com/influxdb/influxdb/protocol"
)

type ProtobufRequestHandler struct {
	coordinator   *Coordinator
	clusterConfig *cluster.ClusterConfiguration
}

func NewProtobufRequestHandler(coordinator *Coordinator, clusterConfig *cluster.ClusterConfiguration) *ProtobufRequestHandler {
	return &ProtobufRequestHandler{
		coordinator:   coordinator,
		clusterConfig: clusterConfig,
	}
}

func (self *ProtobufRequestHandler) HandleRequest(request *protocol.Request, conn net.Conn) error {
	switch *request.Type {
	case protocol.Request_WRITE:
		go self.handleWrites(request, conn)
	case protocol.Request_QUERY:
		go self.handleQuery(request, conn)
	case protocol.Request_HEARTBEAT:
		response := &protocol.Response{
			RequestId: request.Id,
			Type:      protocol.Response_HEARTBEAT.Enum(),
		}
		return self.WriteResponse(conn, response)
	default:
		log.Error("unknown request type: %v", request)
		return errors.New("Unknown request type")
	}
	return nil
}

func (self *ProtobufRequestHandler) handleWrites(request *protocol.Request, conn net.Conn) {
	shard := self.clusterConfig.GetLocalShardById(*request.ShardId)
	log.Debug("HANDLE: (%d):%d:%v", self.clusterConfig.LocalServer.Id, request.GetId(), shard)
	err := shard.WriteLocalOnly(request)
	var response *protocol.Response
	if err != nil {
		log.Error("ProtobufRequestHandler: error writing local shard: %s", err)
		response = &protocol.Response{
			RequestId:    request.Id,
			Type:         protocol.Response_ERROR.Enum(),
			ErrorMessage: protocol.String(err.Error()),
		}
	} else {
		response = &protocol.Response{
			RequestId: request.Id,
			Type:      protocol.Response_END_STREAM.Enum(),
		}
	}
	if err := self.WriteResponse(conn, response); err != nil {
		log.Error("ProtobufRequestHandler: error writing local shard: %s", err)
	}
}

func (self *ProtobufRequestHandler) handleQuery(request *protocol.Request, conn net.Conn) {
	// the query should always parse correctly since it was parsed at the originating server.
	queries, err := parser.ParseQuery(*request.Query)
	if err != nil || len(queries) < 1 {
		log.Error("Error parsing query: ", err)
		errorMsg := fmt.Sprintf("Cannot find user %s", *request.UserName)
		response := &protocol.Response{
			Type:         protocol.Response_ERROR.Enum(),
			ErrorMessage: &errorMsg,
			RequestId:    request.Id,
		}
		self.WriteResponse(conn, response)
		return
	}
	query := queries[0]
	var user common.User
	if *request.IsDbUser {
		user = self.clusterConfig.GetDbUser(*request.Database, *request.UserName)
	} else {
		user = self.clusterConfig.GetClusterAdmin(*request.UserName)
	}

	if user == nil {
		errorMsg := fmt.Sprintf("Cannot find user %s", *request.UserName)
		response := &protocol.Response{
			Type:         protocol.Response_ERROR.Enum(),
			ErrorMessage: &errorMsg,
			RequestId:    request.Id,
		}
		self.WriteResponse(conn, response)
		return
	}

	shard := self.clusterConfig.GetLocalShardById(*request.ShardId)

	querySpec := parser.NewQuerySpec(user, *request.Database, query)

	responseChan := make(chan *protocol.Response)
	if querySpec.IsDestructiveQuery() {
		go shard.HandleDestructiveQuery(querySpec, request, responseChan, true)
	} else {
		go shard.Query(querySpec, responseChan)
	}
	for {
		response := <-responseChan
		response.RequestId = request.Id
		self.WriteResponse(conn, response)

		switch rt := response.GetType(); rt {
		case protocol.Response_END_STREAM,
			protocol.Response_ERROR:
			return
		case protocol.Response_QUERY:
			continue
		default:
			panic(fmt.Errorf("Unexpected response type: %s", rt))
		}
	}
}

func (self *ProtobufRequestHandler) WriteResponse(conn net.Conn, response *protocol.Response) error {
	if response.Size() >= MAX_RESPONSE_SIZE {
		f, s := splitResponse(response)
		err := self.WriteResponse(conn, f)
		if err != nil {
			return err
		}
		return self.WriteResponse(conn, s)
	}

	data, err := response.Encode()
	if err != nil {
		log.Error("error encoding response: %s", err)
		return err
	}

	buff := bytes.NewBuffer(make([]byte, 0, len(data)+8))
	binary.Write(buff, binary.LittleEndian, uint32(len(data)))
	_, err = conn.Write(append(buff.Bytes(), data...))
	if err != nil {
		log.Error("error writing response: %s", err)
		return err
	}
	return nil
}

func splitResponse(response *protocol.Response) (f, s *protocol.Response) {
	f = &protocol.Response{}
	s = &protocol.Response{}
	*f = *response
	*s = *response

	if l := len(response.MultiSeries); l > 1 {
		f.MultiSeries = f.MultiSeries[:l/2]
		s.MultiSeries = s.MultiSeries[l/2:]
		return
	}

	l := len(response.MultiSeries[0].Points)
	f.MultiSeries[0].Points = f.MultiSeries[0].Points[:l/2]
	s.MultiSeries[0].Points = s.MultiSeries[0].Points[l/2:]
	return
}

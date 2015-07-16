package meta

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/influxdb/influxdb/meta/internal"
)

// RPC handles request/response style messaging between cluster nodes
type RPC struct {
	Logger *log.Logger

	store interface {
		cachedData() *Data
		IsLeader() bool
		Leader() string
		Peers() []string
		AddPeer(host string) error
		CreateNode(host string) (*NodeInfo, error)
		NodeByHost(host string) (*NodeInfo, error)
	}
}

type JoinResult struct {
	RaftEnabled bool
	Peers       []string
	NodeID      uint64
}

type Reply interface {
	GetHeader() *internal.ResponseHeader
}

// proxyLeader proxies the connection to the current raft leader
func (r *RPC) proxyLeader(conn *net.TCPConn) {
	if r.store.Leader() == "" {
		r.sendError(conn, "no leader")
		return
	}

	leaderConn, err := net.DialTimeout("tcp", r.store.Leader(), 10*time.Second)
	if err != nil {
		r.sendError(conn, fmt.Sprintf("dial leader: %v", err))
		return
	}
	defer leaderConn.Close()

	leaderConn.Write([]byte{MuxRPCHeader})
	if err := proxy(leaderConn.(*net.TCPConn), conn); err != nil {
		r.sendError(conn, fmt.Sprintf("leader proxy error: %v", err))
	}
}

// handleRPCConn reads a command from the connection and executes it.
func (r *RPC) handleRPCConn(conn net.Conn) {
	defer conn.Close()
	// RPC connections should execute on the leader.  If we are not the leader,
	// proxy the connection to the leader so that clients an connect to any node
	// in the cluster.
	r.Logger.Printf("rpc connection from: %v", conn.RemoteAddr())
	if !r.store.IsLeader() {
		r.proxyLeader(conn.(*net.TCPConn))
		return
	}

	// Read and execute request.
	typ, resp, err := func() (internal.RPCType, proto.Message, error) {
		// Read request size.
		var sz uint64
		if err := binary.Read(conn, binary.BigEndian, &sz); err != nil {
			return internal.RPCType_Error, nil, fmt.Errorf("read size: %s", err)
		}

		// Read request.
		buf := make([]byte, sz)
		if _, err := io.ReadFull(conn, buf); err != nil {
			return internal.RPCType_Error, nil, fmt.Errorf("read request: %s", err)
		}

		// Determine the RPC type
		rpcType := internal.RPCType(btou64(buf[0:8]))
		buf = buf[8:]

		r.Logger.Printf("recv %v request from: %v", rpcType, conn.RemoteAddr())
		switch rpcType {
		case internal.RPCType_FetchData:
			resp, err := r.handleFetchData()
			return rpcType, resp, err
		case internal.RPCType_Join:
			var req internal.JoinRequest
			if err := proto.Unmarshal(buf, &req); err != nil {
				r.Logger.Printf("join request unmarshal: %v", err)
			}
			resp, err := r.handleJoinRequest(&req)
			return rpcType, resp, err
		default:
			return internal.RPCType_Error, nil, fmt.Errorf("unknown rpc type:%v", rpcType)
		}
	}()

	// Handle unexpected RPC errors
	if err != nil {
		resp = &internal.ErrorResponse{
			Header: &internal.ResponseHeader{
				OK: proto.Bool(false),
			},
		}
		typ = internal.RPCType_Error
	}

	// Set the status header and error message
	if reply, ok := resp.(Reply); ok {
		reply.GetHeader().OK = proto.Bool(err == nil)
		if err != nil {
			reply.GetHeader().Error = proto.String(err.Error())
		}
	}

	r.sendResponse(conn, typ, resp)
}

func (r *RPC) sendResponse(conn net.Conn, typ internal.RPCType, resp proto.Message) {
	// Marshal the response back to a protobuf
	buf, err := proto.Marshal(resp)
	if err != nil {
		r.Logger.Printf("unable to marshal response: %v", err)
		return
	}

	// Encode response back to connection.
	if _, err := conn.Write(r.pack(typ, buf)); err != nil {
		r.Logger.Printf("unable to write rpc response: %s", err)
	}
}

func (r *RPC) sendError(conn net.Conn, msg string) {
	resp := &internal.ErrorResponse{
		Header: &internal.ResponseHeader{
			OK:    proto.Bool(false),
			Error: proto.String(msg),
		},
	}

	r.sendResponse(conn, internal.RPCType_Error, resp)
}

// handleFetchData handles a request for the current nodes meta data
func (r *RPC) handleFetchData() (*internal.FetchDataResponse, error) {
	data := r.store.cachedData()
	b, err := data.MarshalBinary()

	return &internal.FetchDataResponse{
		Header: &internal.ResponseHeader{
			OK: proto.Bool(true),
		},
		Data: b}, err
}

// handleJoinRequest handles a request to join the cluster
func (r *RPC) handleJoinRequest(req *internal.JoinRequest) (*internal.JoinResponse, error) {
	r.Logger.Printf("recv join request from: %v", *req.Addr)

	node, err := func() (*NodeInfo, error) {
		// attempt to create the node
		node, err := r.store.CreateNode(*req.Addr)
		// if it exists, return the exting node
		if err == ErrNodeExists {
			return r.store.NodeByHost(*req.Addr)
		} else if err != nil {
			return nil, fmt.Errorf("create node: %v", err)
		}

		// FIXME: jwilder: adding raft nodes is tricky since going
		// from 1 node (leader) to two kills the cluster because
		// quorum is lost after adding the second node.  For now,
		// can only add non-raft enabled nodes

		// If we have less than 3 nodes, add them as raft peers
		// if len(r.store.Peers()) < MaxRaftNodes {
		// 	if err = r.store.AddPeer(*req.Addr); err != nil {
		// 		return node, fmt.Errorf("add peer: %v", err)
		// 	}
		// }
		return node, err
	}()

	nodeID := uint64(0)
	if node != nil {
		nodeID = node.ID
	}

	if err != nil {
		return nil, err
	}

	return &internal.JoinResponse{
		Header: &internal.ResponseHeader{
			OK: proto.Bool(true),
		},
		//EnableRaft: proto.Bool(contains(r.store.Peers(), *req.Addr)),
		EnableRaft: proto.Bool(false),
		Peers:      r.store.Peers(),
		NodeID:     proto.Uint64(nodeID),
	}, err

}

// pack returns a TLV style byte slice encoding the size of the payload, the RPC type
// and the RPC data
func (r *RPC) pack(typ internal.RPCType, b []byte) []byte {
	buf := u64tob(uint64(len(b)) + 8)
	buf = append(buf, u64tob(uint64(typ))...)
	buf = append(buf, b...)
	return buf
}

// fetchMetaData returns the latest copy of the meta store data from the current
// leader.
func (r *RPC) fetchMetaData() (*Data, error) {
	// Retrieve the current known leader.
	leader := r.store.Leader()
	if leader == "" {
		return nil, errors.New("no leader")
	}

	resp, err := r.call(leader, &internal.FetchDataRequest{})
	if err != nil {
		return nil, err
	}

	switch t := resp.(type) {
	case *internal.FetchDataResponse:
		ms := &Data{}
		if err := ms.UnmarshalBinary(t.GetData()); err != nil {
			return nil, err
		}
		return ms, nil
	case *internal.ErrorResponse:
		return nil, fmt.Errorf("rpc failed: %s", t.GetHeader().GetError())
	default:
		return nil, fmt.Errorf("rpc failed: unknown response type: %v", t.String())
	}
}

// join attempts to join a cluster at remoteAddr using localAddr as the current
// node's cluster address
func (r *RPC) join(localAddr, remoteAddr string) (*JoinResult, error) {
	req := &internal.JoinRequest{
		Addr: proto.String(localAddr),
	}

	resp, err := r.call(remoteAddr, req)
	if err != nil {
		return nil, err
	}

	switch t := resp.(type) {
	case *internal.JoinResponse:
		return &JoinResult{
			RaftEnabled: t.GetEnableRaft(),
			Peers:       t.GetPeers(),
			NodeID:      t.GetNodeID(),
		}, nil
	case *internal.ErrorResponse:
		return nil, fmt.Errorf("rpc failed: %s", t.GetHeader().GetError())
	default:
		return nil, fmt.Errorf("rpc failed: unknown response type: %v", t.String())
	}
}

// call sends an encoded request to the remote leader and returns
// an encoded response value.
func (r *RPC) call(dest string, req proto.Message) (proto.Message, error) {

	// Determine type of request
	var rpcType internal.RPCType
	switch t := req.(type) {
	case *internal.JoinRequest:
		rpcType = internal.RPCType_Join
	case *internal.FetchDataRequest:
		rpcType = internal.RPCType_FetchData
	default:
		return nil, fmt.Errorf("unknown rpc request type: %v", t)
	}

	// Create a connection to the leader.
	conn, err := net.DialTimeout("tcp", dest, 10*time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// Write a marker byte for rpc messages.
	_, err = conn.Write([]byte{MuxRPCHeader})
	if err != nil {
		return nil, err
	}

	b, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}

	// Write request size & bytes.
	if _, err := conn.Write(r.pack(rpcType, b)); err != nil {
		return nil, fmt.Errorf("write rpc: %s", err)
	}

	data, err := ioutil.ReadAll(conn)
	if err != nil {
		return nil, fmt.Errorf("read rpc: %v", err)
	}

	sz := btou64(data[0:8])
	if len(data[8:]) != int(sz) {
		return nil, fmt.Errorf("rpc failed: short read: got %v, exp %v", len(data[8:]), sz)
	}

	// See what response type we got back, could get a general error response
	rpcType = internal.RPCType(btou64(data[8:16]))
	data = data[16:]

	var resp proto.Message
	switch rpcType {
	case internal.RPCType_Join:
		resp = &internal.JoinResponse{}
	case internal.RPCType_FetchData:
		resp = &internal.FetchDataResponse{}
	case internal.RPCType_Error:
		resp = &internal.ErrorResponse{}
	default:
		return nil, fmt.Errorf("unknown rpc response type: %v", rpcType)
	}

	if err := proto.Unmarshal(data, resp); err != nil {
		return nil, err
	}

	if reply, ok := resp.(Reply); ok {
		if !reply.GetHeader().GetOK() {
			return nil, fmt.Errorf("rpc failed: %s", reply.GetHeader().GetError())
		}
	}

	return resp, nil
}

func u64tob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func btou64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

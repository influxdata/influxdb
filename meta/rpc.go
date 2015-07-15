package meta

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
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
		Leader() string
		Peers() []string
		AddPeer(host string) error
	}
}

type JoinResult struct {
	RaftEnabled bool
	Peers       []string
}

type Reply interface {
	GetHeader() *internal.ResponseHeader
}

// handleRPCConn reads a command from the connection and executes it.
func (r *RPC) handleRPCConn(conn net.Conn) {
	// Read and execute request.
	resp, err := func() (proto.Message, error) {
		// Read request size.
		var sz uint64
		if err := binary.Read(conn, binary.BigEndian, &sz); err != nil {
			return nil, fmt.Errorf("read size: %s", err)
		}

		// Read request.
		buf := make([]byte, sz)
		if _, err := io.ReadFull(conn, buf); err != nil {
			return nil, fmt.Errorf("read request: %s", err)
		}

		rpcType := internal.RPCType(btou64(buf[0:8]))
		buf = buf[8:]

		r.Logger.Printf("recv %v request from: %v", rpcType, conn.RemoteAddr())
		switch rpcType {
		case internal.RPCType_FetchData:
			return r.handleFetchData()
		case internal.RPCType_Join:
			var req internal.JoinRequest
			if err := proto.Unmarshal(buf, &req); err != nil {
				r.Logger.Printf("join request unmarshal: %v", err)
			}
			return r.handleJoinRequest(&req)
		default:
			return nil, fmt.Errorf("unknown rpc type:%v", rpcType)
		}
	}()

	if reply, ok := resp.(Reply); ok {
		reply.GetHeader().OK = proto.Bool(err == nil)
		if err != nil {
			reply.GetHeader().Error = proto.String(err.Error())
		}
	}

	buf, err := proto.Marshal(resp)
	if err != nil {
		r.Logger.Printf("unable to marshal response: %v", err)
	}

	// Encode response back to connection.
	if err = binary.Write(conn, binary.BigEndian, uint64(len(buf))); err != nil {
		r.Logger.Printf("unable to write rpc response size: %s", err)
	} else if _, err = conn.Write(buf); err != nil {
		r.Logger.Printf("unable to write rpc response: %s", err)
	}
	conn.Close()
}

func (r *RPC) handleFetchData() (*internal.FetchDataResponse, error) {
	data := r.store.cachedData()
	b, err := data.MarshalBinary()

	return &internal.FetchDataResponse{
		Header: &internal.ResponseHeader{
			OK: proto.Bool(true),
		},
		Data: b}, err
}

func (r *RPC) handleJoinRequest(req *internal.JoinRequest) (*internal.JoinResponse, error) {
	if err := r.store.AddPeer(*req.Addr); err != nil {
		r.Logger.Printf("join request failed: %v", err)
	}
	r.Logger.Printf("recv join request from: %v", *req.Addr)
	return &internal.JoinResponse{
		Header: &internal.ResponseHeader{
			OK: proto.Bool(true),
		},
		EnableRaft: proto.Bool(false),
		Peers:      r.store.Peers(),
	}, nil

}

// fetchMetaData returns the latest copy of the meta store data from the current
// leader.
func (r *RPC) fetchMetaData() (*Data, error) {
	// Retrieve the current known leader.
	leader := r.store.Leader()
	if leader == "" {
		return nil, errors.New("no leader")
	}

	typeBuf := u64tob(uint64(internal.RPCType_FetchData))
	data, err := r.call(leader, typeBuf)
	if err != nil {
		return nil, err
	}

	// Unmarshal response.
	var resp internal.FetchDataResponse
	if err := proto.Unmarshal(data, &resp); err != nil {
		return nil, fmt.Errorf("unmarshal response: %s", err)
	}

	if !resp.GetHeader().GetOK() {
		return nil, fmt.Errorf("rpc failed: %s", resp.GetHeader().GetError())
	}

	ms := &Data{}
	if err := ms.UnmarshalBinary(resp.GetData()); err != nil {
		return nil, err
	}
	return ms, nil
}

func (r *RPC) join(localAddr, remoteAddr string) (*JoinResult, error) {
	typeBuf := u64tob(uint64(internal.RPCType_Join))
	req := &internal.JoinRequest{
		Addr: proto.String(localAddr),
	}
	b, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}

	data, err := r.call(remoteAddr, append(typeBuf, b...))
	if err != nil {
		return nil, err
	}

	resp := &internal.JoinResponse{}
	if err := proto.Unmarshal(data, resp); err != nil {
		return nil, err
	}

	if !resp.GetHeader().GetOK() {
		return nil, fmt.Errorf("rpc failed: %s", resp.GetHeader().GetError())
	}
	return &JoinResult{
		RaftEnabled: resp.GetEnableRaft(),
		Peers:       resp.GetPeers(),
	}, nil

}

// call sends an encoded request to the remote leader and returns
// an encoded response value.
func (r *RPC) call(dest string, b []byte) ([]byte, error) {
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

	// Write request size & bytes.
	if err := binary.Write(conn, binary.BigEndian, uint64(len(b))); err != nil {
		return nil, fmt.Errorf("write rpc size: %s", err)
	} else if _, err := conn.Write(b); err != nil {
		return nil, fmt.Errorf("write rpc: %s", err)
	}

	// Read response bytes.
	var sz uint64
	if err := binary.Read(conn, binary.BigEndian, &sz); err != nil {
		return nil, fmt.Errorf("read response size: %s", err)
	}

	buf := make([]byte, sz)
	if _, err := io.ReadFull(conn, buf); err != nil {
		return nil, fmt.Errorf("read response: %s", err)
	}

	return buf, nil
}

func u64tob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func btou64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

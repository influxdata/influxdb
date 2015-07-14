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
	}
}

// handleRPCConn reads a command from the connection and executes it.
func (r *RPC) handleRPCConn(conn net.Conn) {
	// Read and execute request.
	rpcResp, err := func() ([]byte, error) {
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

		// Ensure request can be deserialized before applying.
		var request internal.RPCRequest
		if err := proto.Unmarshal(buf, &request); err != nil {
			return nil, fmt.Errorf("unable to unmarshal request: %s", err)
		}

		switch request.GetType() {
		case internal.RPCRequest_QueryDataRequest:
			data := r.store.cachedData()
			b, err := data.MarshalBinary()
			return b, err
		default:
			panic(fmt.Errorf("cannot execute rpc: %x", buf))
		}
	}()

	// Build response message.
	var resp internal.RPCResponse
	resp.OK = proto.Bool(err == nil)
	if err != nil {
		resp.Error = proto.String(err.Error())
	}
	resp.Data = rpcResp

	// Encode response back to connection.
	if b, err := proto.Marshal(&resp); err != nil {
		panic(err)
	} else if err = binary.Write(conn, binary.BigEndian, uint64(len(b))); err != nil {
		r.Logger.Printf("unable to write rpc response size: %s", err)
	} else if _, err = conn.Write(b); err != nil {
		r.Logger.Printf("unable to write rpc response: %s", err)
	}
	conn.Close()
}

// fetchMetaData returns the latest copy of the meta store data from the current
// leader.
func (r *RPC) fetchMetaData() (*Data, error) {
	typ := internal.RPCRequest_QueryDataRequest
	b, err := proto.Marshal(&internal.RPCRequest{Type: &typ})
	if err != nil {
		return nil, err
	}

	data, err := r.call(b)
	if err != nil {
		return nil, err
	}

	ms := &Data{}
	if err := ms.UnmarshalBinary(data); err != nil {
		return nil, err
	}
	return ms, nil
}

// call sends an encoded request to the remote leader and returns
// an encoded response value.
func (r *RPC) call(b []byte) ([]byte, error) {
	// Retrieve the current known leader.
	leader := r.store.Leader()
	if leader == "" {
		return nil, errors.New("no leader")
	}

	// Create a connection to the leader.
	conn, err := net.DialTimeout("tcp", leader, 10*time.Second)
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

	// Unmarshal response.
	var resp internal.RPCResponse
	if err := proto.Unmarshal(buf, &resp); err != nil {
		return nil, fmt.Errorf("unmarshal response: %s", err)
	} else if !resp.GetOK() {
		return nil, fmt.Errorf("rpc failed: %s", resp.GetError())
	}

	return resp.GetData(), nil
}

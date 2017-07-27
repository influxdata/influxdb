package storage

import (
	"net"

	"github.com/gogo/protobuf/codec"
	"github.com/influxdata/yarpc"
	"github.com/uber-go/zap"
)

type yarpcServer struct {
	addr string
	ln   net.Listener
	rpc  *yarpc.Server

	Store  *Store
	Logger zap.Logger
}

func newYARPCServer(addr string) *yarpcServer {
	return &yarpcServer{addr: addr}
}

func (s *yarpcServer) Open() error {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	s.ln = listener
	go s.serve()
	return nil
}

func (s *yarpcServer) Close() error {
	s.rpc.Stop()
	return nil
}

func (s *yarpcServer) serve() {
	s.rpc = yarpc.NewServer(yarpc.CustomCodec(codec.New(1000)))
	RegisterStorageServer(s.rpc, &rpcService{Store: s.Store, Logger: s.Logger})
	s.Logger.Info("yarpc listening", zap.String("address", s.ln.Addr().String()))
	s.rpc.Serve(s.ln)
}

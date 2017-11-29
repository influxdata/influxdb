package storage

import (
	"net"

	"github.com/influxdata/yarpc"
	"go.uber.org/zap"
)

type yarpcServer struct {
	addr           string
	loggingEnabled bool
	rpc            *yarpc.Server
	store          *Store
	logger         *zap.Logger
}

func (s *yarpcServer) Open() error {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	s.rpc = yarpc.NewServer()
	RegisterStorageServer(s.rpc, &rpcService{loggingEnabled: s.loggingEnabled, Store: s.store, Logger: s.logger})

	go s.serve(listener)
	return nil
}

func (s *yarpcServer) Close() error {
	s.rpc.Stop()
	return nil
}

func (s *yarpcServer) serve(ln net.Listener) {
	s.logger.Info("yarpc listening", zap.String("address", ln.Addr().String()))
	s.rpc.Serve(ln)
}

package storage

import (
	"net"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type grpcServer struct {
	addr           string
	loggingEnabled bool
	rpc            *grpc.Server
	store          Store
	logger         *zap.Logger
}

func (s *grpcServer) Open() error {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	s.rpc = grpc.NewServer()
	RegisterStorageServer(s.rpc, &rpcService{loggingEnabled: s.loggingEnabled, Store: s.store, Logger: s.logger})

	go s.serve(listener)
	return nil
}

func (s *grpcServer) Close() error {
	s.rpc.Stop()
	return nil
}

func (s *grpcServer) serve(ln net.Listener) {
	s.logger.Info("grpc listening", zap.String("address", ln.Addr().String()))
	s.rpc.Serve(ln)
}

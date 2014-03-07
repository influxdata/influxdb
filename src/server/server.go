package server

import (
	"admin"
	"api/http"
	"cluster"
	log "code.google.com/p/log4go"
	"configuration"
	"coordinator"
	"datastore"
	"os"
	"os/signal"
	"syscall"
	"time"
	"wal"
)

type Server struct {
	RaftServer     *coordinator.RaftServer
	ProtobufServer *coordinator.ProtobufServer
	ClusterConfig  *cluster.ClusterConfiguration
	HttpApi        *http.HttpServer
	AdminServer    *admin.HttpServer
	Coordinator    coordinator.Coordinator
	Config         *configuration.Configuration
	RequestHandler *coordinator.ProtobufRequestHandler
	stopped        bool
	writeLog       *wal.WAL
	shardStore     *datastore.LevelDbShardDatastore
}

func NewServer(config *configuration.Configuration) (*Server, error) {
	log.Info("Opening database at %s", config.DataDir)
	shardDb, err := datastore.NewLevelDbShardDatastore(config)
	if err != nil {
		return nil, err
	}

	newClient := func(connectString string) cluster.ServerConnection {
		return coordinator.NewProtobufClient(connectString, config.ProtobufTimeout.Duration)
	}
	writeLog, err := wal.NewWAL(config)
	if err != nil {
		return nil, err
	}

	clusterConfig := cluster.NewClusterConfiguration(config, writeLog, shardDb, newClient)
	raftServer := coordinator.NewRaftServer(config, clusterConfig)
	clusterConfig.LocalRaftName = raftServer.GetRaftName()
	clusterConfig.SetShardCreator(raftServer)
	clusterConfig.CreateFutureShardsAutomaticallyBeforeTimeComes()

	coord := coordinator.NewCoordinatorImpl(config, raftServer, clusterConfig)
	requestHandler := coordinator.NewProtobufRequestHandler(coord, clusterConfig)
	protobufServer := coordinator.NewProtobufServer(config.ProtobufPortString(), requestHandler)

	raftServer.AssignCoordinator(coord)
	httpApi := http.NewHttpServer(config.ApiHttpPortString(), config.AdminAssetsDir, coord, coord, clusterConfig, raftServer)
	httpApi.EnableSsl(config.ApiHttpSslPortString(), config.ApiHttpCertPath)
	adminServer := admin.NewHttpServer(config.AdminAssetsDir, config.AdminHttpPortString())

	return &Server{
		RaftServer:     raftServer,
		ProtobufServer: protobufServer,
		ClusterConfig:  clusterConfig,
		HttpApi:        httpApi,
		Coordinator:    coord,
		AdminServer:    adminServer,
		Config:         config,
		RequestHandler: requestHandler,
		writeLog:       writeLog,
		shardStore:     shardDb}, nil
}

func (self *Server) ListenAndServe() error {
	go self.ProtobufServer.ListenAndServe()

	err := self.RaftServer.ListenAndServe()
	if err != nil {
		return err
	}

	log.Info("Waiting for local server to be added")
	self.ClusterConfig.WaitForLocalServerLoaded()
	self.writeLog.SetServerId(self.ClusterConfig.ServerId())

	log.Info("Recovering from log...")
	err = self.ClusterConfig.RecoverFromWAL()
	if err != nil {
		return err
	}
	log.Info("recovered")

	err = self.Coordinator.(*coordinator.CoordinatorImpl).ConnectToProtobufServers(self.Config.ProtobufConnectionString())
	if err != nil {
		return err
	}
	go self.ListenForSignals()
	log.Info("Starting admin interface on port %d", self.Config.AdminHttpPort)
	go self.AdminServer.ListenAndServe()
	log.Info("Starting Http Api server on port %d", self.Config.ApiHttpPort)
	self.HttpApi.ListenAndServe()
	return nil
}

func (self *Server) Stop() {
	if self.stopped {
		return
	}
	log.Info("Stopping server")
	self.stopped = true

	log.Info("Stopping api server")
	self.HttpApi.Close()
	log.Info("Api server stopped")

	log.Info("Stopping admin server")
	self.AdminServer.Close()
	log.Info("admin server stopped")

	log.Info("Stopping raft server")
	self.RaftServer.Close()
	log.Info("Raft server stopped")

	log.Info("Stopping protobuf server")
	self.ProtobufServer.Close()
	log.Info("protobuf server stopped")

	log.Info("Stopping wal")
	self.writeLog.Close()
	log.Info("wal stopped")

	log.Info("Stopping shard store")
	self.shardStore.Close()
	log.Info("shard store stopped")
}

func (self *Server) ListenForSignals() {
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT)
	for {
		sig := <-ch
		log.Info("Received signal: %s\n", sig.String())
		switch sig {
		case syscall.SIGINT, syscall.SIGTERM:
			self.Stop()
			time.Sleep(time.Second)
			os.Exit(0)
		}
	}
}

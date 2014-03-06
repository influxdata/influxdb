package server

import (
	"admin"
	"api/graphite"
	"api/http"
	"cluster"
	log "code.google.com/p/log4go"
	"configuration"
	"coordinator"
	"datastore"
	"wal"
)

type Server struct {
	RaftServer     *coordinator.RaftServer
	ProtobufServer *coordinator.ProtobufServer
	ClusterConfig  *cluster.ClusterConfiguration
	HttpApi        *http.HttpServer
	GraphiteApi    *graphite.Server
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
	graphiteApi := graphite.NewServer(config.GraphitePortString(), config.GraphiteDatabase, coord, clusterConfig)
	adminServer := admin.NewHttpServer(config.AdminAssetsDir, config.AdminHttpPortString())

	return &Server{
		RaftServer:     raftServer,
		ProtobufServer: protobufServer,
		ClusterConfig:  clusterConfig,
		HttpApi:        httpApi,
		GraphiteApi:    graphiteApi,
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
	log.Info("Starting admin interface on port %d", self.Config.AdminHttpPort)
	go self.AdminServer.ListenAndServe()
	if self.Config.GraphiteEnabled {
		if self.Config.GraphitePort <= 0 || self.Config.GraphiteDatabase == "" {
			log.Warn("Cannot start graphite server. please check your configuration")
		} else {
			log.Info("Starting Graphite Listener on port %d", self.Config.GraphitePort)
			go self.GraphiteApi.ListenAndServe()
		}
	}
	log.Info("Starting Http Api server on port %d", self.Config.ApiHttpPort)
	self.HttpApi.ListenAndServe()
	return nil
}

func (self *Server) Stop() {
	if self.stopped {
		return
	}
	self.stopped = true
	self.RaftServer.Close()
	self.HttpApi.Close()
	self.ProtobufServer.Close()
	self.AdminServer.Close()
	self.writeLog.Close()
	self.shardStore.Close()
	// TODO: close admin server and protobuf client connections
	log.Info("Stopping server")
}

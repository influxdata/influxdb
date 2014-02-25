package server

import (
	"admin"
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
	Db             datastore.Datastore
	ProtobufServer *coordinator.ProtobufServer
	ClusterConfig  *cluster.ClusterConfiguration
	HttpApi        *http.HttpServer
	AdminServer    *admin.HttpServer
	Coordinator    coordinator.Coordinator
	Config         *configuration.Configuration
	RequestHandler *coordinator.ProtobufRequestHandler
	stopped        bool
	writeLog       *wal.WAL
}

func NewServer(config *configuration.Configuration) (*Server, error) {
	log.Info("Opening database at %s", config.DataDir)
	db, err := datastore.NewLevelDbDatastore(config.DataDir, config.LevelDbMaxOpenFiles)
	if err != nil {
		return nil, err
	}

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

	coord := coordinator.NewCoordinatorImpl(config, db, raftServer, clusterConfig)
	requestHandler := coordinator.NewProtobufRequestHandler(db, coord, clusterConfig)
	protobufServer := coordinator.NewProtobufServer(config.ProtobufPortString(), requestHandler)

	raftServer.AssignCoordinator(coord)
	httpApi := http.NewHttpServer(config.ApiHttpPortString(), config.AdminAssetsDir, coord, coord, clusterConfig, raftServer)
	httpApi.EnableSsl(config.ApiHttpSslPortString(), config.ApiHttpCertPath)
	adminServer := admin.NewHttpServer(config.AdminAssetsDir, config.AdminHttpPortString())

	return &Server{
		RaftServer:     raftServer,
		Db:             db,
		ProtobufServer: protobufServer,
		ClusterConfig:  clusterConfig,
		HttpApi:        httpApi,
		Coordinator:    coord,
		AdminServer:    adminServer,
		Config:         config,
		RequestHandler: requestHandler,
		writeLog:       writeLog}, nil
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
	self.Db.Close()
	self.HttpApi.Close()
	self.ProtobufServer.Close()
	self.AdminServer.Close()
	self.writeLog.Close()
	// TODO: close admin server and protobuf client connections
	log.Info("Stopping server")
}

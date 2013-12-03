package server

import (
	"admin"
	"api/http"
	"configuration"
	"coordinator"
	"datastore"
	"engine"
	"log"
	"time"
)

type Server struct {
	RaftServer     *coordinator.RaftServer
	Db             datastore.Datastore
	ProtobufServer *coordinator.ProtobufServer
	ClusterConfig  *coordinator.ClusterConfiguration
	HttpApi        *http.HttpServer
	AdminServer    *admin.HttpServer
	Coordinator    coordinator.Coordinator
	Config         *configuration.Configuration
	RequestHandler *coordinator.ProtobufRequestHandler
	stopped        bool
}

func NewServer(config *configuration.Configuration) (*Server, error) {
	log.Println("Opening database at ", config.DataDir)
	db, err := datastore.NewLevelDbDatastore(config.DataDir)
	if err != nil {
		return nil, err
	}

	clusterConfig := coordinator.NewClusterConfiguration()
	raftServer := coordinator.NewRaftServer(config, clusterConfig)
	coord := coordinator.NewCoordinatorImpl(db, raftServer, clusterConfig)
	requestHandler := coordinator.NewProtobufRequestHandler(db, coord, clusterConfig)
	protobufServer := coordinator.NewProtobufServer(config.ProtobufPortString(), requestHandler)

	eng, err := engine.NewQueryEngine(coord)
	if err != nil {
		return nil, err
	}

	httpApi := http.NewHttpServer(config.ApiHttpPortString(), config.AdminAssetsDir, eng, coord, coord)
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
		RequestHandler: requestHandler}, nil
}

func (self *Server) ListenAndServe() error {
	go self.ProtobufServer.ListenAndServe()

	retryUntilJoinedCluster := false
	if len(self.Config.SeedServers) > 0 {
		retryUntilJoinedCluster = true
	}
	go self.RaftServer.ListenAndServe(self.Config.SeedServers, retryUntilJoinedCluster)
	time.Sleep(time.Second * 3)
	err := self.Coordinator.(*coordinator.CoordinatorImpl).ConnectToProtobufServers(self.Config.ProtobufConnectionString())
	if err != nil {
		return err
	}
	log.Println("Starting admin interface on port", self.Config.AdminHttpPort)
	go self.AdminServer.ListenAndServe()
	log.Println("Starting Http Api server on port", self.Config.ApiHttpPort)
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
	// TODO: close admin server and protobuf client connections
}

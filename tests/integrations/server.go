package integrations

//import (
//"fmt"
//"log"
//"net/http"
//"net/url"
//"path/filepath"

//"github.com/influxdb/influxdb"
//"github.com/influxdb/influxdb/messaging"
//)

//type Config struct {
//BrokerURL *url.URL
//BrokerDir string

//ServerURL *url.URL
//ServerDir string
//}

//type Server struct {
//broker        *messaging.Broker
//brokerHandler http.Handler

//server        *influxdb.Server
//serverHandler http.Handler

//config Config
//}

//func NewServer(c Config) (*Server, error) {
//return &Server{
//config: c,
//}, nil
//}

//func (s *Server) Start() error {
//// Launch the broker
//s.broker = messaging.NewBroker()
//if err := s.broker.Open(s.config.BrokerDir, s.config.BrokerURL); err != nil {
//return err
//}
//if err := s.broker.Initialize(); err != nil {
//return err
//}
//s.brokerHandler = messaging.NewHandler(s.broker)
//go func() { log.Fatal(http.ListenAndServe(s.config.BrokerURL.Host, s.brokerHandler)) }()

//// Lanch the server
//s.server = influxdb.NewServer()
//if err := s.server.Open(s.config.ServerDir); err != nil {
//return err
//}

//if err := s.broker.CreateReplica(1); err != nil {
//return err
//}

//c := messaging.NewClient(1)
//if err := c.Open(filepath.Join(s.server.Path(), "messaging"), []*url.URL{s.broker.URL()}); err != nil {
//return fmt.Errorf("Couldn't open new messaging client: %s", err)
//}
//if err := s.server.SetClient(c); err != nil {
//return fmt.Errorf("Couldn't setclient: %s", err)
//}

//// Initialize the server.
//if err := s.server.Initialize(s.broker.URL()); err != nil {
//return err
//}

//return nil
//}

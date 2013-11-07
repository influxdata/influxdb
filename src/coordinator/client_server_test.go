package coordinator

import (
	"datastore"
	"fmt"
	. "launchpad.net/gocheck"
	"os"
	"os/signal"
	"protocol"
	"runtime"
	"syscall"
	"time"
)

type ClientServerSuite struct{}

var _ = Suite(&ClientServerSuite{})

const DB_DIR = "/tmp/influxdb/datastore_test"

func waitForSignals(stopped <-chan bool) {
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT)
	for {
		sig := <-ch
		fmt.Printf("Received signal: %s\n", sig.String())
		switch sig {
		case syscall.SIGINT, syscall.SIGTERM:
			runtime.SetCPUProfileRate(0)
			<-stopped
			os.Exit(0)
		}
	}
}

func startProfiler(filename string) error {
	if filename == "" {
		return nil
	}

	cpuProfileFile, err := os.Create(filename)
	if err != nil {
		return err
	}
	runtime.SetCPUProfileRate(500)
	stopped := make(chan bool)

	go waitForSignals(stopped)

	go func() {
		for {
			select {
			default:
				data := runtime.CPUProfile()
				if data == nil {
					cpuProfileFile.Close()
					stopped <- true
					break
				}
				cpuProfileFile.Write(data)
			}
		}
	}()
	return nil
}

func newDatastore(c *C) datastore.Datastore {
	os.MkdirAll(DB_DIR, 0744)
	db, err := datastore.NewLevelDbDatastore(DB_DIR)
	c.Assert(err, Equals, nil)
	return db
}

func cleanDb(db datastore.Datastore) {
	if db != nil {
		db.Close()
	}
	os.RemoveAll(DB_DIR)
}

func (self *ClientServerSuite) TestClientCanMakeRequests(c *C) {
	runtime.GOMAXPROCS(5)
	startProfiler("/tmp/asdf")
	go waitForSignals(make(chan bool))
	server := startAndVerifyCluster(1, c)[0]
	defer clean(server)
	db := newDatastore(c)
	requestHandler := NewProtobufRequestHandler(db, server)
	protobufServer := NewProtobufServer(":8091", requestHandler)
	c.Assert(protobufServer, Not(IsNil))
	protobufClient := NewProtobufClient("localhost:8091")
	responseStream := make(chan *protocol.Response, 1)

	mock := `
  {
    "points": [
      { "values": [{"int64_value": 3}]},
      { "values": [{"int64_value": 23}]}
    ],
    "name": "foo",
    "fields": ["val"]
  }`
	fmt.Println("creating series")
	series := stringToSeries(mock, c)
	id := uint32(1)
	database := "pauldb"
	proxyWrite := protocol.Request_PROXY_WRITE
	request := &protocol.Request{Id: &id, Type: &proxyWrite, Database: &database, Series: series}

	time.Sleep(time.Second * 1)
	err := protobufClient.MakeRequest(request, responseStream)
	c.Assert(err, IsNil)
	timer := time.NewTimer(time.Second)
	select {
	case <-timer.C:
		c.Error("Timed out waiting for response")
	case response := <-responseStream:
		c.Assert(*response.Type, Equals, protocol.Response_WRITE_OK)
	}
}

func (self *ClientServerSuite) TestClientReconnectsIfDisconnected(c *C) {
}

func (self *ClientServerSuite) TestServerExecutesReplayRequestIfWriteIsOutOfSequence(c *C) {
}

func (self *ClientServerSuite) TestServerKillsOldHandlerWhenClientReconnects(c *C) {

}

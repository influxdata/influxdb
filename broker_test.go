package influxdb_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/influxdb/influxdb"
)

func TestBroker_WillRunQueries(t *testing.T) {
	// this handler should just work
	testHandler := &BrokerTestHandler{}
	server := httptest.NewServer(testHandler)
	defer server.Close()
	// this will timeout on the trigger request
	timeoutHandler := &BrokerTestHandler{wait: 1100 * time.Millisecond}
	timeoutServer := httptest.NewServer(timeoutHandler)
	defer timeoutServer.Close()
	// this will return a 500
	badHandler := &BrokerTestHandler{sendError: true}
	badServer := httptest.NewServer(badHandler)
	defer badServer.Close()

	// return the data nodes so all the failure cases get hit first
	testNodeServer := &BrokerTestDataNodeServer{
		dataNodes: []*influxdb.DataNode{
			&influxdb.DataNode{URL: &url.URL{Host: "127.0.0.1:8090"}},
			&influxdb.DataNode{URL: &url.URL{Host: timeoutServer.URL[7:]}},
			&influxdb.DataNode{URL: &url.URL{Host: badServer.URL[7:]}},
			&influxdb.DataNode{URL: &url.URL{Host: server.URL[7:]}},
		},
	}

	b := influxdb.NewBroker()
	// set the trigger times and failure sleeps for the test
	b.TriggerInterval = 2 * time.Millisecond
	b.TriggerTimeout = 100 * time.Millisecond
	b.TriggerFailurePause = 2 * time.Millisecond

	f := tempfile()
	defer os.Remove(f)

	if err := b.Open(f, &url.URL{Host: "127.0.0.1:8080"}); err != nil {
		t.Fatalf("error opening broker: %s", err)
	}
	if err := b.Initialize(); err != nil {
		t.Fatalf("error initializing broker: %s", err)
	}
	defer b.Close()

	b.RunContinuousQueryLoop(testNodeServer)
	// every failure and success case should be hit in this time frame
	time.Sleep(1400 * time.Millisecond)
	if timeoutHandler.requestCount != 1 {
		t.Fatal("broker should have only sent 1 request to the server that times out.")
	}
	if badHandler.requestCount != 1 {
		t.Fatal("broker should have only sent 1 request to the bad server. i.e. it didn't keep the state to make request to the good server")
	}
	if testHandler.requestCount < 1 || testHandler.processRequestCount < 1 {
		t.Fatal("broker failed to send multiple continuous query requests to the data node")
	}
}

type BrokerTestDataNodeServer struct {
	dataNodes []*influxdb.DataNode
}

func (b *BrokerTestDataNodeServer) DataNodes() []*influxdb.DataNode {
	return b.dataNodes
}

type BrokerTestHandler struct {
	requestCount        int
	processRequestCount int
	sendError           bool
	wait                time.Duration
}

// ServeHTTP serves an HTTP request.
func (h *BrokerTestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.requestCount++
	<-time.After(h.wait)
	if h.sendError {
		w.WriteHeader(http.StatusInternalServerError)
	}
	switch r.URL.Path {
	case "/process_continuous_queries":
		if r.Method == "POST" {
			h.processRequestCount++
			w.WriteHeader(http.StatusAccepted)
		} else {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		}
	default:
		http.NotFound(w, r)
	}
}

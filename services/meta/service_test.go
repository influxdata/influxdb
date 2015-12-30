package meta_test

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"runtime"
	"testing"

	"github.com/influxdb/influxdb"
	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/services/meta"
	"github.com/influxdb/influxdb/tcp"
)

// Test the ping endpoint.
func TestMetaService_PingEndpoint(t *testing.T) {
	t.Parallel()

	cfg := newConfig()
	defer os.RemoveAll(cfg.Dir)
	s := newService(cfg)
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	url, err := url.Parse(s.URL())
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Head("http://" + url.String() + "/ping")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status:\n\texp: %d\n\tgot: %d\n", http.StatusOK, resp.StatusCode)
	}
	_, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMetaService_CreateDatabase(t *testing.T) {
	cfg := newConfig()
	defer os.RemoveAll(cfg.Dir)
	s := newService(cfg)
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	c := meta.NewClient([]string{s.URL()}, false)
	if err := c.Open(); err != nil {
		t.Fatalf(err.Error())
	}
	defer c.Close()

	c.ExecuteStatement(mustParseStatement("CREATE DATABASE foo"))
	db, err := c.Database("foo")
	if err != nil {
		t.Fatalf(err.Error())
	}
	if db.Name != "foo" {
		t.Fatalf("db name wrong: %s", db.Name)
	}
}

func TestMetaService_CreateRemoveMetaNode(t *testing.T) {
	cfg1 := newConfig()
	defer os.RemoveAll(cfg1.Dir)
	cfg2 := newConfig()
	defer os.RemoveAll(cfg2.Dir)
	cfg3 := newConfig()
	defer os.RemoveAll(cfg3.Dir)
	cfg4 := newConfig()
	defer os.RemoveAll(cfg4.Dir)

	s1 := newService(cfg1)
	if err := s1.Open(); err != nil {
		t.Fatalf(err.Error())
	}
	defer s1.Close()

	cfg2.JoinPeers = []string{s1.URL()}
	s2 := newService(cfg2)
	if err := s2.Open(); err != nil {
		t.Fatal(err.Error())
	}
	defer s2.Close()

	func() {
		cfg3.JoinPeers = []string{s2.URL()}
		s3 := newService(cfg3)
		if err := s3.Open(); err != nil {
			t.Fatal(err.Error())
		}
		defer s3.Close()

		fmt.Println("ALL OPEN!")

		c1 := meta.NewClient([]string{s1.URL()}, false)
		if err := c1.Open(); err != nil {
			t.Fatal(err.Error())
		}
		defer c1.Close()

		metaNodes, _ := c1.MetaNodes()
		if len(metaNodes) != 3 {
			t.Fatalf("meta nodes wrong: %v", metaNodes)
		}
	}()

	c := meta.NewClient([]string{s1.URL()}, false)
	if err := c.Open(); err != nil {
		t.Fatal(err.Error())
	}
	defer c.Close()

	if res := c.ExecuteStatement(mustParseStatement("DROP META SERVER 3")); res.Err != nil {
		t.Fatal(res.Err)
	}

	metaNodes, _ := c.MetaNodes()
	if len(metaNodes) != 2 {
		t.Fatalf("meta nodes wrong: %v", metaNodes)
	}

	cfg4.JoinPeers = []string{s1.URL()}
	s4 := newService(cfg4)
	if err := s4.Open(); err != nil {
		t.Fatal(err.Error())
	}
	defer s4.Close()

	metaNodes, _ = c.MetaNodes()
	if len(metaNodes) != 3 {
		t.Fatalf("meta nodes wrong: %v", metaNodes)
	}
}

// Ensure that if we attempt to create a database and the client
// is pointed at a server that isn't the leader, it automatically
// hits the leader and finishes the command
func TestMetaService_CommandAgainstNonLeader(t *testing.T) {

}

func newConfig() *meta.Config {
	cfg := meta.NewConfig()
	cfg.BindAddress = "127.0.0.1:0"
	cfg.HTTPBindAddress = "127.0.0.1:0"
	cfg.Dir = testTempDir(2)
	return cfg
}

func testTempDir(skip int) string {
	// Get name of the calling function.
	pc, _, _, ok := runtime.Caller(skip)
	if !ok {
		panic("failed to get name of test function")
	}
	_, prefix := path.Split(runtime.FuncForPC(pc).Name())
	// Make a temp dir prefixed with calling function's name.
	dir, err := ioutil.TempDir("/tmp", prefix)
	if err != nil {
		panic(err)
	}
	return dir
}

func newService(cfg *meta.Config) *meta.Service {
	// Open shared TCP connection.
	ln, err := net.Listen("tcp", cfg.BindAddress)
	if err != nil {
		panic(err)
	}

	// Multiplex listener.
	mux := tcp.NewMux()

	s := meta.NewService(cfg, &influxdb.Node{})
	s.RaftListener = mux.Listen(meta.MuxHeader)

	go mux.Serve(ln)

	return s
}

func mustParseStatement(s string) influxql.Statement {
	stmt, err := influxql.ParseStatement(s)
	if err != nil {
		panic(err)
	}
	return stmt
}

package admin

import (
	"io/ioutil"
	. "launchpad.net/gocheck"
	"net/http"
	"path"
	"testing"
)

// Hook up gocheck into the gotest runner.
func Test(t *testing.T) {
	TestingT(t)
}

type HttpServerSuite struct {
}

var _ = Suite(&HttpServerSuite{})

func (self *HttpServerSuite) TestServesIndexByDefault(c *C) {
	// prepare some dummy site files
	dir := c.MkDir()
	content := []byte("Welcome to Influxdb")
	path := path.Join(dir, "index.html")
	err := ioutil.WriteFile(path, content, 0644)
	c.Assert(err, IsNil)
	s := NewHttpServer(dir, ":8083")
	go func() { s.ListenAndServe() }()
	resp, err := http.Get("http://localhost:8083/")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	actualContent, err := ioutil.ReadAll(resp.Body)
	c.Assert(string(actualContent), Equals, string(content))
	c.Assert(err, IsNil)
}

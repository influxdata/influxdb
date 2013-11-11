package admin

import (
	"io/ioutil"
	. "launchpad.net/gocheck"
	"net/http"
	"os"
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
	err := os.Mkdir("./site/", 0755)
	c.Assert(err, IsNil)
	defer os.RemoveAll("./site/")
	content := []byte("Welcome to Influxdb")
	err = ioutil.WriteFile("./site/index.html", content, 0644)

	s := NewHttpServer("./site/", ":8083")
	c.Assert(err, IsNil)
	go func() { s.ListenAndServe() }()
	resp, err := http.Get("http://localhost:8083/")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	actualContent, err := ioutil.ReadAll(resp.Body)
	c.Assert(actualContent, DeepEquals, content)
	c.Assert(err, IsNil)
}

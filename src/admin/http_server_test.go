package admin

import (
	"io/ioutil"
	. "launchpad.net/gocheck"
	"net/http"
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
	s := NewHttpServer("./site/", ":8083")
	go func() { s.ListenAndServe() }()
	resp, err := http.Get("http://localhost:8083/")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	_, err = ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
}

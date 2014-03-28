package admin

import (
	"net"
	"net/http"
	"strings"
)

type HttpServer struct {
	homeDir  string
	port     string
	listener net.Listener
	closed   bool
}

/*
  homeDir is the directory that is the root of the admin site.
  port should be a string that looks like ":8080" or whatever port to serve on.
*/
func NewHttpServer(homeDir, port string) *HttpServer {
	return &HttpServer{homeDir: homeDir, port: port, closed: true}
}

func (self *HttpServer) ListenAndServe() {
	if self.port == "" {
		return
	}

	self.closed = false
	var err error
	self.listener, err = net.Listen("tcp", self.port)
	if err != nil {
		panic(err)
	}
	err = http.Serve(self.listener, http.FileServer(http.Dir(self.homeDir)))
	if !strings.Contains(err.Error(), "closed") {
		panic(err)
	}
}

func (self *HttpServer) Close() {
	if self.closed {
		return
	}

	self.closed = true
	self.listener.Close()
}

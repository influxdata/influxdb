package admin

import (
	"net"
	"net/http"
	"strings"

	"github.com/rakyll/statik/fs"

	_ "github.com/influxdb/influxdb/statik"
)

type HttpServer struct {
	port     string
	listener net.Listener
	closed   bool
}

// port should be a string that looks like ":8083" or whatever port to serve on.
func NewHttpServer(port string) *HttpServer {
	return &HttpServer{port: port, closed: true}
}

func (s *HttpServer) ListenAndServe() {
	if s.port == "" {
		return
	}

	s.closed = false
	var err error
	s.listener, _ = net.Listen("tcp", s.port)
	if err != nil {
		return
	}

	statikFS, _ := fs.New()

	err = http.Serve(s.listener, http.FileServer(statikFS))
	if !strings.Contains(err.Error(), "closed") {
		panic(err)
	}
}

func (s *HttpServer) Close() {
	if s.closed {
		return
	}

	s.closed = true
	s.listener.Close()
}

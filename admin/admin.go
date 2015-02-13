package admin

import (
	"net"
	"net/http"
	"strings"

	"github.com/rakyll/statik/fs"

	_ "github.com/influxdb/influxdb/statik"
)

type Server struct {
	port     string
	listener net.Listener
	closed   bool
}

// port should be a string that looks like ":8083" or whatever port to serve on.
func NewServer(port string) *Server {
	return &Server{port: port, closed: true}
}

func (s *Server) ListenAndServe() {
	if s.port == "" {
		return
	}

	var err error
	s.listener, err = net.Listen("tcp", s.port)
	if err != nil {
		return
	}

	s.closed = false
	statikFS, _ := fs.New()

	err = http.Serve(s.listener, http.FileServer(statikFS))
	if !strings.Contains(err.Error(), "closed") {
		panic(err)
	}
}

func (s *Server) Close() {
	if s.closed {
		return
	}

	s.closed = true
	s.listener.Close()
}

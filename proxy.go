package mrfusion

import (
	"encoding/json"
	"net/http"

	"golang.org/x/net/context"
)

// Request contains the information needed to make an HTTP call.
type Request struct {
	Method string          // Method is the HTTP Verb (POST, PUT, PATCH, GET, etc)
	Path   string          // URL Path (not host)
	Body   json.RawMessage // Body is sent if not nil
}

//Proxy will forward the request onto the
type Proxy interface {
	// Do transformation Request and returns the http.Response
	Do(ctx context.Context, req *Request) (*http.Response, error)
	// Connect will transform Server a URL for `Do`
	Connect(ctx context.Context, srv *Server) error
}

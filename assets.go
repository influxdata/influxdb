package mrfusion

import "net/http"

// Assets returns a handler to serve the website.
type Assets interface {
	Serve() http.Handler
}

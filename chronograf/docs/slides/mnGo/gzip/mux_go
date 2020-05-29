// +build OMIT
package server

import (
	"net/http"

	"github.com/NYTimes/gziphandler"
	"github.com/bouk/httprouter"
)

func NewMux(opts MuxOpts, service Service) http.Handler {
	router := httprouter.New()
	...
	prefixedAssets := NewDefaultURLPrefixer(basepath, assets, opts.Logger)
	// Compress the assets with gzip if an accepted encoding
	compressed := gziphandler.GzipHandler(prefixedAssets)
	...
	return handler
}

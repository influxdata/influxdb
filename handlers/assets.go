package handlers

import (
	"net/http"

	"github.com/influxdata/mrfusion"
	"github.com/influxdata/mrfusion/dist"
)

const (
	Dir     = "ui/build"
	Default = "ui/build/index.html"
)

// AssetsOpts configures the asset middleware
type AssetsOpts struct {
	// Develop when true serves assets from ui/build directory directly; false will use internal bindata.
	Develop bool
	// Logger will log the asset served
	Logger mrfusion.Logger
}

// Assets creates a middleware that will serve a single page app.
func Assets(opts AssetsOpts) http.Handler {
	var assets mrfusion.Assets
	if opts.Develop {
		assets = &dist.DebugAssets{
			Dir:     Dir,
			Default: Default,
		}
	} else {
		assets = &dist.BindataAssets{
			Prefix:  Dir,
			Default: Default,
		}
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if opts.Logger != nil {
			opts.Logger.
				WithField("component", "server").
				WithField("remote_addr", r.RemoteAddr).
				WithField("method", r.Method).
				WithField("url", r.URL).
				Info("Serving assets")
		}
		assets.Handler().ServeHTTP(w, r)
	})
}

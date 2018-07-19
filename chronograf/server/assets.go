package server

import (
	"net/http"

	"github.com/influxdata/platform/chronograf"
	"github.com/influxdata/platform/chronograf/dist"
)

const (
	// Dir is prefix of the assets in the bindata
	Dir = "../ui/build"
	// Default is the default item to load if 404
	Default = "../ui/build/index.html"
	// DebugDir is the prefix of the assets in development mode
	DebugDir = "ui/build"
	// DebugDefault is the default item to load if 404
	DebugDefault = "ui/build/index.html"
	// DefaultContentType is the content-type to return for the Default file
	DefaultContentType = "text/html; charset=utf-8"
)

// AssetsOpts configures the asset middleware
type AssetsOpts struct {
	// Develop when true serves assets from ui/build directory directly; false will use internal bindata.
	Develop bool
	// Logger will log the asset served
	Logger chronograf.Logger
}

// Assets creates a middleware that will serve a single page app.
func Assets(opts AssetsOpts) http.Handler {
	var assets chronograf.Assets
	if opts.Develop {
		assets = &dist.DebugAssets{
			Dir:     DebugDir,
			Default: DebugDefault,
		}
	} else {
		assets = &dist.BindataAssets{
			Prefix:             Dir,
			Default:            Default,
			DefaultContentType: DefaultContentType,
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

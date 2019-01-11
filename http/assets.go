package http

import (
	"net/http"

	// TODO: use platform version of the code
	"github.com/influxdata/influxdb/chronograf"
	"github.com/influxdata/influxdb/chronograf/dist"
)

const (
	// Dir is prefix of the assets in the bindata
	Dir = "../../ui/build"
	// Default is the default item to load if 404
	Default = "../../ui/build/index.html"
	// DebugDir is the prefix of the assets in development mode
	DebugDir = "ui/build"
	// DebugDefault is the default item to load if 404
	DebugDefault = "ui/build/index.html"
	// DefaultContentType is the content-type to return for the Default file
	DefaultContentType = "text/html; charset=utf-8"
)

// AssetHandler is an http handler for serving chronograf assets.
type AssetHandler struct {
	DeveloperMode bool
}

// NewAssetHandler is the constructor an asset handler.
func NewAssetHandler() *AssetHandler {
	return &AssetHandler{
		DeveloperMode: true,
	}
}

// ServeHTTP implements the http handler interface for serving assets.
func (h *AssetHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var assets chronograf.Assets
	if h.DeveloperMode {
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

	assets.Handler().ServeHTTP(w, r)
}

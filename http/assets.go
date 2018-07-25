package http

import (
	"net/http"

	// TODO: use platform version of the code
	"github.com/influxdata/platform/chronograf"
	"github.com/influxdata/platform/chronograf/dist"
)

const (
	// Dir is prefix of the assets in the bindata
	Dir = "../chronograf/ui/build"
	// Default is the default item to load if 404
	Default = "../chronograf/ui/build/index.html"
	// DebugDir is the prefix of the assets in development mode
	DebugDir = "chronograf/ui/build"
	// DebugDefault is the default item to load if 404
	DebugDefault = "chronograf/ui/build/index.html"
	// DefaultContentType is the content-type to return for the Default file
	DefaultContentType = "text/html; charset=utf-8"
)

// AssetHandler is an http handler for serving chronograf assets.
type AssetHandler struct {
	Develop bool
}

// NewAssetHandler is the constructor an asset handler.
func NewAssetHandler() *AssetHandler {
	return &AssetHandler{
		Develop: true,
	}
}

// ServeHTTP implements the http handler interface for serving assets.
func (h *AssetHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var assets chronograf.Assets
	if h.Develop {
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

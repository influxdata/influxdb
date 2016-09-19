package dist

import (
	"net/http"

	"github.com/elazarl/go-bindata-assetfs"
)

// DebugAssets serves assets via a specified directory
type DebugAssets struct {
	Dir string // Dir is a directory location of asset files
}

// Handler is an http.FileServer for the Dir
func (d *DebugAssets) Handler() http.Handler {
	return http.FileServer(http.Dir(d.Dir))
}

// BindataAssets serves assets from go-bindata
type BindataAssets struct {
	Prefix string // Prefix is prepended to the http file request
}

// Handler serves go-bindata using a go-bindata-assetfs façade
func (b *BindataAssets) Handler() http.Handler {
	var dir http.FileSystem = &assetfs.AssetFS{
		Asset:     Asset,
		AssetDir:  AssetDir,
		AssetInfo: AssetInfo,
		Prefix:    b.Prefix,
	}
	return http.FileServer(dir)
}

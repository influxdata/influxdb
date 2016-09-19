package dist

import (
	"net/http"

	"github.com/elazarl/go-bindata-assetfs"
)

// DebugAssets serves assets via a specified directory
type DebugAssets struct {
	Dir string // Dir is a directory location of asset files
}

// Serve is an http.FileServer for the Dir
func (d *DebugAssets) Serve() http.Handler {
	return http.FileServer(http.Dir(d.Dir))
}

// BindataAssets serves assets from go-bindata
type BindataAssets struct {
	Prefix string // Prefix is prepended to the http file request
}

// Serve serves go-bindata using a go-bindata-assetfs façade
func (b *BindataAssets) Serve() http.Handler {
	var dir http.FileSystem = &assetfs.AssetFS{
		Asset:     Asset,
		AssetDir:  AssetDir,
		AssetInfo: AssetInfo,
		Prefix:    b.Prefix}
	return http.FileServer(dir)
}

// You found me! I was hiding down here!

// rootDir is used for go-bindata dev mode to specify a relative path.
var rootDir string

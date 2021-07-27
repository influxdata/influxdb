//go:generate env GO111MODULE=on go run github.com/kevinburke/go-bindata/go-bindata -o static_gen.go -ignore 'map|go' -tags assets -pkg static data/...

package static

import (
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	assetfs "github.com/elazarl/go-bindata-assetfs"
	platform "github.com/influxdata/influxdb/v2"
)

const (
	// defaultFile is the default UI asset file that will be served if no other
	// static asset matches. This is particularly useful for serving content
	// related to a SPA with client-side routing.
	defaultFile = "index.html"

	// embedBaseDir is the prefix for files in the bundle with the binary.
	embedBaseDir = "data"

	// uiBaseDir is the directory in embedBaseDir where the built UI assets
	// reside.
	uiBaseDir = "build"

	// swaggerFile is the name of the swagger JSON.
	swaggerFile = "swagger.json"
)

// NewAssetHandler returns an http.Handler to serve files from the provided
// path. If no --assets-path flag is used when starting influxd, the path will
// be empty and files are served from the embedded filesystem.
func NewAssetHandler(assetsPath string) http.Handler {
	var fileOpener http.FileSystem

	if assetsPath == "" {
		fileOpener = &assetfs.AssetFS{
			Asset:     Asset,
			AssetDir:  AssetDir,
			AssetInfo: AssetInfo,
			Prefix:    filepath.Join(embedBaseDir, uiBaseDir),
		}
	} else {
		fileOpener = http.FS(os.DirFS(assetsPath))
	}

	return mwSetCacheControl(assetHandler(fileOpener))
}

// NewSwaggerHandler returns an http.Handler to serve the swaggerFile from the
// embedBaseDir. If the swaggerFile is not found, returns a 404.
func NewSwaggerHandler() http.Handler {
	fileOpener := &assetfs.AssetFS{
		Asset:     Asset,
		AssetDir:  AssetDir,
		AssetInfo: AssetInfo,
		Prefix:    embedBaseDir,
	}

	return mwSetCacheControl(swaggerHandler(fileOpener))
}

// mwSetCacheControl sets a default cache control header.
func mwSetCacheControl(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Cache-Control", "public, max-age=3600")
		next.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}

// swaggerHandler returns a handler that serves the swaggerFile or returns a 404
// if the swaggerFile is not present.
func swaggerHandler(fileOpener http.FileSystem) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		f, err := fileOpener.Open(swaggerFile)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		defer f.Close()

		staticFileHandler(f).ServeHTTP(w, r)
	}

	return http.HandlerFunc(fn)
}

// assetHandler returns a handler that either serves the file at that path, or
// the default file if a file cannot be found at that path. If the default file
// is served, the request path is re-written to the root path to simplify
// metrics reporting.
func assetHandler(fileOpener http.FileSystem) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(path.Clean(r.URL.Path), "/")
		// If the root directory is being requested, respond with the default file.
		if name == "" {
			name = defaultFile
		}

		// Try to open the file requested by name, falling back to the default file.
		// If even the default file can't be found, the binary must not have been
		// built with assets, so respond with not found.
		f, fallback, err := openAsset(fileOpener, name)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		defer f.Close()

		// If the default file will be served because the requested path didn't
		// match any existing files, re-write the request path to the root path.
		// This is to ensure that metrics do not get collected for an arbitrarily
		// large range of incorrect paths.
		if fallback {
			r.URL.Path = "/"
		}

		staticFileHandler(f).ServeHTTP(w, r)
	}

	return http.HandlerFunc(fn)
}

// staticFileHandler sets the ETag header prior to calling http.ServeContent
// with the contents of the file.
func staticFileHandler(f fs.File) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		content, ok := f.(io.ReadSeeker)
		if !ok {
			err := fmt.Errorf("could not open file for reading")
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		i, err := f.Stat()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		modTime, err := modTimeFromInfo(i, buildTime)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("ETag", etag(i.Size(), modTime))

		// ServeContent will automatically set the content-type header for files
		// from the extension of "name", and will also set the Last-Modified header
		// from the provided time.
		http.ServeContent(w, r, i.Name(), modTime, content)
	}

	return http.HandlerFunc(fn)
}

// openAsset attempts to open the asset by name in the given directory, falling
// back to the default file if the named asset can't be found. Returns an error
// if even the default asset can't be opened.
func openAsset(fileOpener http.FileSystem, name string) (fs.File, bool, error) {
	var fallback bool

	f, err := fileOpener.Open(name)
	if err != nil {
		if os.IsNotExist(err) {
			fallback = true
			f, err = fileOpener.Open(defaultFile)
		}
		if err != nil {
			return nil, fallback, err
		}
	}

	return f, fallback, nil
}

// modTimeFromInfo gets the modification time from an fs.FileInfo. If this
// modification time is time.Time{}, it falls back to the time returned by
// timeFunc. The modification time will only be time.Time{} if using assets
// embedded with go:embed.
func modTimeFromInfo(i fs.FileInfo, timeFunc func() (time.Time, error)) (time.Time, error) {
	modTime := i.ModTime()
	if modTime.IsZero() {
		return timeFunc()
	}

	return modTime, nil
}

// etag calculates an etag string from the provided file size and modification
// time.
func etag(s int64, mt time.Time) string {
	hour, minute, second := mt.Clock()
	return fmt.Sprintf(`"%d%d%d%d%d"`, s, mt.Day(), hour, minute, second)
}

func buildTime() (time.Time, error) {
	return time.Parse(time.RFC3339, platform.GetBuildInfo().Date)
}

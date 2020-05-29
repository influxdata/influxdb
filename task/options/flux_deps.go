package options

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/influxdata/flux/dependencies/filesystem"
	fluxhttp "github.com/influxdata/flux/dependencies/http"
	"github.com/influxdata/flux/dependencies/secret"
	fluxurl "github.com/influxdata/flux/dependencies/url"
)

var (
	_ fluxhttp.Client    = httpClient{}
	_ filesystem.Service = fileSystem{}
	_ secret.Service     = secretService{}
	_ fluxurl.Validator  = urlValidator{}
)

type httpClient struct{}

func (httpClient) Do(*http.Request) (*http.Response, error) {
	return &http.Response{
		Body: ioutil.NopCloser(&bytes.Reader{}),
	}, nil
}

type secretService struct{}

func (secretService) LoadSecret(ctx context.Context, k string) (string, error) { return "", nil }

type urlValidator struct{}

func (urlValidator) Validate(*url.URL) error { return nil }

type fileSystem struct{}

func (fileSystem) Open(_ string) (filesystem.File, error)   { return file{}, nil }
func (fileSystem) Create(_ string) (filesystem.File, error) { return file{}, nil }
func (fileSystem) Stat(_ string) (os.FileInfo, error)       { return nil, nil }

type file struct{}

func (file) Read(_ []byte) (int, error)         { return 0, nil }
func (file) Write(_ []byte) (int, error)        { return 0, nil }
func (file) Close() error                       { return nil }
func (file) Seek(_ int64, _ int) (int64, error) { return 0, nil }
func (file) Stat() (os.FileInfo, error)         { return info{}, nil }

type info struct{}

func (info) Name() string       { return "" }
func (info) Size() int64        { return 0 }
func (info) Mode() os.FileMode  { return 0 }
func (info) ModTime() time.Time { return time.Now() }
func (info) IsDir() bool        { return false }
func (info) Sys() interface{}   { return nil }

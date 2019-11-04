package pkger_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/influxdata/influxdb/http"
	"github.com/influxdata/influxdb/pkger"
	"github.com/jsteenb2/testttp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHTTPServer(t *testing.T) {
	t.Run("create pkg", func(t *testing.T) {
		t.Run("should successfully return with valid req body", func(t *testing.T) {
			svr := pkger.NewHTTPServer(http.ErrorHandler(0), new(pkger.Service))

			body := newReqBody(t, pkger.ReqCreatePkg{
				PkgName:        "name1",
				PkgDescription: "desc1",
				PkgVersion:     "v1",
			})

			testttp.POST(t, svr, "/api/v2/packages", body,
				testttp.StatusOK(),
				testttp.Resp(func(t *testing.T, w *httptest.ResponseRecorder) {
					var resp pkger.RespCreatePkg
					decodeBody(t, w.Body, &resp)

					pkg := resp.Package
					assert.Equal(t, pkger.APIVersion, pkg.APIVersion)
					assert.Equal(t, "package", pkg.Kind)

					meta := pkg.Metadata
					assert.Equal(t, "name1", meta.Name)
					assert.Equal(t, "desc1", meta.Description)
					assert.Equal(t, "v1", meta.Version)

					assert.NotNil(t, pkg.Spec.Resources)
				}),
			)
		})
	})
}

func decodeBody(t *testing.T, r io.Reader, v interface{}) {
	t.Helper()

	if err := json.NewDecoder(r).Decode(v); err != nil {
		require.FailNow(t, err.Error())
	}
}

func newReqBody(t *testing.T, v interface{}) *bytes.Buffer {
	t.Helper()

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(v); err != nil {
		require.FailNow(t, "unexpected json encoding error", err)
	}
	return &buf
}

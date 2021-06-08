package transport

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func newTestServer(t *testing.T) (*httptest.Server, *mock.MockAnnotationService) {
	ctrlr := gomock.NewController(t)
	svc := mock.NewMockAnnotationService(ctrlr)
	server := NewAnnotationHandler(zaptest.NewLogger(t), svc)
	return httptest.NewServer(server), svc
}

func newTestRequest(t *testing.T, method, path string, body interface{}) *http.Request {
	dat, err := json.Marshal(body)
	require.NoError(t, err)

	req, err := http.NewRequest(method, path, bytes.NewBuffer(dat))
	require.NoError(t, err)

	req.Header.Add("Content-Type", "application/json")

	return req
}

func doTestRequest(t *testing.T, req *http.Request, wantCode int, needJSON bool) *http.Response {
	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, wantCode, res.StatusCode)
	if needJSON {
		require.Equal(t, "application/json; charset=utf-8", res.Header.Get("Content-Type"))
	}
	return res
}

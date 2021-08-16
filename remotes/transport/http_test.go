package transport

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/feature"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/stretchr/testify/assert"
	tmock "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

var (
	orgStr         = "1234123412341234"
	orgID, _       = platform.IDFromString(orgStr)
	remoteOrgStr   = "9876987698769876"
	remoteOrgID, _ = platform.IDFromString(remoteOrgStr)
	idStr          = "4321432143214321"
	id, _          = platform.IDFromString(idStr)
	testConn       = influxdb.RemoteConnection{
		ID:               *id,
		OrgID:            *orgID,
		Name:             "example",
		RemoteURL:        "https://influxdb.cloud",
		RemoteOrgID:      *remoteOrgID,
		AllowInsecureTLS: true,
	}
)

func TestRemoteConnectionHandler(t *testing.T) {
	t.Run("get remotes happy path", func(t *testing.T) {
		ts, svc := newTestServer(t)
		defer ts.Close()

		req := newTestRequest(t, "GET", ts.URL, nil)

		q := req.URL.Query()
		q.Add("orgID", orgStr)
		q.Add("name", testConn.Name)
		q.Add("remoteURL", testConn.RemoteURL)
		req.URL.RawQuery = q.Encode()

		expected := influxdb.RemoteConnections{Remotes: []influxdb.RemoteConnection{testConn}}

		svc.EXPECT().
			ListRemoteConnections(gomock.Any(), tmock.MatchedBy(func(in influxdb.RemoteConnectionListFilter) bool {
				return assert.Equal(t, *orgID, in.OrgID) &&
					assert.Equal(t, testConn.Name, *in.Name) &&
					assert.Equal(t, testConn.RemoteURL, *in.RemoteURL)
			})).Return(&expected, nil)

		res := doTestRequest(t, req, http.StatusOK, true)

		var got influxdb.RemoteConnections
		require.NoError(t, json.NewDecoder(res.Body).Decode(&got))
		require.Equal(t, expected, got)
	})

	t.Run("create remote happy path", func(t *testing.T) {
		ts, svc := newTestServer(t)
		defer ts.Close()

		body := influxdb.CreateRemoteConnectionRequest{
			OrgID:            testConn.OrgID,
			Name:             testConn.Name,
			RemoteURL:        testConn.RemoteURL,
			RemoteToken:      "my super secret token",
			RemoteOrgID:      testConn.RemoteOrgID,
			AllowInsecureTLS: testConn.AllowInsecureTLS,
		}

		req := newTestRequest(t, "POST", ts.URL, &body)

		svc.EXPECT().CreateRemoteConnection(gomock.Any(), body).Return(&testConn, nil)

		res := doTestRequest(t, req, http.StatusCreated, true)

		var got influxdb.RemoteConnection
		require.NoError(t, json.NewDecoder(res.Body).Decode(&got))
		require.Equal(t, testConn, got)
	})

	t.Run("dry-run create happy path", func(t *testing.T) {
		ts, svc := newTestServer(t)
		defer ts.Close()

		body := influxdb.CreateRemoteConnectionRequest{
			OrgID:            testConn.OrgID,
			Name:             testConn.Name,
			RemoteURL:        testConn.RemoteURL,
			RemoteToken:      "my super secret token",
			RemoteOrgID:      testConn.RemoteOrgID,
			AllowInsecureTLS: testConn.AllowInsecureTLS,
		}

		req := newTestRequest(t, "POST", ts.URL, &body)
		q := req.URL.Query()
		q.Add("dryRun", "true")
		req.URL.RawQuery = q.Encode()

		svc.EXPECT().ValidateNewRemoteConnection(gomock.Any(), body).Return(nil)

		doTestRequest(t, req, http.StatusNoContent, false)
	})

	t.Run("get remote happy path", func(t *testing.T) {
		ts, svc := newTestServer(t)
		defer ts.Close()

		req := newTestRequest(t, "GET", ts.URL+"/"+id.String(), nil)

		svc.EXPECT().GetRemoteConnection(gomock.Any(), *id).Return(&testConn, nil)

		res := doTestRequest(t, req, http.StatusOK, true)

		var got influxdb.RemoteConnection
		require.NoError(t, json.NewDecoder(res.Body).Decode(&got))
		require.Equal(t, testConn, got)
	})

	t.Run("delete remote happy path", func(t *testing.T) {
		ts, svc := newTestServer(t)
		defer ts.Close()

		req := newTestRequest(t, "DELETE", ts.URL+"/"+id.String(), nil)

		svc.EXPECT().DeleteRemoteConnection(gomock.Any(), *id).Return(nil)

		doTestRequest(t, req, http.StatusNoContent, false)
	})

	t.Run("update remote happy path", func(t *testing.T) {
		ts, svc := newTestServer(t)
		defer ts.Close()

		newToken := "a new even more secret token"
		body := influxdb.UpdateRemoteConnectionRequest{RemoteToken: &newToken}

		req := newTestRequest(t, "PATCH", ts.URL+"/"+id.String(), &body)

		svc.EXPECT().UpdateRemoteConnection(gomock.Any(), *id, body).Return(&testConn, nil)

		res := doTestRequest(t, req, http.StatusOK, true)

		var got influxdb.RemoteConnection
		require.NoError(t, json.NewDecoder(res.Body).Decode(&got))
		require.Equal(t, testConn, got)
	})

	t.Run("dry-run update happy path", func(t *testing.T) {
		ts, svc := newTestServer(t)
		defer ts.Close()

		newToken := "a new even more secret token"
		body := influxdb.UpdateRemoteConnectionRequest{RemoteToken: &newToken}

		req := newTestRequest(t, "PATCH", ts.URL+"/"+id.String(), &body)
		q := req.URL.Query()
		q.Add("dryRun", "true")
		req.URL.RawQuery = q.Encode()

		svc.EXPECT().ValidateUpdatedRemoteConnection(gomock.Any(), *id, body).Return(nil)

		doTestRequest(t, req, http.StatusNoContent, false)
	})

	t.Run("validate remote happy path", func(t *testing.T) {
		ts, svc := newTestServer(t)
		defer ts.Close()

		req := newTestRequest(t, "POST", ts.URL+"/"+id.String()+"/validate", nil)

		svc.EXPECT().ValidateRemoteConnection(gomock.Any(), *id).Return(nil)

		doTestRequest(t, req, http.StatusNoContent, false)
	})

	t.Run("invalid remote IDs return 400", func(t *testing.T) {
		ts, _ := newTestServer(t)
		defer ts.Close()

		req1 := newTestRequest(t, "GET", ts.URL+"/foo", nil)
		req2 := newTestRequest(t, "PATCH", ts.URL+"/foo", &influxdb.UpdateRemoteConnectionRequest{})
		req3 := newTestRequest(t, "DELETE", ts.URL+"/foo", nil)

		for _, req := range []*http.Request{req1, req2, req3} {
			t.Run(req.Method, func(t *testing.T) {
				doTestRequest(t, req, http.StatusBadRequest, true)
			})
		}
	})

	t.Run("invalid org ID to GET /remotes returns 400", func(t *testing.T) {
		ts, _ := newTestServer(t)
		defer ts.Close()

		req := newTestRequest(t, "GET", ts.URL, nil)
		q := req.URL.Query()
		q.Add("orgID", "foo")
		req.URL.RawQuery = q.Encode()

		doTestRequest(t, req, http.StatusBadRequest, true)
	})

	t.Run("invalid request bodies return 400", func(t *testing.T) {
		ts, _ := newTestServer(t)
		defer ts.Close()

		body := "o no not an object"
		req1 := newTestRequest(t, "POST", ts.URL, &body)
		req2 := newTestRequest(t, "PATCH", ts.URL+"/"+id.String(), &body)

		for _, req := range []*http.Request{req1, req2} {
			t.Run(req.Method, func(t *testing.T) {
				doTestRequest(t, req, http.StatusBadRequest, true)
			})
		}
	})
}

func newTestServer(t *testing.T) (*httptest.Server, *mock.MockRemoteConnectionService) {
	ctrlr := gomock.NewController(t)
	svc := mock.NewMockRemoteConnectionService(ctrlr)
	server := annotatedTestServer(NewRemoteConnectionHandler(zaptest.NewLogger(t), svc))
	return httptest.NewServer(server), svc
}

func annotatedTestServer(serv http.Handler) http.Handler {
	replicationFlag := feature.MakeFlag("", feature.ReplicationStreamBackend().Key(), "", true, 0, true)

	return feature.NewHandler(
		zap.NewNop(),
		feature.DefaultFlagger(),
		[]feature.Flag{replicationFlag},
		serv,
	)
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

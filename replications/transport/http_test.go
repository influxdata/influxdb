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
	"github.com/influxdata/influxdb/v2/replications/mock"
	"github.com/stretchr/testify/assert"
	tmock "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

var (
	orgStr            = "1234123412341234"
	orgID, _          = platform.IDFromString(orgStr)
	remoteStr         = "9876987698769876"
	remoteID, _       = platform.IDFromString(remoteStr)
	idStr             = "4321432143214321"
	id, _             = platform.IDFromString(idStr)
	localBucketStr    = "1111111111111111"
	localBucketId, _  = platform.IDFromString(localBucketStr)
	remoteBucketStr   = "1234567887654321"
	remoteBucketID, _ = platform.IDFromString(remoteBucketStr)
	testReplication   = influxdb.Replication{
		ID:                *id,
		OrgID:             *orgID,
		RemoteID:          *remoteID,
		LocalBucketID:     *localBucketId,
		RemoteBucketID:    *remoteBucketID,
		Name:              "example",
		MaxQueueSizeBytes: influxdb.DefaultReplicationMaxQueueSizeBytes,
	}
)

func TestReplicationHandler(t *testing.T) {
	t.Run("get replications happy path", func(t *testing.T) {
		ts, svc := newTestServer(t)
		defer ts.Close()

		req := newTestRequest(t, "GET", ts.URL, nil)

		q := req.URL.Query()
		q.Add("orgID", orgStr)
		q.Add("name", testReplication.Name)
		q.Add("remoteID", remoteStr)
		q.Add("localBucketID", localBucketStr)
		req.URL.RawQuery = q.Encode()

		expected := influxdb.Replications{Replications: []influxdb.Replication{testReplication}}

		svc.EXPECT().
			ListReplications(gomock.Any(), tmock.MatchedBy(func(in influxdb.ReplicationListFilter) bool {
				return assert.Equal(t, *orgID, in.OrgID) &&
					assert.Equal(t, testReplication.Name, *in.Name) &&
					assert.Equal(t, testReplication.RemoteID, *in.RemoteID) &&
					assert.Equal(t, testReplication.LocalBucketID, *in.LocalBucketID)
			})).Return(&expected, nil)

		res := doTestRequest(t, req, http.StatusOK, true)

		var got influxdb.Replications
		require.NoError(t, json.NewDecoder(res.Body).Decode(&got))
		require.Equal(t, expected, got)
	})

	t.Run("create replication happy path", func(t *testing.T) {

		body := influxdb.CreateReplicationRequest{
			OrgID:          testReplication.OrgID,
			Name:           testReplication.Name,
			RemoteID:       testReplication.RemoteID,
			LocalBucketID:  testReplication.LocalBucketID,
			RemoteBucketID: testReplication.RemoteBucketID,
		}

		t.Run("with explicit queue size", func(t *testing.T) {
			ts, svc := newTestServer(t)
			defer ts.Close()

			body := body
			body.MaxQueueSizeBytes = 2 * influxdb.DefaultReplicationMaxQueueSizeBytes

			req := newTestRequest(t, "POST", ts.URL, &body)

			svc.EXPECT().CreateReplication(gomock.Any(), body).Return(&testReplication, nil)

			res := doTestRequest(t, req, http.StatusCreated, true)

			var got influxdb.Replication
			require.NoError(t, json.NewDecoder(res.Body).Decode(&got))
			require.Equal(t, testReplication, got)
		})

		t.Run("with default queue size", func(t *testing.T) {
			ts, svc := newTestServer(t)
			defer ts.Close()

			req := newTestRequest(t, "POST", ts.URL, &body)

			expectedBody := body
			expectedBody.MaxQueueSizeBytes = influxdb.DefaultReplicationMaxQueueSizeBytes

			svc.EXPECT().CreateReplication(gomock.Any(), expectedBody).Return(&testReplication, nil)

			res := doTestRequest(t, req, http.StatusCreated, true)

			var got influxdb.Replication
			require.NoError(t, json.NewDecoder(res.Body).Decode(&got))
			require.Equal(t, testReplication, got)
		})
	})

	t.Run("dry-run create happy path", func(t *testing.T) {

		body := influxdb.CreateReplicationRequest{
			OrgID:          testReplication.OrgID,
			Name:           testReplication.Name,
			RemoteID:       testReplication.RemoteID,
			LocalBucketID:  testReplication.LocalBucketID,
			RemoteBucketID: testReplication.RemoteBucketID,
		}

		t.Run("with explicit queue size", func(t *testing.T) {
			ts, svc := newTestServer(t)
			defer ts.Close()

			body := body
			body.MaxQueueSizeBytes = 2 * influxdb.DefaultReplicationMaxQueueSizeBytes

			req := newTestRequest(t, "POST", ts.URL, &body)
			q := req.URL.Query()
			q.Add("validate", "true")
			req.URL.RawQuery = q.Encode()

			svc.EXPECT().ValidateNewReplication(gomock.Any(), body).Return(nil)

			doTestRequest(t, req, http.StatusNoContent, false)
		})

		t.Run("with default queue size", func(t *testing.T) {
			ts, svc := newTestServer(t)
			defer ts.Close()

			req := newTestRequest(t, "POST", ts.URL, &body)
			q := req.URL.Query()
			q.Add("validate", "true")
			req.URL.RawQuery = q.Encode()

			expectedBody := body
			expectedBody.MaxQueueSizeBytes = influxdb.DefaultReplicationMaxQueueSizeBytes

			svc.EXPECT().ValidateNewReplication(gomock.Any(), expectedBody).Return(nil)

			doTestRequest(t, req, http.StatusNoContent, false)
		})
	})

	t.Run("get replication happy path", func(t *testing.T) {
		ts, svc := newTestServer(t)
		defer ts.Close()

		req := newTestRequest(t, "GET", ts.URL+"/"+id.String(), nil)

		svc.EXPECT().GetReplication(gomock.Any(), *id).Return(&testReplication, nil)

		res := doTestRequest(t, req, http.StatusOK, true)

		var got influxdb.Replication
		require.NoError(t, json.NewDecoder(res.Body).Decode(&got))
		require.Equal(t, testReplication, got)
	})

	t.Run("delete replication happy path", func(t *testing.T) {
		ts, svc := newTestServer(t)
		defer ts.Close()

		req := newTestRequest(t, "DELETE", ts.URL+"/"+id.String(), nil)

		svc.EXPECT().DeleteReplication(gomock.Any(), *id).Return(nil)

		doTestRequest(t, req, http.StatusNoContent, false)
	})

	t.Run("update replication happy path", func(t *testing.T) {
		ts, svc := newTestServer(t)
		defer ts.Close()

		newDescription := "my cool replication"
		newQueueSize := 3 * influxdb.DefaultReplicationMaxQueueSizeBytes
		body := influxdb.UpdateReplicationRequest{Description: &newDescription, MaxQueueSizeBytes: &newQueueSize}

		req := newTestRequest(t, "PATCH", ts.URL+"/"+id.String(), body)

		svc.EXPECT().UpdateReplication(gomock.Any(), *id, body).Return(&testReplication, nil)

		res := doTestRequest(t, req, http.StatusOK, true)

		var got influxdb.Replication
		require.NoError(t, json.NewDecoder(res.Body).Decode(&got))
		require.Equal(t, testReplication, got)
	})

	t.Run("dry-run update happy path", func(t *testing.T) {
		ts, svc := newTestServer(t)
		defer ts.Close()

		newDescription := "my cool replication"
		newQueueSize := 3 * influxdb.DefaultReplicationMaxQueueSizeBytes
		body := influxdb.UpdateReplicationRequest{Description: &newDescription, MaxQueueSizeBytes: &newQueueSize}

		req := newTestRequest(t, "PATCH", ts.URL+"/"+id.String(), body)
		q := req.URL.Query()
		q.Add("validate", "true")
		req.URL.RawQuery = q.Encode()

		svc.EXPECT().ValidateUpdatedReplication(gomock.Any(), *id, body).Return(nil)

		doTestRequest(t, req, http.StatusNoContent, false)
	})

	t.Run("validate replication happy path", func(t *testing.T) {
		ts, svc := newTestServer(t)
		defer ts.Close()

		req := newTestRequest(t, "POST", ts.URL+"/"+id.String()+"/validate", nil)

		svc.EXPECT().ValidateReplication(gomock.Any(), *id).Return(nil)

		doTestRequest(t, req, http.StatusNoContent, false)
	})

	t.Run("invalid replication IDs return 400", func(t *testing.T) {
		ts, _ := newTestServer(t)
		defer ts.Close()

		req1 := newTestRequest(t, "GET", ts.URL+"/foo", nil)
		req2 := newTestRequest(t, "PATCH", ts.URL+"/foo", &influxdb.UpdateReplicationRequest{})
		req3 := newTestRequest(t, "DELETE", ts.URL+"/foo", nil)

		for _, req := range []*http.Request{req1, req2, req3} {
			t.Run(req.Method, func(t *testing.T) {
				doTestRequest(t, req, http.StatusBadRequest, true)
			})
		}
	})

	t.Run("invalid org ID to GET /replications returns 400", func(t *testing.T) {
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

	t.Run("too-small queue size on create is rejected", func(t *testing.T) {
		ts, _ := newTestServer(t)
		defer ts.Close()

		body := influxdb.CreateReplicationRequest{
			OrgID:             testReplication.OrgID,
			Name:              testReplication.Name,
			RemoteID:          testReplication.RemoteID,
			LocalBucketID:     testReplication.LocalBucketID,
			RemoteBucketID:    testReplication.RemoteBucketID,
			MaxQueueSizeBytes: influxdb.MinReplicationMaxQueueSizeBytes / 2,
		}

		req := newTestRequest(t, "POST", ts.URL, &body)

		doTestRequest(t, req, http.StatusBadRequest, true)
	})

	t.Run("too-small queue size on update is rejected", func(t *testing.T) {
		ts, _ := newTestServer(t)
		defer ts.Close()

		newSize := influxdb.MinReplicationMaxQueueSizeBytes / 2
		body := influxdb.UpdateReplicationRequest{MaxQueueSizeBytes: &newSize}

		req := newTestRequest(t, "PATCH", ts.URL+"/"+id.String(), &body)

		doTestRequest(t, req, http.StatusBadRequest, true)
	})
}

func newTestServer(t *testing.T) (*httptest.Server, *mock.MockReplicationService) {
	ctrl := gomock.NewController(t)
	svc := mock.NewMockReplicationService(ctrl)
	server := annotatedTestServer(newReplicationHandler(zaptest.NewLogger(t), svc))
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

package transport

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/influxdata/influxdb/v2"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"github.com/stretchr/testify/require"
)

var (
	testCreateStream = influxdb.Stream{
		Name: "test stream",
	}

	testReadStream1 = &influxdb.ReadStream{
		ID:        *influxdbtesting.IDPtr(1),
		Name:      "test stream 1",
		CreatedAt: now,
		UpdatedAt: now,
	}

	testReadStream2 = &influxdb.ReadStream{
		ID:        *influxdbtesting.IDPtr(2),
		Name:      "test stream 2",
		CreatedAt: now,
		UpdatedAt: now,
	}
)

func TestStreamsRouter(t *testing.T) {
	t.Parallel()

	t.Run("create or update stream happy path", func(t *testing.T) {
		ts, svc := newTestServer(t)
		defer ts.Close()

		req := newTestRequest(t, "PUT", ts.URL+"/streams", testCreateStream)

		q := req.URL.Query()
		q.Add("orgID", orgStr)
		req.URL.RawQuery = q.Encode()

		svc.EXPECT().
			CreateOrUpdateStream(gomock.Any(), *orgID, testCreateStream).
			Return(testReadStream1, nil)

		res := doTestRequest(t, req, http.StatusOK, true)

		got := &influxdb.ReadStream{}
		err := json.NewDecoder(res.Body).Decode(got)
		require.NoError(t, err)
		require.Equal(t, testReadStream1, got)
	})

	t.Run("get streams happy path", func(t *testing.T) {
		ts, svc := newTestServer(t)
		defer ts.Close()

		req := newTestRequest(t, "GET", ts.URL+"/streams", nil)

		q := req.URL.Query()
		q.Add("orgID", orgStr)
		q.Add("endTime", now.Format(time.RFC3339))
		q.Add("streamIncludes", "stream1")
		q.Add("streamIncludes", "stream2")
		req.URL.RawQuery = q.Encode()

		want := []influxdb.ReadStream{*testReadStream1, *testReadStream2}

		svc.EXPECT().
			ListStreams(gomock.Any(), *orgID, influxdb.StreamListFilter{
				StreamIncludes: []string{"stream1", "stream2"},
				BasicFilter: influxdb.BasicFilter{
					StartTime: &time.Time{},
					EndTime:   &now,
				},
			}).
			Return(want, nil)

		res := doTestRequest(t, req, http.StatusOK, true)

		got := []influxdb.ReadStream{}
		err := json.NewDecoder(res.Body).Decode(&got)
		require.NoError(t, err)
		require.ElementsMatch(t, want, got)
	})

	t.Run("delete streams (by name) happy path", func(t *testing.T) {
		ts, svc := newTestServer(t)
		defer ts.Close()

		req := newTestRequest(t, "DELETE", ts.URL+"/streams", nil)
		q := req.URL.Query()
		q.Add("orgID", orgStr)
		q.Add("stream", "stream1")
		q.Add("stream", "stream2")
		req.URL.RawQuery = q.Encode()

		svc.EXPECT().
			DeleteStreams(gomock.Any(), *orgID, influxdb.BasicStream{
				Names: []string{"stream1", "stream2"},
			}).
			Return(nil)

		doTestRequest(t, req, http.StatusNoContent, false)
	})

	t.Run("delete stream happy path", func(t *testing.T) {
		ts, svc := newTestServer(t)
		defer ts.Close()

		req := newTestRequest(t, "DELETE", ts.URL+"/streams/"+idStr, nil)

		svc.EXPECT().
			DeleteStreamByID(gomock.Any(), *id).
			Return(nil)

		doTestRequest(t, req, http.StatusNoContent, false)
	})

	t.Run("update stream by id happy path", func(t *testing.T) {
		ts, svc := newTestServer(t)
		defer ts.Close()

		req := newTestRequest(t, "PUT", ts.URL+"/streams/"+idStr, testCreateStream)

		svc.EXPECT().
			UpdateStream(gomock.Any(), *id, testCreateStream).
			Return(testReadStream1, nil)

		res := doTestRequest(t, req, http.StatusOK, true)

		got := &influxdb.ReadStream{}
		err := json.NewDecoder(res.Body).Decode(got)
		require.NoError(t, err)
		require.Equal(t, testReadStream1, got)
	})

	t.Run("invalid org ids return 400 when required", func(t *testing.T) {
		methods := []string{"GET", "PUT", "DELETE"}

		for _, m := range methods {
			t.Run(m, func(t *testing.T) {
				ts, _ := newTestServer(t)
				defer ts.Close()

				req := newTestRequest(t, m, ts.URL+"/streams", nil)
				q := req.URL.Query()
				q.Add("orgID", "badid")
				req.URL.RawQuery = q.Encode()

				doTestRequest(t, req, http.StatusBadRequest, false)
			})
		}
	})

	t.Run("invalid stream ids return 400 when required", func(t *testing.T) {
		methods := []string{"DELETE", "PUT"}

		for _, m := range methods {
			t.Run(m, func(t *testing.T) {
				ts, _ := newTestServer(t)
				defer ts.Close()

				req := newTestRequest(t, m, ts.URL+"/streams/badID", nil)
				doTestRequest(t, req, http.StatusBadRequest, false)
			})
		}
	})
}

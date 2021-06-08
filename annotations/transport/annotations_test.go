package transport

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"github.com/stretchr/testify/require"
)

var (
	orgStr   = "1234123412341234"
	orgID, _ = platform.IDFromString(orgStr)
	idStr    = "4321432143214321"
	id, _    = platform.IDFromString(idStr)
	now      = time.Now().UTC().Truncate(time.Second)
	later    = now.Add(5 * time.Minute)

	testCreate = influxdb.AnnotationCreate{
		StreamTag: "sometag",
		Summary:   "testing the api",
		EndTime:   &now,
		StartTime: &now,
	}

	testEvent = influxdb.AnnotationEvent{
		ID:               *id,
		AnnotationCreate: testCreate,
	}

	testRead1 = influxdb.ReadAnnotation{
		ID: *influxdbtesting.IDPtr(1),
	}

	testRead2 = influxdb.ReadAnnotation{
		ID: *influxdbtesting.IDPtr(2),
	}
)

func TestAnnotationHandler(t *testing.T) {
	t.Parallel()

	t.Run("get annotations happy path", func(t *testing.T) {
		ts, svc := newTestServer(t)
		defer ts.Close()

		now := time.Now().UTC().Truncate(time.Second)

		req := newTestRequest(t, "GET", ts.URL+"/annotations", nil)

		q := req.URL.Query()
		q.Add("orgID", orgStr)
		q.Add("endTime", now.Format(time.RFC3339))
		// the remaining query params are not necessary for this test, but intended as a reference for usage
		q.Add("stickerIncludes[product]", "oss")
		q.Add("stickerIncludes[env]", "dev")
		q.Add("streamIncludes", "stream1")
		q.Add("streamIncludes", "stream2")
		req.URL.RawQuery = q.Encode()

		want := []influxdb.AnnotationList{
			{
				StreamTag:   "stream1",
				Annotations: []influxdb.ReadAnnotation{testRead1},
			},
			{
				StreamTag:   "stream2",
				Annotations: []influxdb.ReadAnnotation{testRead2},
			},
		}

		svc.EXPECT().
			ListAnnotations(gomock.Any(), *orgID, influxdb.AnnotationListFilter{
				StickerIncludes: map[string]string{"product": "oss", "env": "dev"},
				StreamIncludes:  []string{"stream1", "stream2"},
				BasicFilter: influxdb.BasicFilter{
					StartTime: &time.Time{},
					EndTime:   &now,
				},
			}).
			Return(influxdb.ReadAnnotations{
				"stream1": []influxdb.ReadAnnotation{testRead1},
				"stream2": []influxdb.ReadAnnotation{testRead2},
			}, nil)

		res := doTestRequest(t, req, http.StatusOK, true)

		got := []influxdb.AnnotationList{}
		err := json.NewDecoder(res.Body).Decode(&got)
		require.NoError(t, err)
		require.ElementsMatch(t, want, got)
	})

	t.Run("create annotations happy path", func(t *testing.T) {
		ts, svc := newTestServer(t)
		defer ts.Close()

		createAnnotations := []influxdb.AnnotationCreate{testCreate}

		req := newTestRequest(t, "POST", ts.URL+"/annotations", createAnnotations)

		q := req.URL.Query()
		q.Add("orgID", orgStr)
		req.URL.RawQuery = q.Encode()

		want := []influxdb.AnnotationEvent{testEvent}

		svc.EXPECT().
			CreateAnnotations(gomock.Any(), *orgID, createAnnotations).
			Return(want, nil)

		res := doTestRequest(t, req, http.StatusOK, true)

		got := []influxdb.AnnotationEvent{}
		err := json.NewDecoder(res.Body).Decode(&got)
		require.NoError(t, err)
		require.Equal(t, want, got)
	})

	t.Run("delete annotations happy path", func(t *testing.T) {
		ts, svc := newTestServer(t)
		defer ts.Close()

		req := newTestRequest(t, "DELETE", ts.URL+"/annotations", nil)
		q := req.URL.Query()
		q.Add("orgID", orgStr)
		q.Add("stream", "someTag")
		q.Add("startTime", now.Format(time.RFC3339))
		q.Add("endTime", later.Format(time.RFC3339))
		req.URL.RawQuery = q.Encode()

		svc.EXPECT().
			DeleteAnnotations(gomock.Any(), *orgID, influxdb.AnnotationDeleteFilter{
				StreamTag: "someTag",
				StartTime: &now,
				EndTime:   &later,
				Stickers:  map[string]string{},
			}).
			Return(nil)

		doTestRequest(t, req, http.StatusNoContent, false)
	})

	t.Run("get annotation happy path", func(t *testing.T) {
		ts, svc := newTestServer(t)
		defer ts.Close()

		req := newTestRequest(t, "GET", ts.URL+"/annotations/"+idStr, nil)

		svc.EXPECT().
			GetAnnotation(gomock.Any(), *id).
			Return(&testEvent, nil)

		res := doTestRequest(t, req, http.StatusOK, true)

		got := &influxdb.AnnotationEvent{}
		err := json.NewDecoder(res.Body).Decode(got)
		require.NoError(t, err)
		require.Equal(t, &testEvent, got)
	})

	t.Run("delete annotation happy path", func(t *testing.T) {
		ts, svc := newTestServer(t)
		defer ts.Close()

		req := newTestRequest(t, "DELETE", ts.URL+"/annotations/"+idStr, nil)

		svc.EXPECT().
			DeleteAnnotation(gomock.Any(), *id).
			Return(nil)

		doTestRequest(t, req, http.StatusNoContent, false)
	})

	t.Run("update annotation happy path", func(t *testing.T) {
		ts, svc := newTestServer(t)
		defer ts.Close()

		req := newTestRequest(t, "PUT", ts.URL+"/annotations/"+idStr, testCreate)

		svc.EXPECT().
			UpdateAnnotation(gomock.Any(), *id, testCreate).
			Return(&testEvent, nil)

		res := doTestRequest(t, req, http.StatusOK, true)

		got := &influxdb.AnnotationEvent{}
		err := json.NewDecoder(res.Body).Decode(got)
		require.NoError(t, err)
		require.Equal(t, &testEvent, got)
	})

	t.Run("invalid org ids return 400 when required", func(t *testing.T) {
		methods := []string{"POST", "GET", "DELETE"}

		for _, m := range methods {
			t.Run(m, func(t *testing.T) {
				ts, _ := newTestServer(t)
				defer ts.Close()

				req := newTestRequest(t, m, ts.URL+"/annotations", nil)
				q := req.URL.Query()
				q.Add("orgID", "badid")
				req.URL.RawQuery = q.Encode()

				doTestRequest(t, req, http.StatusBadRequest, false)
			})
		}
	})

	t.Run("invalid annotation ids return 400 when required", func(t *testing.T) {
		methods := []string{"GET", "DELETE", "PUT"}

		for _, m := range methods {
			t.Run(m, func(t *testing.T) {
				ts, _ := newTestServer(t)
				defer ts.Close()

				req := newTestRequest(t, m, ts.URL+"/annotations/badID", nil)
				doTestRequest(t, req, http.StatusBadRequest, false)
			})
		}
	})
}

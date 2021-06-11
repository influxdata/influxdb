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
	testCreateAnnotation = influxdb.AnnotationCreate{
		StreamTag: "sometag",
		Summary:   "testing the api",
		Message:   "stored annotation message",
		Stickers:  map[string]string{"val1": "sticker1", "val2": "sticker2"},
		EndTime:   &now,
		StartTime: &now,
	}

	testEvent = influxdb.AnnotationEvent{
		ID:               *id,
		AnnotationCreate: testCreateAnnotation,
	}

	testReadAnnotation1 = influxdb.ReadAnnotation{
		ID: *influxdbtesting.IDPtr(1),
	}

	testReadAnnotation2 = influxdb.ReadAnnotation{
		ID: *influxdbtesting.IDPtr(2),
	}

	testStoredAnnotation = influxdb.StoredAnnotation{
		ID:        *id,
		OrgID:     *orgID,
		StreamID:  *influxdbtesting.IDPtr(3),
		StreamTag: "sometag",
		Summary:   "testing the api",
		Message:   "stored annotation message",
		Stickers:  []string{"val1=sticker1", "val2=sticker2"},
		Lower:     now.Format(time.RFC3339),
		Upper:     now.Format(time.RFC3339),
	}

	testReadAnnotations = influxdb.ReadAnnotations{
		"sometag": []influxdb.ReadAnnotation{
			{
				ID:        testStoredAnnotation.ID,
				Summary:   testStoredAnnotation.Summary,
				Message:   testStoredAnnotation.Message,
				Stickers:  map[string]string{"val1": "sticker1", "val2": "sticker2"},
				EndTime:   testStoredAnnotation.Lower,
				StartTime: testStoredAnnotation.Upper,
			},
		},
	}
)

func TestAnnotationRouter(t *testing.T) {
	t.Parallel()

	t.Run("get annotations happy path", func(t *testing.T) {
		ts, svc := newTestServer(t)
		defer ts.Close()

		req := newTestRequest(t, "GET", ts.URL+"/annotations", nil)

		q := req.URL.Query()
		q.Add("orgID", orgStr)
		q.Add("endTime", now.Format(time.RFC3339))
		q.Add("stickerIncludes[product]", "oss")
		q.Add("stickerIncludes[env]", "dev")
		q.Add("streamIncludes", "stream1")
		q.Add("streamIncludes", "stream2")
		req.URL.RawQuery = q.Encode()

		want := []influxdb.AnnotationList{
			{
				StreamTag:   "stream1",
				Annotations: []influxdb.ReadAnnotation{testReadAnnotation1},
			},
			{
				StreamTag:   "stream2",
				Annotations: []influxdb.ReadAnnotation{testReadAnnotation2},
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
			Return([]influxdb.StoredAnnotation{
				{
					ID:        testReadAnnotation1.ID,
					StreamTag: "stream1",
				},
				{
					ID:        testReadAnnotation2.ID,
					StreamTag: "stream2",
				},
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

		createAnnotations := []influxdb.AnnotationCreate{testCreateAnnotation}

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
			Return(&testStoredAnnotation, nil)

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

		req := newTestRequest(t, "PUT", ts.URL+"/annotations/"+idStr, testCreateAnnotation)

		svc.EXPECT().
			UpdateAnnotation(gomock.Any(), *id, testCreateAnnotation).
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

func TestStoredAnnotationsToReadAnnotations(t *testing.T) {
	t.Parallel()

	got, err := storedAnnotationsToReadAnnotations([]influxdb.StoredAnnotation{testStoredAnnotation})
	require.NoError(t, err)
	require.Equal(t, got, testReadAnnotations)
}

func TestStoredAnnotationToEvent(t *testing.T) {
	t.Parallel()

	got, err := storedAnnotationToEvent(&testStoredAnnotation)
	require.NoError(t, err)
	require.Equal(t, got, &testEvent)
}

func TestStickerSliceToMap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		stickers []string
		want     map[string]string
		wantErr  error
	}{
		{
			"good stickers",
			[]string{"good1=val1", "good2=val2"},
			map[string]string{"good1": "val1", "good2": "val2"},
			nil,
		},
		{
			"bad stickers",
			[]string{"this is an invalid sticker", "shouldbe=likethis"},
			nil,
			invalidStickerError("this is an invalid sticker"),
		},
		{
			"no stickers",
			[]string{},
			map[string]string{},
			nil,
		},
	}

	for _, tt := range tests {
		got, err := stickerSliceToMap(tt.stickers)
		require.Equal(t, tt.want, got)
		require.Equal(t, tt.wantErr, err)
	}
}

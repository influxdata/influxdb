package http

import (
	"context"
	"errors"
	"net/http"
	"time"

	platform "github.com/influxdata/influxdb"
	"github.com/julienschmidt/httprouter"
	"go.uber.org/zap"
)

// UsageHandler represents an HTTP API handler for usages.
type UsageHandler struct {
	*httprouter.Router

	Logger *zap.Logger

	UsageService platform.UsageService
}

// NewUsageHandler returns a new instance of UsageHandler.
func NewUsageHandler() *UsageHandler {
	h := &UsageHandler{
		Router: NewRouter(),
		Logger: zap.NewNop(),
	}

	h.HandlerFunc("GET", "/api/v2/usage", h.handleGetUsage)
	return h
}

// handleGetUsage is the HTTP handler for the GET /api/v2/usage route.
func (h *UsageHandler) handleGetUsage(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeGetUsageRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	b, err := h.UsageService.GetUsage(ctx, req.filter)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, b); err != nil {
		logEncodingError(h.Logger, r, err)
		return
	}
}

type getUsageRequest struct {
	filter platform.UsageFilter
}

func decodeGetUsageRequest(ctx context.Context, r *http.Request) (*getUsageRequest, error) {
	req := &getUsageRequest{}
	qp := r.URL.Query()

	orgID := qp.Get("orgID")
	if orgID != "" {
		var id platform.ID
		if err := (&id).DecodeFromString(orgID); err != nil {
			return nil, err
		}
		req.filter.OrgID = &id
	}

	bucketID := qp.Get("bucketID")
	if bucketID != "" {
		var id platform.ID
		if err := (&id).DecodeFromString(bucketID); err != nil {
			return nil, err
		}
		req.filter.BucketID = &id
	}

	start := qp.Get("start")
	stop := qp.Get("stop")

	if start == "" && stop != "" {
		return nil, errors.New("start query param required")
	}
	if stop == "" && start != "" {
		return nil, errors.New("stop query param required")
	}

	if start == "" && stop == "" {
		now := time.Now()
		month := roundToMonth(now)

		req.filter.Range = &platform.Timespan{
			Start: month,
			Stop:  now,
		}
	}

	if start != "" && stop != "" {
		startTime, err := time.Parse(time.RFC3339, start)
		if err != nil {
			return nil, err
		}

		stopTime, err := time.Parse(time.RFC3339, stop)
		if err != nil {
			return nil, err
		}

		req.filter.Range = &platform.Timespan{
			Start: startTime,
			Stop:  stopTime,
		}
	}

	return req, nil
}

func roundToMonth(t time.Time) time.Time {
	h, m, s := t.Clock()
	d := t.Day()

	delta := (time.Duration(d) * 24 * time.Hour) + time.Duration(h)*time.Hour + time.Duration(m)*time.Minute + time.Duration(s)*time.Second

	return t.Add(-1 * delta).Round(time.Minute)
}

package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/influxdata/httprouter"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	"go.uber.org/zap"
)

// RestoreBackend is all services and associated parameters required to construct the RestoreHandler.
type RestoreBackend struct {
	Logger *zap.Logger
	influxdb.HTTPErrorHandler

	RestoreService influxdb.RestoreService
}

// NewRestoreBackend returns a new instance of RestoreBackend.
func NewRestoreBackend(b *APIBackend) *RestoreBackend {
	return &RestoreBackend{
		Logger: b.Logger.With(zap.String("handler", "restore")),

		HTTPErrorHandler: b.HTTPErrorHandler,
		RestoreService:   b.RestoreService,
	}
}

// RestoreHandler is http handler for restore service.
type RestoreHandler struct {
	*httprouter.Router
	influxdb.HTTPErrorHandler
	Logger *zap.Logger

	RestoreService influxdb.RestoreService
}

const (
	prefixRestore     = "/api/v2/restore"
	restoreKVPath     = prefixRestore + "/kv"
	restoreBucketPath = prefixRestore + "/buckets/:bucketID"
	restoreShardPath  = prefixRestore + "/shards/:shardID"
)

// NewRestoreHandler creates a new handler at /api/v2/restore to receive restore requests.
func NewRestoreHandler(b *RestoreBackend) *RestoreHandler {
	h := &RestoreHandler{
		HTTPErrorHandler: b.HTTPErrorHandler,
		Router:           NewRouter(b.HTTPErrorHandler),
		Logger:           b.Logger,
		RestoreService:   b.RestoreService,
	}

	h.HandlerFunc(http.MethodPost, restoreKVPath, h.handleRestoreKVStore)
	h.HandlerFunc(http.MethodPost, restoreBucketPath, h.handleRestoreBucket)
	h.HandlerFunc(http.MethodPost, restoreShardPath, h.handleRestoreShard)

	return h
}

func (h *RestoreHandler) handleRestoreKVStore(w http.ResponseWriter, r *http.Request) {
	span, r := tracing.ExtractFromHTTPRequest(r, "RestoreHandler.handleRestoreKVStore")
	defer span.Finish()

	ctx := r.Context()

	if err := h.RestoreService.RestoreKVStore(ctx, r.Body); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
}

func (h *RestoreHandler) handleRestoreBucket(w http.ResponseWriter, r *http.Request) {
	span, r := tracing.ExtractFromHTTPRequest(r, "RestoreHandler.handleRestoreBucket")
	defer span.Finish()

	ctx := r.Context()

	// Read bucket ID.
	bucketID, err := decodeIDFromCtx(r.Context(), "bucketID")
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	// Read serialized DBI data.
	buf, err := ioutil.ReadAll(r.Body)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	shardIDMap, err := h.RestoreService.RestoreBucket(ctx, bucketID, buf)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := json.NewEncoder(w).Encode(shardIDMap); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
}

func (h *RestoreHandler) handleRestoreShard(w http.ResponseWriter, r *http.Request) {
	span, r := tracing.ExtractFromHTTPRequest(r, "RestoreHandler.handleRestoreShard")
	defer span.Finish()

	ctx := r.Context()

	params := httprouter.ParamsFromContext(ctx)
	shardID, err := strconv.ParseUint(params.ByName("shardID"), 10, 64)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := h.RestoreService.RestoreShard(ctx, shardID, r.Body); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
}

// RestoreService is the client implementation of influxdb.RestoreService.
type RestoreService struct {
	Addr               string
	Token              string
	InsecureSkipVerify bool
}

func (s *RestoreService) RestoreKVStore(ctx context.Context, r io.Reader) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	u, err := NewURL(s.Addr, restoreKVPath)
	if err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPost, u.String(), r)
	if err != nil {
		return err
	}
	SetToken(s.Token, req)
	req = req.WithContext(ctx)

	hc := NewClient(u.Scheme, s.InsecureSkipVerify)
	hc.Timeout = httpClientTimeout
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if err := CheckError(resp); err != nil {
		return err
	}

	return nil
}

func (s *RestoreService) RestoreBucket(ctx context.Context, id influxdb.ID, dbi []byte) (map[uint64]uint64, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	u, err := NewURL(s.Addr, prefixRestore+fmt.Sprintf("/buckets/%s", id.String()))
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(http.MethodPost, u.String(), bytes.NewReader(dbi))
	if err != nil {
		return nil, err
	}
	SetToken(s.Token, req)
	req = req.WithContext(ctx)

	hc := NewClient(u.Scheme, s.InsecureSkipVerify)
	hc.Timeout = httpClientTimeout
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if err := CheckError(resp); err != nil {
		return nil, err
	}

	shardIDMap := make(map[uint64]uint64)
	if err := json.NewDecoder(resp.Body).Decode(&shardIDMap); err != nil {
		return nil, err
	}
	return shardIDMap, nil
}

func (s *RestoreService) RestoreShard(ctx context.Context, shardID uint64, r io.Reader) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	u, err := NewURL(s.Addr, fmt.Sprintf(prefixRestore+"/shards/%d", shardID))
	if err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPost, u.String(), r)
	if err != nil {
		return err
	}
	SetToken(s.Token, req)
	req = req.WithContext(ctx)

	hc := NewClient(u.Scheme, s.InsecureSkipVerify)
	hc.Timeout = httpClientTimeout
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if err := CheckError(resp); err != nil {
		return err
	}

	return nil
}

package http

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/influxdata/influxdb/v2/kit/platform/errors"

	"github.com/influxdata/httprouter"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	"go.uber.org/zap"
)

// BackupBackend is all services and associated parameters required to construct the BackupHandler.
type BackupBackend struct {
	Logger *zap.Logger
	errors.HTTPErrorHandler

	BackupService influxdb.BackupService
}

// NewBackupBackend returns a new instance of BackupBackend.
func NewBackupBackend(b *APIBackend) *BackupBackend {
	return &BackupBackend{
		Logger: b.Logger.With(zap.String("handler", "backup")),

		HTTPErrorHandler: b.HTTPErrorHandler,
		BackupService:    b.BackupService,
	}
}

// BackupHandler is http handler for backup service.
type BackupHandler struct {
	*httprouter.Router
	errors.HTTPErrorHandler
	Logger *zap.Logger

	BackupService influxdb.BackupService
}

const (
	prefixBackup      = "/api/v2/backup"
	backupKVStorePath = prefixBackup + "/kv"
	backupShardPath   = prefixBackup + "/shards/:shardID"

	httpClientTimeout = time.Hour
)

// NewBackupHandler creates a new handler at /api/v2/backup to receive backup requests.
func NewBackupHandler(b *BackupBackend) *BackupHandler {
	h := &BackupHandler{
		HTTPErrorHandler: b.HTTPErrorHandler,
		Router:           NewRouter(b.HTTPErrorHandler),
		Logger:           b.Logger,
		BackupService:    b.BackupService,
	}

	h.HandlerFunc(http.MethodGet, backupKVStorePath, h.handleBackupKVStore)
	h.HandlerFunc(http.MethodGet, backupShardPath, h.handleBackupShard)

	return h
}

func (h *BackupHandler) handleBackupKVStore(w http.ResponseWriter, r *http.Request) {
	span, r := tracing.ExtractFromHTTPRequest(r, "BackupHandler.handleBackupKVStore")
	defer span.Finish()

	ctx := r.Context()

	if err := h.BackupService.BackupKVStore(ctx, w); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
}

func (h *BackupHandler) handleBackupShard(w http.ResponseWriter, r *http.Request) {
	span, r := tracing.ExtractFromHTTPRequest(r, "BackupHandler.handleBackupShard")
	defer span.Finish()

	ctx := r.Context()

	params := httprouter.ParamsFromContext(ctx)
	shardID, err := strconv.ParseUint(params.ByName("shardID"), 10, 64)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	var since time.Time
	if s := r.URL.Query().Get("since"); s != "" {
		if since, err = time.ParseInLocation(time.RFC3339, s, time.UTC); err != nil {
			h.HandleHTTPError(ctx, err, w)
			return
		}
	}

	if err := h.BackupService.BackupShard(ctx, w, shardID, since); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
}

// BackupService is the client implementation of influxdb.BackupService.
type BackupService struct {
	Addr               string
	Token              string
	InsecureSkipVerify bool
}

func (s *BackupService) BackupKVStore(ctx context.Context, w io.Writer) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	u, err := NewURL(s.Addr, prefixBackup+"/kv")
	if err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
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

	if _, err := io.Copy(w, resp.Body); err != nil {
		return err
	}
	return resp.Body.Close()
}

func (s *BackupService) BackupShard(ctx context.Context, w io.Writer, shardID uint64, since time.Time) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	u, err := NewURL(s.Addr, fmt.Sprintf(prefixBackup+"/shards/%d", shardID))
	if err != nil {
		return err
	}
	if !since.IsZero() {
		u.RawQuery = (url.Values{"since": {since.UTC().Format(time.RFC3339)}}).Encode()
	}

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
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

	if _, err := io.Copy(w, resp.Body); err != nil {
		return err
	}
	return resp.Body.Close()
}

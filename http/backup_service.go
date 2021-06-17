package http

import (
	"net/http"
	"strconv"
	"time"

	"github.com/influxdata/httprouter"
	"github.com/influxdata/influxdb/v2"
	// "github.com/influxdata/influxdb/v2/bolt"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	"go.uber.org/zap"
)

// BackupBackend is all services and associated parameters required to construct the BackupHandler.
type BackupBackend struct {
	Logger *zap.Logger
	influxdb.HTTPErrorHandler

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
	influxdb.HTTPErrorHandler
	Logger *zap.Logger

	BackupService influxdb.BackupService
}

const (
	prefixBackup      = "/api/v2/backup"
	backupKVStorePath = prefixBackup + "/kv"
	backupShardPath   = prefixBackup + "/shards/:shardID"
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

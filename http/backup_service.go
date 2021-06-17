package http

import (
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strconv"
	"time"

	"github.com/NYTimes/gziphandler"
	"github.com/influxdata/influxdb/v2/authorizer"
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

	BackupService           influxdb.BackupService
	SqlBackupRestoreService influxdb.SqlBackupRestoreService
	BucketManifestWriter    influxdb.BucketManifestWriter
}

// NewBackupBackend returns a new instance of BackupBackend.
func NewBackupBackend(b *APIBackend) *BackupBackend {
	return &BackupBackend{
		Logger: b.Logger.With(zap.String("handler", "backup")),

		HTTPErrorHandler:        b.HTTPErrorHandler,
		BackupService:           b.BackupService,
		SqlBackupRestoreService: b.SqlBackupRestoreService,
		BucketManifestWriter:    b.BucketManifestWriter,
	}
}

// BackupHandler is http handler for backup service.
type BackupHandler struct {
	*httprouter.Router
	errors.HTTPErrorHandler
	Logger *zap.Logger

	BackupService           influxdb.BackupService
	SqlBackupRestoreService influxdb.SqlBackupRestoreService
	BucketManifestWriter    influxdb.BucketManifestWriter
}

const (
	prefixBackup       = "/api/v2/backup"
	backupKVStorePath  = prefixBackup + "/kv"
	backupShardPath    = prefixBackup + "/shards/:shardID"
	backupMetadataPath = prefixBackup + "/metadata"
)

// NewBackupHandler creates a new handler at /api/v2/backup to receive backup requests.
func NewBackupHandler(b *BackupBackend) *BackupHandler {
	h := &BackupHandler{
		HTTPErrorHandler:        b.HTTPErrorHandler,
		Router:                  NewRouter(b.HTTPErrorHandler),
		Logger:                  b.Logger,
		BackupService:           b.BackupService,
		SqlBackupRestoreService: b.SqlBackupRestoreService,
		BucketManifestWriter:    b.BucketManifestWriter,
	}

	h.HandlerFunc(http.MethodGet, backupKVStorePath, h.handleBackupKVStore) // Deprecated

	h.Handler(http.MethodGet, backupShardPath, gziphandler.GzipHandler(http.HandlerFunc(h.handleBackupShard)))
	h.Handler(http.MethodGet, backupMetadataPath, gziphandler.GzipHandler(h.requireOperPermissions(http.HandlerFunc(h.handleBackupMetadata))))

	return h
}

// requireOperPermissions returns an "unauthorized" response for requests that do not have OperPermissions.
// This is needed for the handleBackupMetadata handler, which sets a header prior to
// accessing any methods on the BackupService which would also return an "authorized" response.
func (h *BackupHandler) requireOperPermissions(next http.Handler) http.HandlerFunc {
	fn := func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		if err := authorizer.IsAllowedAll(ctx, influxdb.OperPermissions()); err != nil {
			h.HandleHTTPError(ctx, err, w)
			return
		}
		next.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
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

func (h *BackupHandler) handleBackupMetadata(w http.ResponseWriter, r *http.Request) {
	span, r := tracing.ExtractFromHTTPRequest(r, "BackupHandler.handleBackupMetadata")
	defer span.Finish()

	ctx := r.Context()

	// Lock the sqlite and bolt databases prior to writing the response to prevent
	// data inconsistencies.
	h.BackupService.LockKVStore()
	defer h.BackupService.UnlockKVStore()

	h.SqlBackupRestoreService.LockSqlStore()
	defer h.SqlBackupRestoreService.UnlockSqlStore()

	dataWriter := multipart.NewWriter(w)
	w.Header().Set("Content-Type", "multipart/mixed; boundary="+dataWriter.Boundary())

	parts := []struct {
		contentType        string
		contentDisposition string
		writeFn            func(io.Writer) error
	}{
		{
			"application/octet-stream",
			fmt.Sprintf("attachment; name=%q", "kv"),
			func(fw io.Writer) error {
				return h.BackupService.BackupKVStore(ctx, fw)
			},
		},
		{
			"application/octet-stream",
			fmt.Sprintf("attachment; name=%q", "sql"),
			func(fw io.Writer) error {
				return h.SqlBackupRestoreService.BackupSqlStore(ctx, fw)
			},
		},
		{
			"application/json; charset=utf-8",
			fmt.Sprintf("attachment; name=%q", "buckets"),
			func(fw io.Writer) error {
				return h.BucketManifestWriter.WriteManifest(ctx, fw)
			},
		},
	}

	for _, p := range parts {
		pw, err := dataWriter.CreatePart(map[string][]string{
			"Content-Type":        {p.contentType},
			"Content-Disposition": {p.contentDisposition},
		})
		if err != nil {
			h.HandleHTTPError(ctx, err, w)
			return
		}

		if err := p.writeFn(pw); err != nil {
			h.HandleHTTPError(ctx, err, w)
			return
		}
	}

	if err := dataWriter.Close(); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
}

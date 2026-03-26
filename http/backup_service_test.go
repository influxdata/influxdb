package http

import (
	"compress/gzip"
	"context"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/influxdata/influxdb/v2"
	influxdbcontext "github.com/influxdata/influxdb/v2/context"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestBackupMetaService(t *testing.T) {
	ctrlr := gomock.NewController(t)
	backupSvc := mock.NewMockBackupService(ctrlr)
	sqlBackupSvc := mock.NewMockSqlBackupRestoreService(ctrlr)
	bucketManifestWriter := mock.NewMockBucketManifestWriter(ctrlr)

	b := &BackupBackend{
		BackupService:           backupSvc,
		SqlBackupRestoreService: sqlBackupSvc,
		BucketManifestWriter:    bucketManifestWriter,
	}
	h := NewBackupHandler(b)

	rr := httptest.NewRecorder()
	r, err := http.NewRequest(http.MethodGet, "/", nil)
	require.NoError(t, err)

	backupSvc.EXPECT().
		BackupKVStore(gomock.Any(), gomock.Any()).
		Return(nil)

	backupSvc.EXPECT().RLockKVStore()
	backupSvc.EXPECT().UnlockKVStore()

	sqlBackupSvc.EXPECT().
		BackupSqlStore(gomock.Any(), gomock.Any()).
		Return(nil)

	sqlBackupSvc.EXPECT().RLockSqlStore()
	sqlBackupSvc.EXPECT().RUnlockSqlStore()

	bucketManifestWriter.EXPECT().
		WriteManifest(gomock.Any(), gomock.Any()).
		Return(nil)

	h.handleBackupMetadata(rr, r)
	rs := rr.Result()
	require.Equal(t, rs.StatusCode, http.StatusOK)

	// Parse the multi-part response
	// First get the boundary from the header
	_, params, err := mime.ParseMediaType(rs.Header.Get("Content-Type"))
	require.NoError(t, err)
	mr := multipart.NewReader(rs.Body, params["boundary"])

	// Go through the parts of the response and verify the part names appear in the correct order
	wantContentTypes := []string{"application/octet-stream", "application/octet-stream", "application/json; charset=utf-8"}
	wantPartNames := []string{"kv", "sql", "buckets"}
	for i := 0; ; i++ {
		p, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		require.Equal(t, wantContentTypes[i], p.Header.Get("Content-Type"))

		_, params, err := mime.ParseMediaType(p.Header.Get("Content-Disposition"))
		require.NoError(t, err)
		require.Equal(t, wantPartNames[i], params["name"])
	}
}

func TestRequireOperPermissions(t *testing.T) {
	tests := []struct {
		name            string
		permList        []influxdb.Permission
		wantStatus      int
		wantContentType string
	}{
		{
			"authorized to do the backup",
			influxdb.OperPermissions(),
			http.StatusOK,
			"text/plain; charset=utf-8",
		},
		{
			"not authorized to do the backup",
			influxdb.ReadAllPermissions(),
			http.StatusUnauthorized,
			"application/json; charset=utf-8",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// next will only be called if the authorization allows it. this is a dummy function
			// that will set the content-type header to something other than application/json
			next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/plain; charset=utf-8")
				w.Write([]byte("OK"))
			})

			rr := httptest.NewRecorder()

			r, err := http.NewRequest(http.MethodGet, "/", nil)
			require.NoError(t, err)
			ctx := influxdbcontext.SetAuthorizer(context.Background(), mock.NewMockAuthorizer(false, tt.permList))
			r = r.WithContext(ctx)

			h := BackupHandler{
				HTTPErrorHandler: kithttp.NewErrorHandler(zaptest.NewLogger(t)),
			}
			h.requireOperPermissions(next).ServeHTTP(rr, r)
			rs := rr.Result()

			require.Equal(t, tt.wantStatus, rs.StatusCode)
			require.Equal(t, tt.wantContentType, rs.Header.Get("Content-Type"))
		})
	}
}

func TestBackupMetaServiceCompressionLevel(t *testing.T) {
	tests := []struct {
		name       string
		level      string
		wantStatus int
	}{
		{
			name:       "default compression when omitted",
			level:      "",
			wantStatus: http.StatusOK,
		},
		{
			name:       "explicit default compression",
			level:      "default",
			wantStatus: http.StatusOK,
		},
		{
			name:       "full compression",
			level:      "full",
			wantStatus: http.StatusOK,
		},
		{
			name:       "speedy compression",
			level:      "speedy",
			wantStatus: http.StatusOK,
		},
		{
			name:       "no compression",
			level:      "none",
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid compression level",
			level:      "bogus",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrlr := gomock.NewController(t)
			backupSvc := mock.NewMockBackupService(ctrlr)
			sqlBackupSvc := mock.NewMockSqlBackupRestoreService(ctrlr)
			bucketManifestWriter := mock.NewMockBucketManifestWriter(ctrlr)

			b := &BackupBackend{
				Logger:                  zaptest.NewLogger(t),
				HTTPErrorHandler:        kithttp.NewErrorHandler(zaptest.NewLogger(t)),
				BackupService:           backupSvc,
				SqlBackupRestoreService: sqlBackupSvc,
				BucketManifestWriter:    bucketManifestWriter,
			}
			h := NewBackupHandler(b)

			url := "/api/v2/backup/metadata"
			if tt.level != "" {
				url += "?gzip_compression_level=" + tt.level
			}
			r, err := http.NewRequest(http.MethodGet, url, nil)
			require.NoError(t, err)
			r.Header.Set("Accept-Encoding", "gzip")

			ctx := influxdbcontext.SetAuthorizer(r.Context(), mock.NewMockAuthorizer(false, influxdb.OperPermissions()))
			r = r.WithContext(ctx)

			if tt.wantStatus == http.StatusOK {
				backupSvc.EXPECT().RLockKVStore()
				backupSvc.EXPECT().UnlockKVStore()
				backupSvc.EXPECT().
					BackupKVStore(gomock.Any(), gomock.Any()).
					Return(nil)

				sqlBackupSvc.EXPECT().RLockSqlStore()
				sqlBackupSvc.EXPECT().RUnlockSqlStore()
				sqlBackupSvc.EXPECT().
					BackupSqlStore(gomock.Any(), gomock.Any()).
					Return(nil)

				bucketManifestWriter.EXPECT().
					WriteManifest(gomock.Any(), gomock.Any()).
					Return(nil)
			}

			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, r)
			rs := rr.Result()

			require.Equal(t, tt.wantStatus, rs.StatusCode)

			// The "none" level bypasses gziphandler and sets Content-Encoding directly,
			// so we can verify it produces valid gzip output even with small responses.
			if tt.level == "none" {
				require.Equal(t, "gzip", rs.Header.Get("Content-Encoding"))
				gr, err := gzip.NewReader(rs.Body)
				require.NoError(t, err)
				defer gr.Close()
				_, err = io.ReadAll(gr)
				require.NoError(t, err)
			}
		})
	}
}

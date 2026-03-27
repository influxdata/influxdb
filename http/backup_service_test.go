package http

import (
	"bytes"
	"context"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/NYTimes/gziphandler"
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

func TestBackupCompressionLevel(t *testing.T) {
	routes := []struct {
		name string
		path string
	}{
		{"metadata", "/api/v2/backup/metadata"},
		{"shard", "/api/v2/backup/shards/100"},
	}

	levels := []struct {
		name       string
		level      string
		wantStatus int
	}{
		{"default compression when omitted", "", http.StatusOK},
		{"explicit default compression", "default", http.StatusOK},
		{"full compression", "full", http.StatusOK},
		{"speedy compression", "speedy", http.StatusOK},
		{"no compression", "none", http.StatusOK},
		{"invalid compression level", "bogus", http.StatusBadRequest},
	}

	const SmallPayload = gziphandler.DefaultMinSize - 100
	const LargePayload = gziphandler.DefaultMinSize + 100

	// Use gziphandler.DefaultMinSize; responses smaller than that
	// are not compressed regardless of level. Test both sides of that threshold
	// to show that "none" is the only level that unconditionally skips gzip.
	sizes := []struct {
		name        string
		payloadSize int
	}{
		{"small payload", SmallPayload},
		{"large payload", LargePayload},
	}

	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			for _, sz := range sizes {
				t.Run(sz.name, func(t *testing.T) {
					payload := bytes.Repeat([]byte("x"), sz.payloadSize)

					for _, tt := range levels {
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

							r, err := http.NewRequest(http.MethodGet, route.path, nil)
							require.NoError(t, err)
							r.Header.Set("Accept-Encoding", "gzip")
							if tt.level != "" {
								r.Header.Set("Gzip-Compression-Level", tt.level)
							}

							ctx := influxdbcontext.SetAuthorizer(r.Context(), mock.NewMockAuthorizer(false, influxdb.OperPermissions()))
							r = r.WithContext(ctx)

							if tt.wantStatus == http.StatusOK {
								switch route.name {
								case "metadata":
									backupSvc.EXPECT().RLockKVStore()
									backupSvc.EXPECT().UnlockKVStore()
									backupSvc.EXPECT().
										BackupKVStore(gomock.Any(), gomock.Any()).
										DoAndReturn(func(_ context.Context, w io.Writer) error {
											_, err := w.Write(payload)
											return err
										})

									sqlBackupSvc.EXPECT().RLockSqlStore()
									sqlBackupSvc.EXPECT().RUnlockSqlStore()
									sqlBackupSvc.EXPECT().
										BackupSqlStore(gomock.Any(), gomock.Any()).
										DoAndReturn(func(_ context.Context, w io.Writer) error {
											_, err := w.Write(payload)
											return err
										})

									bucketManifestWriter.EXPECT().
										WriteManifest(gomock.Any(), gomock.Any()).
										DoAndReturn(func(_ context.Context, w io.Writer) error {
											_, err := w.Write(payload)
											return err
										})
								case "shard":
									backupSvc.EXPECT().
										BackupShard(gomock.Any(), gomock.Any(), uint64(100), gomock.Any()).
										DoAndReturn(func(_ context.Context, w io.Writer, _ uint64, _ interface{}) error {
											_, err := w.Write(payload)
											return err
										})
								}
							}

							rr := httptest.NewRecorder()
							h.ServeHTTP(rr, r)
							rs := rr.Result()

							require.Equal(t, tt.wantStatus, rs.StatusCode)

							if tt.wantStatus == http.StatusOK {
								if tt.level == "none" {
									// "none" skips gzip entirely; verify no Content-Encoding is set.
									require.Empty(t, rs.Header.Get("Content-Encoding"))
								} else if sz.payloadSize > 1400 {
									// Payload exceeds gziphandler.DefaultMinSize, so it should be compressed.
									require.Equal(t, "gzip", rs.Header.Get("Content-Encoding"))
								} else {
									// Payload is below the minimum size threshold; gziphandler does not compress.
									require.Empty(t, rs.Header.Get("Content-Encoding"))
								}
							}
						})
					}
				})
			}
		})
	}
}

package http

import (
	"context"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/influxdata/influxdb/v2"
	influxdbcontext "github.com/influxdata/influxdb/v2/context"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/stretchr/testify/require"
)

func TestBackupMetaService(t *testing.T) {
	ctrlr := gomock.NewController(t)
	backupSvc := mock.NewMockBackupService(ctrlr)
	sqlBackupSvc := mock.NewMockSqlBackupRestoreService(ctrlr)

	b := &BackupBackend{
		BackupService:           backupSvc,
		SqlBackupRestoreService: sqlBackupSvc,
	}
	h := NewBackupHandler(b)

	rr := httptest.NewRecorder()
	r, err := http.NewRequest(http.MethodGet, "/", nil)
	require.NoError(t, err)

	backupSvc.EXPECT().
		BackupKVStore(gomock.Any(), gomock.Any()).
		Return(nil)

	backupSvc.EXPECT().
		LockKVStore()

	backupSvc.EXPECT().
		UnlockKVStore()

	sqlBackupSvc.EXPECT().
		BackupSqlStore(gomock.Any(), gomock.Any()).
		Return(nil)

	sqlBackupSvc.EXPECT().
		LockSqlStore()

	sqlBackupSvc.EXPECT().
		UnlockSqlStore()

	h.handleBackupMetadata(rr, r)
	rs := rr.Result()
	require.Equal(t, rs.StatusCode, http.StatusOK)

	// Parse the multi-part response
	// First get the boundary from the header
	_, params, err := mime.ParseMediaType(rs.Header.Get("Content-Type"))
	require.NoError(t, err)
	mr := multipart.NewReader(rs.Body, params["boundary"])

	// Go through the parts of the response and collect the form and file names.
	// The file from the part could be read using something like ioutil.ReadAll, but for testing
	// the contents of the part is not meaningful.
	got := make(map[string]string)
	for {
		p, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		require.Equal(t, "application/octet-stream", p.Header.Get("Content-Type"))
		got[p.FormName()] = p.FileName()
	}

	// wants is a map of form names with the expected extension of the file for that part
	wants := map[string]string{
		"kv":      ".bolt",
		"sql":     ".sqlite",
		"buckets": ".json",
	}

	// make sure all of the parts that we want are present in the parsed response
	for formName, ext := range wants {
		gotFile, ok := got[formName]
		require.True(t, ok)

		gotExt := filepath.Ext(gotFile)
		require.Equal(t, ext, gotExt)
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
				HTTPErrorHandler: kithttp.ErrorHandler(0),
			}
			h.requireOperPermissions(next).ServeHTTP(rr, r)
			rs := rr.Result()

			require.Equal(t, tt.wantStatus, rs.StatusCode)
			require.Equal(t, tt.wantContentType, rs.Header.Get("Content-Type"))
		})
	}
}

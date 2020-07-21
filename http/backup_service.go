package http

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"time"

	"github.com/influxdata/httprouter"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/bolt"
	"github.com/influxdata/influxdb/v2/internal/fs"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

// BackupBackend is all services and associated parameters required to construct the BackupHandler.
type BackupBackend struct {
	Logger *zap.Logger
	influxdb.HTTPErrorHandler

	BackupService   influxdb.BackupService
	KVBackupService influxdb.KVBackupService
}

// NewBackupBackend returns a new instance of BackupBackend.
func NewBackupBackend(b *APIBackend) *BackupBackend {
	return &BackupBackend{
		Logger: b.Logger.With(zap.String("handler", "backup")),

		HTTPErrorHandler: b.HTTPErrorHandler,
		BackupService:    b.BackupService,
		KVBackupService:  b.KVBackupService,
	}
}

// BackupHandler is http handler for backup service.
type BackupHandler struct {
	*httprouter.Router
	influxdb.HTTPErrorHandler
	Logger *zap.Logger

	BackupService   influxdb.BackupService
	KVBackupService influxdb.KVBackupService
}

const (
	prefixBackup        = "/api/v2/backup"
	backupIDParamName   = "backup_id"
	backupFileParamName = "backup_file"
	backupFilePath      = prefixBackup + "/:" + backupIDParamName + "/file/:" + backupFileParamName

	httpClientTimeout = time.Hour
)

func composeBackupFilePath(backupID int, backupFile string) string {
	return path.Join(prefixBackup, fmt.Sprint(backupID), "file", fmt.Sprint(backupFile))
}

// NewBackupHandler creates a new handler at /api/v2/backup to receive backup requests.
func NewBackupHandler(b *BackupBackend) *BackupHandler {
	h := &BackupHandler{
		HTTPErrorHandler: b.HTTPErrorHandler,
		Router:           NewRouter(b.HTTPErrorHandler),
		Logger:           b.Logger,
		BackupService:    b.BackupService,
		KVBackupService:  b.KVBackupService,
	}

	h.HandlerFunc(http.MethodPost, prefixBackup, h.handleCreate)
	h.HandlerFunc(http.MethodGet, backupFilePath, h.handleFetchFile)

	return h
}

type backup struct {
	ID    int      `json:"id,omitempty"`
	Files []string `json:"files,omitempty"`
}

func (h *BackupHandler) handleCreate(w http.ResponseWriter, r *http.Request) {
	span, r := tracing.ExtractFromHTTPRequest(r, "BackupHandler.handleCreate")
	defer span.Finish()

	ctx := r.Context()

	id, files, err := h.BackupService.CreateBackup(ctx)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	internalBackupPath := h.BackupService.InternalBackupPath(id)

	boltPath := filepath.Join(internalBackupPath, bolt.DefaultFilename)
	boltFile, err := os.OpenFile(boltPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0660)
	if err != nil {
		err = multierr.Append(err, os.RemoveAll(internalBackupPath))
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err = h.KVBackupService.Backup(ctx, boltFile); err != nil {
		err = multierr.Append(err, os.RemoveAll(internalBackupPath))
		h.HandleHTTPError(ctx, err, w)
		return
	}

	files = append(files, bolt.DefaultFilename)

	credsExist, err := h.backupCredentials(internalBackupPath)

	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if credsExist {
		files = append(files, fs.DefaultConfigsFile)
	}

	b := backup{
		ID:    id,
		Files: files,
	}
	if err = json.NewEncoder(w).Encode(&b); err != nil {
		err = multierr.Append(err, os.RemoveAll(internalBackupPath))
		h.HandleHTTPError(ctx, err, w)
		return
	}
}

func (h *BackupHandler) backupCredentials(internalBackupPath string) (bool, error) {
	credBackupPath := filepath.Join(internalBackupPath, fs.DefaultConfigsFile)

	credPath, err := defaultConfigsPath()
	if err != nil {
		return false, err
	}
	token, err := ioutil.ReadFile(credPath)
	if err != nil && !os.IsNotExist(err) {
		return false, err
	} else if os.IsNotExist(err) {
		return false, nil
	}

	if err := ioutil.WriteFile(credBackupPath, []byte(token), 0600); err != nil {
		return false, err
	}
	return true, nil
}

func (h *BackupHandler) handleFetchFile(w http.ResponseWriter, r *http.Request) {
	span, r := tracing.ExtractFromHTTPRequest(r, "BackupHandler.handleFetchFile")
	defer span.Finish()

	ctx := r.Context()

	params := httprouter.ParamsFromContext(ctx)
	backupID, err := strconv.Atoi(params.ByName("backup_id"))
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	backupFile := params.ByName("backup_file")

	if err = h.BackupService.FetchBackupFile(ctx, backupID, backupFile, w); err != nil {
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

func (s *BackupService) CreateBackup(ctx context.Context) (int, []string, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	u, err := NewURL(s.Addr, prefixBackup)
	if err != nil {
		return 0, nil, err
	}

	req, err := http.NewRequest(http.MethodPost, u.String(), nil)
	if err != nil {
		return 0, nil, err
	}
	SetToken(s.Token, req)
	req = req.WithContext(ctx)

	hc := NewClient(u.Scheme, s.InsecureSkipVerify)
	hc.Timeout = httpClientTimeout
	resp, err := hc.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()

	if err := CheckError(resp); err != nil {
		return 0, nil, err
	}

	var b backup
	if err = json.NewDecoder(resp.Body).Decode(&b); err != nil {
		return 0, nil, err
	}

	return b.ID, b.Files, nil
}

func (s *BackupService) FetchBackupFile(ctx context.Context, backupID int, backupFile string, w io.Writer) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	u, err := NewURL(s.Addr, composeBackupFilePath(backupID, backupFile))
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

	_, err = io.Copy(w, resp.Body)
	if err != nil {
		return err
	}

	return nil
}

func defaultConfigsPath() (string, error) {
	dir, err := fs.InfluxDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, fs.DefaultConfigsFile), nil
}

func (s *BackupService) InternalBackupPath(backupID int) string {
	panic("internal method not implemented here")
}

package http

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/influxdata/influxdb/v2/v1/services/meta"

	"github.com/influxdata/httprouter"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	"go.uber.org/zap"
)

// RestoreBackend is all services and associated parameters required to construct the RestoreHandler.
type RestoreBackend struct {
	Logger *zap.Logger
	errors.HTTPErrorHandler

	RestoreService          influxdb.RestoreService
	SqlBackupRestoreService influxdb.SqlBackupRestoreService
	BucketService           influxdb.BucketService
}

// NewRestoreBackend returns a new instance of RestoreBackend.
func NewRestoreBackend(b *APIBackend) *RestoreBackend {
	return &RestoreBackend{
		Logger: b.Logger.With(zap.String("handler", "restore")),

		HTTPErrorHandler:        b.HTTPErrorHandler,
		RestoreService:          b.RestoreService,
		SqlBackupRestoreService: b.SqlBackupRestoreService,
		BucketService:           b.BucketService,
	}
}

// RestoreHandler is http handler for restore service.
type RestoreHandler struct {
	*httprouter.Router
	api *kithttp.API
	errors.HTTPErrorHandler
	Logger *zap.Logger

	RestoreService          influxdb.RestoreService
	SqlBackupRestoreService influxdb.SqlBackupRestoreService
	BucketService           influxdb.BucketService
}

const (
	prefixRestore    = "/api/v2/restore"
	restoreKVPath    = prefixRestore + "/kv"
	restoreSqlPath   = prefixRestore + "/sql"
	restoreShardPath = prefixRestore + "/shards/:shardID"

	restoreBucketPath         = prefixRestore + "/buckets/:bucketID" // Deprecated
	restoreBucketMetadataPath = prefixRestore + "/bucket-metadata"
)

// NewRestoreHandler creates a new handler at /api/v2/restore to receive restore requests.
func NewRestoreHandler(b *RestoreBackend) *RestoreHandler {
	h := &RestoreHandler{
		HTTPErrorHandler:        b.HTTPErrorHandler,
		Router:                  NewRouter(b.HTTPErrorHandler),
		Logger:                  b.Logger,
		RestoreService:          b.RestoreService,
		SqlBackupRestoreService: b.SqlBackupRestoreService,
		BucketService:           b.BucketService,
		api:                     kithttp.NewAPI(kithttp.WithLog(b.Logger)),
	}

	h.HandlerFunc(http.MethodPost, restoreKVPath, h.handleRestoreKVStore)
	h.HandlerFunc(http.MethodPost, restoreSqlPath, h.handleRestoreSqlStore)
	h.HandlerFunc(http.MethodPost, restoreBucketPath, h.handleRestoreBucket)
	h.HandlerFunc(http.MethodPost, restoreBucketMetadataPath, h.handleRestoreBucketMetadata)
	h.HandlerFunc(http.MethodPost, restoreShardPath, h.handleRestoreShard)

	return h
}

func (h *RestoreHandler) handleRestoreKVStore(w http.ResponseWriter, r *http.Request) {
	span, r := tracing.ExtractFromHTTPRequest(r, "RestoreHandler.handleRestoreKVStore")
	defer span.Finish()

	ctx := r.Context()

	var kvBytes io.Reader = r.Body
	if r.Header.Get("Content-Encoding") == "gzip" {
		gzr, err := gzip.NewReader(kvBytes)
		if err != nil {
			err = &errors.Error{
				Code: errors.EInvalid,
				Msg:  "failed to decode gzip request body",
				Err:  err,
			}
			h.HandleHTTPError(ctx, err, w)
		}
		defer gzr.Close()
		kvBytes = gzr
	}

	if err := h.RestoreService.RestoreKVStore(ctx, kvBytes); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
}

func (h *RestoreHandler) handleRestoreSqlStore(w http.ResponseWriter, r *http.Request) {
	span, r := tracing.ExtractFromHTTPRequest(r, "RestoreHandler.handleRestoreSqlStore")
	defer span.Finish()

	ctx := r.Context()

	var sqlBytes io.Reader = r.Body
	if r.Header.Get("Content-Encoding") == "gzip" {
		gzr, err := gzip.NewReader(sqlBytes)
		if err != nil {
			err = &errors.Error{
				Code: errors.EInvalid,
				Msg:  "failed to decode gzip request body",
				Err:  err,
			}
			h.HandleHTTPError(ctx, err, w)
		}
		defer gzr.Close()
		sqlBytes = gzr
	}

	if err := h.SqlBackupRestoreService.RestoreSqlStore(ctx, sqlBytes); err != nil {
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

func (h *RestoreHandler) handleRestoreBucketMetadata(w http.ResponseWriter, r *http.Request) {
	span, r := tracing.ExtractFromHTTPRequest(r, "RestoreHandler.handleRestoreBucketMetadata")
	defer span.Finish()
	ctx := r.Context()

	var b influxdb.BucketMetadataManifest
	if err := h.api.DecodeJSON(r.Body, &b); err != nil {
		h.api.Err(w, r, err)
		return
	}

	// Create the bucket - This will fail if the bucket already exists.
	// TODO: Could we support restoring to an existing bucket?
	var description string
	if b.Description != nil {
		description = *b.Description
	}
	var rp, sgd time.Duration
	if len(b.RetentionPolicies) > 0 {
		policy := b.RetentionPolicies[0]
		rp = policy.Duration
		sgd = policy.ShardGroupDuration
	}

	bkt := influxdb.Bucket{
		OrgID:              b.OrganizationID,
		Name:               b.BucketName,
		Description:        description,
		RetentionPeriod:    rp,
		ShardGroupDuration: sgd,
	}
	if err := h.BucketService.CreateBucket(ctx, &bkt); err != nil {
		h.api.Err(w, r, err)
		return
	}

	// Restore shard-level metadata for the new bucket.
	// TODO: It's silly to marshal the DBI into binary here only to unmarshal it again within
	//  the RestoreService, but it's the easiest way to share code with the 2.0.x restore API
	//  and avoid introducing a circular dependency on the `meta` package.
	//  When we reach a point where we feel comfortable deleting the 2.0.x endpoints, consider
	//  refactoring this to pass a struct directly instead of the marshalled bytes.
	dbi := manifestToDbInfo(b)
	rawDbi, err := dbi.MarshalBinary()
	if err != nil {
		h.api.Err(w, r, err)
		return
	}
	shardIDMap, err := h.RestoreService.RestoreBucket(ctx, bkt.ID, rawDbi)
	if err != nil {
		h.Logger.Warn("Cleaning up after failed bucket-restore", zap.String("bucket_id", bkt.ID.String()))
		if err2 := h.BucketService.DeleteBucket(ctx, bkt.ID); err2 != nil {
			h.Logger.Error("Failed to clean up bucket after failed restore",
				zap.String("bucket_id", bkt.ID.String()), zap.Error(err2))
		}
		h.api.Err(w, r, err)
		return
	}

	res := influxdb.RestoredBucketMappings{
		ID:            bkt.ID,
		Name:          bkt.Name,
		ShardMappings: make([]influxdb.RestoredShardMapping, 0, len(shardIDMap)),
	}

	for old, new := range shardIDMap {
		res.ShardMappings = append(res.ShardMappings, influxdb.RestoredShardMapping{OldId: old, NewId: new})
	}

	h.api.Respond(w, r, http.StatusCreated, res)
}

func manifestToDbInfo(m influxdb.BucketMetadataManifest) meta.DatabaseInfo {
	dbi := meta.DatabaseInfo{
		Name:                   m.BucketName,
		DefaultRetentionPolicy: m.DefaultRetentionPolicy,
		RetentionPolicies:      make([]meta.RetentionPolicyInfo, len(m.RetentionPolicies)),
	}
	for i, rp := range m.RetentionPolicies {
		dbi.RetentionPolicies[i] = manifestToRpInfo(rp)
	}

	return dbi
}

func manifestToRpInfo(m influxdb.RetentionPolicyManifest) meta.RetentionPolicyInfo {
	rpi := meta.RetentionPolicyInfo{
		Name:               m.Name,
		ReplicaN:           m.ReplicaN,
		Duration:           m.Duration,
		ShardGroupDuration: m.ShardGroupDuration,
		ShardGroups:        make([]meta.ShardGroupInfo, len(m.ShardGroups)),
		Subscriptions:      make([]meta.SubscriptionInfo, len(m.Subscriptions)),
	}

	for i, sg := range m.ShardGroups {
		rpi.ShardGroups[i] = manifestToSgInfo(sg)
	}
	for i, s := range m.Subscriptions {
		rpi.Subscriptions[i] = meta.SubscriptionInfo{
			Name:         s.Name,
			Mode:         s.Mode,
			Destinations: s.Destinations,
		}
	}

	return rpi
}

func manifestToSgInfo(m influxdb.ShardGroupManifest) meta.ShardGroupInfo {
	var delAt, truncAt time.Time
	if m.DeletedAt != nil {
		delAt = *m.DeletedAt
	}
	if m.TruncatedAt != nil {
		truncAt = *m.TruncatedAt
	}
	sgi := meta.ShardGroupInfo{
		ID:          m.ID,
		StartTime:   m.StartTime,
		EndTime:     m.EndTime,
		DeletedAt:   delAt,
		TruncatedAt: truncAt,
		Shards:      make([]meta.ShardInfo, len(m.Shards)),
	}

	for i, sh := range m.Shards {
		sgi.Shards[i] = manifestToShardInfo(sh)
	}

	return sgi
}

func manifestToShardInfo(m influxdb.ShardManifest) meta.ShardInfo {
	si := meta.ShardInfo{
		ID:     m.ID,
		Owners: make([]meta.ShardOwner, len(m.ShardOwners)),
	}
	for i, so := range m.ShardOwners {
		si.Owners[i] = meta.ShardOwner{NodeID: so.NodeID}
	}

	return si
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

	var tsmBytes io.Reader = r.Body
	if r.Header.Get("Content-Encoding") == "gzip" {
		gzr, err := gzip.NewReader(tsmBytes)
		if err != nil {
			err = &errors.Error{
				Code: errors.EInvalid,
				Msg:  "failed to decode gzip request body",
				Err:  err,
			}
			h.HandleHTTPError(ctx, err, w)
		}
		defer gzr.Close()
		tsmBytes = gzr
	}

	if err := h.RestoreService.RestoreShard(ctx, shardID, tsmBytes); err != nil {
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

func (s *RestoreService) RestoreBucket(ctx context.Context, id platform.ID, dbi []byte) (map[uint64]uint64, error) {
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

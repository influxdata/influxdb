package http

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/influxdata/httprouter"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorizer"
	context2 "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/influxdata/influxdb/v2/v1/services/meta"
	"go.uber.org/zap"
)

// RestoreBackend is all services and associated parameters required to construct the RestoreHandler.
type RestoreBackend struct {
	Logger *zap.Logger
	errors.HTTPErrorHandler

	RestoreService          influxdb.RestoreService
	SqlBackupRestoreService influxdb.SqlBackupRestoreService
	BucketService           influxdb.BucketService
	AuthorizationService    influxdb.AuthorizationService
}

// NewRestoreBackend returns a new instance of RestoreBackend.
func NewRestoreBackend(b *APIBackend) *RestoreBackend {
	return &RestoreBackend{
		Logger: b.Logger.With(zap.String("handler", "restore")),

		HTTPErrorHandler:        b.HTTPErrorHandler,
		RestoreService:          b.RestoreService,
		SqlBackupRestoreService: b.SqlBackupRestoreService,
		BucketService:           b.BucketService,
		AuthorizationService:    b.AuthorizationService,
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
	AuthorizationService    influxdb.AuthorizationService
}

const (
	prefixRestore    = "/api/v2/restore"
	restoreKVPath    = prefixRestore + "/kv"
	restoreSqlPath   = prefixRestore + "/sql"
	restoreShardPath = prefixRestore + "/shards/:shardID"

	restoreBucketPath                   = prefixRestore + "/buckets/:bucketID" // Deprecated. Used by 2.0.x clients.
	restoreBucketMetadataDeprecatedPath = prefixRestore + "/bucket-metadata"   // Deprecated. Used by 2.1.0 of the CLI
	restoreBucketMetadataPath           = prefixRestore + "/bucketMetadata"
)

// Valid values for the onConflict query parameter of restoreBucketMetadataPath,
// controlling what happens when the target bucket already exists.
const (
	onConflictError   = "error"
	onConflictSkip    = "skip"
	onConflictReplace = "replace"
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
		AuthorizationService:    b.AuthorizationService,
		api:                     kithttp.NewAPI(kithttp.WithLog(b.Logger)),
	}

	h.HandlerFunc(http.MethodPost, restoreKVPath, h.handleRestoreKVStore)
	h.HandlerFunc(http.MethodPost, restoreSqlPath, h.handleRestoreSqlStore)
	h.HandlerFunc(http.MethodPost, restoreBucketPath, h.handleRestoreBucket)
	h.HandlerFunc(http.MethodPost, restoreBucketMetadataDeprecatedPath, h.handleRestoreBucketMetadata)
	h.HandlerFunc(http.MethodPost, restoreBucketMetadataPath, h.handleRestoreBucketMetadata)
	h.HandlerFunc(http.MethodPost, restoreShardPath, h.handleRestoreShard)

	return h
}

func (h *RestoreHandler) getOperatorToken(ctx context.Context) (influxdb.Authorization, error) {
	// Get the token post-restore
	auths, _, err := h.AuthorizationService.FindAuthorizations(ctx, influxdb.AuthorizationFilter{})
	if err != nil {
		return influxdb.Authorization{}, err
	}

	var operToken *influxdb.Authorization
	for _, a := range auths {
		authCtx := context.Background()
		authCtx = context2.SetAuthorizer(authCtx, a)
		if authorizer.IsAllowedAll(authCtx, influxdb.OperPermissions()) == nil {
			operToken = a
			break
		}
	}

	if operToken == nil {
		return influxdb.Authorization{}, fmt.Errorf("invalid backup without an operator token, consider editing the BoltDB in the backup with 'influxd recovery'")
	}

	return *operToken, nil
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

	// Get the token post-restore
	operatorToken, err := h.getOperatorToken(ctx)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	// Return the new token to the caller so it can continue the restore
	response := make(map[string]string)
	response["token"] = operatorToken.Token

	h.api.Respond(w, r, http.StatusOK, response)
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
	buf, err := io.ReadAll(r.Body)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	shardIDMap, err := h.RestoreService.RestoreBucket(ctx, bucketID, buf, false)
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

	onConflict := r.URL.Query().Get("onConflict")
	switch onConflict {
	case "":
		onConflict = onConflictError
	case onConflictError, onConflictSkip, onConflictReplace:
	default:
		h.api.Err(w, r, &errors.Error{
			Code: errors.EInvalid,
			Msg: fmt.Sprintf("invalid onConflict value %q, must be one of %q, %q, %q",
				onConflict, onConflictError, onConflictSkip, onConflictReplace),
		})
		return
	}

	var b influxdb.BucketMetadataManifest
	if err := h.api.DecodeJSON(r.Body, &b); err != nil {
		h.api.Err(w, r, err)
		return
	}

	for _, rps := range b.RetentionPolicies {
		for _, sg := range rps.ShardGroups {
			if len(sg.Shards) <= 0 && sg.DeletedAt == nil {
				h.Logger.Warn("Restore: ShardGroup has not been deleted and has no shards",
					zap.String("bucket", b.BucketName),
					zap.String("rp", rps.Name),
					zap.Uint64("shard-group-id", sg.ID), zap.Time("start-time", sg.StartTime),
					zap.Time("end-time", sg.EndTime))
			}
		}
	}

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
	target, outcome, err := h.createRestoredBucket(ctx, &bkt, onConflict)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}
	if outcome == restoredBucketSkipped {
		// Skipped: respond with no shard mappings so the client has nothing to
		// upload for this bucket, leaving the existing data untouched.
		h.api.Respond(w, r, http.StatusOK, influxdb.RestoredBucketMappings{
			ID:            target.ID,
			Name:          target.Name,
			ShardMappings: []influxdb.RestoredShardMapping{},
		})
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
	shardIDMap, err := h.RestoreService.RestoreBucket(ctx, target.ID, rawDbi, outcome == restoredBucketReplaced)
	if err != nil {
		if outcome == restoredBucketCreated {
			h.Logger.Warn("Cleaning up after failed bucket-restore", zap.String("bucket_id", target.ID.String()))
			if err2 := h.BucketService.DeleteBucket(ctx, target.ID); err2 != nil {
				h.Logger.Error("Failed to clean up bucket after failed restore",
					zap.String("bucket_id", target.ID.String()), zap.Error(err2))
			}
		}
		h.api.Err(w, r, err)
		return
	}

	if outcome == restoredBucketReplaced {
		// Bring the bucket's own metadata (description, retention) in line with
		// the backup, now that the data restore is committed. Failing the
		// request here would abort the client's shard uploads mid-replace, so a
		// failure only logs: the restored data is intact and the bucket merely
		// keeps its previous description and retention settings.
		if _, err := h.BucketService.UpdateBucket(ctx, target.ID, influxdb.BucketUpdate{
			Description:        &bkt.Description,
			RetentionPeriod:    &bkt.RetentionPeriod,
			ShardGroupDuration: &bkt.ShardGroupDuration,
		}); err != nil {
			h.Logger.Warn("Failed to update replaced bucket's metadata to match the backup",
				zap.String("bucket_id", target.ID.String()), zap.Error(err))
		}
	}

	res := influxdb.RestoredBucketMappings{
		ID:            target.ID,
		Name:          target.Name,
		ShardMappings: make([]influxdb.RestoredShardMapping, 0, len(shardIDMap)),
	}

	for old, new := range shardIDMap {
		res.ShardMappings = append(res.ShardMappings, influxdb.RestoredShardMapping{OldId: old, NewId: new})
	}

	code := http.StatusCreated
	if outcome != restoredBucketCreated {
		code = http.StatusOK
	}
	h.api.Respond(w, r, code, res)
}

// restoredBucketOutcome reports how createRestoredBucket resolved the target
// bucket of a metadata restore.
type restoredBucketOutcome int

const (
	// restoredBucketCreated means the bucket did not exist and was created.
	restoredBucketCreated restoredBucketOutcome = iota
	// restoredBucketSkipped means the bucket exists and must be left untouched.
	restoredBucketSkipped
	// restoredBucketReplaced means the bucket exists and the restore should
	// replace its contents, keeping its ID so tokens, DBRP mappings, and tasks
	// referencing that ID keep working.
	restoredBucketReplaced
)

// createRestoredBucket ensures the target bucket for a metadata restore exists,
// applying the onConflict strategy when a bucket with the same name already
// exists. The returned bucket is the restore target; the outcome tells the
// caller how to proceed with it.
func (h *RestoreHandler) createRestoredBucket(ctx context.Context, bkt *influxdb.Bucket, onConflict string) (*influxdb.Bucket, restoredBucketOutcome, error) {
	if err := h.BucketService.CreateBucket(ctx, bkt); err != nil {
		if onConflict == onConflictError || errors.ErrorCode(err) != errors.EConflict {
			return nil, restoredBucketCreated, err
		}
		existing, err := h.BucketService.FindBucketByName(ctx, bkt.OrgID, bkt.Name)
		if err != nil {
			return nil, restoredBucketCreated, err
		}
		if onConflict == onConflictSkip {
			h.Logger.Info("Restore: bucket already exists, skipping",
				zap.String("bucket", bkt.Name))
			return existing, restoredBucketSkipped, nil
		}
		h.Logger.Info("Restore: bucket already exists, replacing its contents",
			zap.String("bucket", bkt.Name), zap.String("bucket_id", existing.ID.String()))
		return existing, restoredBucketReplaced, nil
	}
	return bkt, restoredBucketCreated, nil
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

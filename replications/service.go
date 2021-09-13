package replications

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	ierrors "github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/replications/internal"
	"github.com/influxdata/influxdb/v2/snowflake"
	"github.com/influxdata/influxdb/v2/sqlite"
	"github.com/mattn/go-sqlite3"
	"go.uber.org/zap"
)

var errReplicationNotFound = &ierrors.Error{
	Code: ierrors.ENotFound,
	Msg:  "replication not found",
}

func errRemoteNotFound(id platform.ID, cause error) error {
	return &ierrors.Error{
		Code: ierrors.EInvalid,
		Msg:  fmt.Sprintf("remote %q not found", id),
		Err:  cause,
	}
}

func errLocalBucketNotFound(id platform.ID, cause error) error {
	return &ierrors.Error{
		Code: ierrors.EInvalid,
		Msg:  fmt.Sprintf("local bucket %q not found", id),
		Err:  cause,
	}
}

func NewService(store *sqlite.SqlStore, bktSvc BucketService, log *zap.Logger) *service {
	return &service{
		store:         store,
		idGenerator:   snowflake.NewIDGenerator(),
		bucketService: bktSvc,
		validator:     internal.NewValidator(),
		log:           log,
	}
}

type ReplicationValidator interface {
	ValidateReplication(context.Context, *internal.ReplicationHTTPConfig) error
}

type BucketService interface {
	RLock()
	RUnlock()
	FindBucketByID(ctx context.Context, id platform.ID) (*influxdb.Bucket, error)
}

type service struct {
	store         *sqlite.SqlStore
	idGenerator   platform.IDGenerator
	bucketService BucketService
	validator     ReplicationValidator
	log           *zap.Logger
}

func (s service) ListReplications(ctx context.Context, filter influxdb.ReplicationListFilter) (*influxdb.Replications, error) {
	q := sq.Select(
		"id", "org_id", "name", "description",
		"remote_id", "local_bucket_id", "remote_bucket_id",
		"max_queue_size_bytes", "current_queue_size_bytes",
		"latest_response_code", "latest_error_message").
		From("replications").
		Where(sq.Eq{"org_id": filter.OrgID})

	if filter.Name != nil {
		q = q.Where(sq.Eq{"name": *filter.Name})
	}
	if filter.RemoteID != nil {
		q = q.Where(sq.Eq{"remote_id": *filter.RemoteID})
	}
	if filter.LocalBucketID != nil {
		q = q.Where(sq.Eq{"local_bucket_id": *filter.LocalBucketID})
	}

	query, args, err := q.ToSql()
	if err != nil {
		return nil, err
	}

	var rs influxdb.Replications
	if err := s.store.DB.SelectContext(ctx, &rs.Replications, query, args...); err != nil {
		return nil, err
	}
	return &rs, nil
}

func (s service) CreateReplication(ctx context.Context, request influxdb.CreateReplicationRequest) (*influxdb.Replication, error) {
	s.bucketService.RLock()
	defer s.bucketService.RUnlock()

	s.store.Mu.Lock()
	defer s.store.Mu.Unlock()

	if _, err := s.bucketService.FindBucketByID(ctx, request.LocalBucketID); err != nil {
		return nil, errLocalBucketNotFound(request.LocalBucketID, err)
	}

	q := sq.Insert("replications").
		SetMap(sq.Eq{
			"id":                       s.idGenerator.ID(),
			"org_id":                   request.OrgID,
			"name":                     request.Name,
			"description":              request.Description,
			"remote_id":                request.RemoteID,
			"local_bucket_id":          request.LocalBucketID,
			"remote_bucket_id":         request.RemoteBucketID,
			"max_queue_size_bytes":     request.MaxQueueSizeBytes,
			"current_queue_size_bytes": 0,
			"created_at":               "datetime('now')",
			"updated_at":               "datetime('now')",
		}).
		Suffix("RETURNING id, org_id, name, description, remote_id, local_bucket_id, remote_bucket_id, max_queue_size_bytes, current_queue_size_bytes")

	query, args, err := q.ToSql()
	if err != nil {
		return nil, err
	}

	var r influxdb.Replication
	if err := s.store.DB.GetContext(ctx, &r, query, args...); err != nil {
		if sqlErr, ok := err.(sqlite3.Error); ok && sqlErr.ExtendedCode == sqlite3.ErrConstraintForeignKey {
			return nil, errRemoteNotFound(request.RemoteID, err)
		}
		return nil, err
	}
	return &r, nil
}

func (s service) ValidateNewReplication(ctx context.Context, request influxdb.CreateReplicationRequest) error {
	if _, err := s.bucketService.FindBucketByID(ctx, request.LocalBucketID); err != nil {
		return errLocalBucketNotFound(request.LocalBucketID, err)
	}

	config := internal.ReplicationHTTPConfig{RemoteBucketID: request.RemoteBucketID}
	if err := s.populateRemoteHTTPConfig(ctx, request.RemoteID, &config); err != nil {
		return err
	}

	if err := s.validator.ValidateReplication(ctx, &config); err != nil {
		return &ierrors.Error{
			Code: ierrors.EInvalid,
			Msg:  "replication parameters fail validation",
			Err:  err,
		}
	}
	return nil
}

func (s service) GetReplication(ctx context.Context, id platform.ID) (*influxdb.Replication, error) {
	q := sq.Select(
		"id", "org_id", "name", "description",
		"remote_id", "local_bucket_id", "remote_bucket_id",
		"max_queue_size_bytes", "current_queue_size_bytes",
		"latest_response_code", "latest_error_message").
		From("replications").
		Where(sq.Eq{"id": id})

	query, args, err := q.ToSql()
	if err != nil {
		return nil, err
	}

	var r influxdb.Replication
	if err := s.store.DB.GetContext(ctx, &r, query, args...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errReplicationNotFound
		}
		return nil, err
	}
	return &r, nil
}

func (s service) UpdateReplication(ctx context.Context, id platform.ID, request influxdb.UpdateReplicationRequest) (*influxdb.Replication, error) {
	s.store.Mu.Lock()
	defer s.store.Mu.Unlock()

	updates := sq.Eq{"updated_at": sq.Expr("datetime('now')")}
	if request.Name != nil {
		updates["name"] = *request.Name
	}
	if request.Description != nil {
		updates["description"] = *request.Description
	}
	if request.RemoteID != nil {
		updates["remote_id"] = *request.RemoteID
	}
	if request.RemoteBucketID != nil {
		updates["remote_bucket_id"] = *request.RemoteBucketID
	}
	if request.MaxQueueSizeBytes != nil {
		updates["max_queue_size_bytes"] = *request.MaxQueueSizeBytes
	}

	q := sq.Update("replications").SetMap(updates).Where(sq.Eq{"id": id}).
		Suffix("RETURNING id, org_id, name, description, remote_id, local_bucket_id, remote_bucket_id, max_queue_size_bytes, current_queue_size_bytes")

	query, args, err := q.ToSql()
	if err != nil {
		return nil, err
	}

	var r influxdb.Replication
	if err := s.store.DB.GetContext(ctx, &r, query, args...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errReplicationNotFound
		}
		if sqlErr, ok := err.(sqlite3.Error); ok && request.RemoteID != nil && sqlErr.ExtendedCode == sqlite3.ErrConstraintForeignKey {
			return nil, errRemoteNotFound(*request.RemoteID, err)
		}
		return nil, err
	}
	return &r, nil
}

func (s service) ValidateUpdatedReplication(ctx context.Context, id platform.ID, request influxdb.UpdateReplicationRequest) error {
	baseConfig, err := s.getFullHTTPConfig(ctx, id)
	if err != nil {
		return err
	}

	if request.RemoteID != nil {
		if err := s.populateRemoteHTTPConfig(ctx, *request.RemoteID, baseConfig); err != nil {
			return err
		}
	}

	if err := s.validator.ValidateReplication(ctx, baseConfig); err != nil {
		return &ierrors.Error{
			Code: ierrors.EInvalid,
			Msg:  "validation fails after applying update",
			Err:  err,
		}
	}
	return nil
}

func (s service) DeleteReplication(ctx context.Context, id platform.ID) error {
	s.store.Mu.Lock()
	defer s.store.Mu.Unlock()

	q := sq.Delete("replications").Where(sq.Eq{"id": id}).Suffix("RETURNING id")
	query, args, err := q.ToSql()
	if err != nil {
		return err
	}

	var d platform.ID
	if err := s.store.DB.GetContext(ctx, &d, query, args...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return errReplicationNotFound
		}
		return err
	}
	return nil
}

func (s service) DeleteBucketReplications(ctx context.Context, localBucketID platform.ID) error {
	s.store.Mu.Lock()
	defer s.store.Mu.Unlock()

	q := sq.Delete("replications").Where(sq.Eq{"local_bucket_id": localBucketID}).Suffix("RETURNING id")
	query, args, err := q.ToSql()
	if err != nil {
		return err
	}

	var deleted []string
	if err := s.store.DB.SelectContext(ctx, &deleted, query, args...); err != nil {
		return err
	}
	s.log.Debug("Deleted all replications for local bucket",
		zap.String("bucket_id", localBucketID.String()), zap.Strings("ids", deleted))

	return nil
}

func (s service) ValidateReplication(ctx context.Context, id platform.ID) error {
	config, err := s.getFullHTTPConfig(ctx, id)
	if err != nil {
		return err
	}
	if err := s.validator.ValidateReplication(ctx, config); err != nil {
		return &ierrors.Error{
			Code: ierrors.EInvalid,
			Msg:  "remote failed validation",
			Err:  err,
		}
	}
	return nil
}

func (s service) getFullHTTPConfig(ctx context.Context, id platform.ID) (*internal.ReplicationHTTPConfig, error) {
	q := sq.Select("c.remote_url", "c.remote_api_token", "c.remote_org_id", "c.allow_insecure_tls", "r.remote_bucket_id").
		From("replications r").InnerJoin("remotes c ON r.remote_id = c.id AND r.id = ?", id)

	query, args, err := q.ToSql()
	if err != nil {
		return nil, err
	}

	var rc internal.ReplicationHTTPConfig
	if err := s.store.DB.GetContext(ctx, &rc, query, args...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errReplicationNotFound
		}
		return nil, err
	}
	return &rc, nil
}

func (s service) populateRemoteHTTPConfig(ctx context.Context, id platform.ID, target *internal.ReplicationHTTPConfig) error {
	q := sq.Select("remote_url", "remote_api_token", "remote_org_id", "allow_insecure_tls").
		From("remotes").Where(sq.Eq{"id": id})
	query, args, err := q.ToSql()
	if err != nil {
		return err
	}

	if err := s.store.DB.GetContext(ctx, target, query, args...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return errRemoteNotFound(id, nil)
		}
		return err
	}

	return nil
}

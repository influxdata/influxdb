package internal

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	ierrors "github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/sqlite"
	"github.com/mattn/go-sqlite3"
)

var errReplicationNotFound = &ierrors.Error{
	Code: ierrors.ENotFound,
	Msg:  "replication not found",
}

var errMissingIDName = &ierrors.Error{
	Code: ierrors.EUnprocessableEntity,
	Msg:  "one of remote_bucket_id, remote_bucket_name should be provided",
}

func errRemoteNotFound(id platform.ID, cause error) error {
	return &ierrors.Error{
		Code: ierrors.EInvalid,
		Msg:  fmt.Sprintf("remote %q not found", id),
		Err:  cause,
	}
}

type Store struct {
	sqlStore *sqlite.SqlStore
}

func NewStore(sqlStore *sqlite.SqlStore) *Store {
	return &Store{
		sqlStore: sqlStore,
	}
}

func (s *Store) Lock() {
	s.sqlStore.Mu.Lock()
}

func (s *Store) Unlock() {
	s.sqlStore.Mu.Unlock()
}

// ListReplications returns a list of replications matching the provided filter.
func (s *Store) ListReplications(ctx context.Context, filter influxdb.ReplicationListFilter) (*influxdb.Replications, error) {
	q := sq.Select(
		"id", "org_id", "name", "description", "remote_id", "local_bucket_id", "remote_bucket_id", "remote_bucket_name",
		"max_queue_size_bytes", "latest_response_code", "latest_error_message", "drop_non_retryable_data",
		"max_age_seconds").
		From("replications")

	if filter.OrgID.Valid() {
		q = q.Where(sq.Eq{"org_id": filter.OrgID})
	}
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
	if err := s.sqlStore.DB.SelectContext(ctx, &rs.Replications, query, args...); err != nil {
		return nil, err
	}

	return &rs, nil
}

// CreateReplication persists a new replication in the database. Caller is responsible for managing locks.
func (s *Store) CreateReplication(ctx context.Context, newID platform.ID, request influxdb.CreateReplicationRequest) (*influxdb.Replication, error) {
	fields := sq.Eq{
		"id":                      newID,
		"org_id":                  request.OrgID,
		"name":                    request.Name,
		"description":             request.Description,
		"remote_id":               request.RemoteID,
		"local_bucket_id":         request.LocalBucketID,
		"max_queue_size_bytes":    request.MaxQueueSizeBytes,
		"drop_non_retryable_data": request.DropNonRetryableData,
		"max_age_seconds":         request.MaxAgeSeconds,
		"created_at":              "datetime('now')",
		"updated_at":              "datetime('now')",
	}

	if request.RemoteBucketID != platform.ID(0) {
		fields["remote_bucket_id"] = request.RemoteBucketID
		fields["remote_bucket_name"] = ""
	} else if request.RemoteBucketName != "" {
		fields["remote_bucket_id"] = nil
		fields["remote_bucket_name"] = request.RemoteBucketName
	} else {
		return nil, errMissingIDName
	}

	q := sq.Insert("replications").
		SetMap(fields).
		Suffix("RETURNING id, org_id, name, description, remote_id, local_bucket_id, remote_bucket_id, remote_bucket_name, max_queue_size_bytes, drop_non_retryable_data, max_age_seconds")

	query, args, err := q.ToSql()
	if err != nil {
		return nil, err
	}

	var r influxdb.Replication

	if err := s.sqlStore.DB.GetContext(ctx, &r, query, args...); err != nil {
		if sqlErr, ok := err.(sqlite3.Error); ok && sqlErr.ExtendedCode == sqlite3.ErrConstraintForeignKey {
			return nil, errRemoteNotFound(request.RemoteID, err)
		}
		return nil, err
	}

	return &r, nil
}

// GetReplication gets a replication by ID from the database.
func (s *Store) GetReplication(ctx context.Context, id platform.ID) (*influxdb.Replication, error) {
	q := sq.Select(
		"id", "org_id", "name", "description", "remote_id", "local_bucket_id", "remote_bucket_id", "remote_bucket_name",
		"max_queue_size_bytes", "latest_response_code", "latest_error_message", "drop_non_retryable_data",
		"max_age_seconds").
		From("replications").
		Where(sq.Eq{"id": id})

	query, args, err := q.ToSql()
	if err != nil {
		return nil, err
	}

	var r influxdb.Replication
	if err := s.sqlStore.DB.GetContext(ctx, &r, query, args...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errReplicationNotFound
		}
		return nil, err
	}

	return &r, nil
}

// UpdateReplication updates a replication by ID. Caller is responsible for managing locks.
func (s *Store) UpdateReplication(ctx context.Context, id platform.ID, request influxdb.UpdateReplicationRequest) (*influxdb.Replication, error) {
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
	if request.RemoteBucketName != nil {
		updates["remote_bucket_name"] = *request.RemoteBucketName
	}
	if request.MaxQueueSizeBytes != nil {
		updates["max_queue_size_bytes"] = *request.MaxQueueSizeBytes
	}
	if request.DropNonRetryableData != nil {
		updates["drop_non_retryable_data"] = *request.DropNonRetryableData
	}
	if request.MaxAgeSeconds != nil {
		updates["max_age_seconds"] = *request.MaxAgeSeconds
	}

	q := sq.Update("replications").SetMap(updates).Where(sq.Eq{"id": id}).
		Suffix("RETURNING id, org_id, name, description, remote_id, local_bucket_id, remote_bucket_id, remote_bucket_name, max_queue_size_bytes, drop_non_retryable_data, max_age_seconds")

	query, args, err := q.ToSql()
	if err != nil {
		return nil, err
	}

	var r influxdb.Replication
	if err := s.sqlStore.DB.GetContext(ctx, &r, query, args...); err != nil {
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

// UpdateResponseInfo sets the most recent HTTP status code and error message received for a replication remote write.
func (s *Store) UpdateResponseInfo(ctx context.Context, id platform.ID, code int, message string) error {
	updates := sq.Eq{
		"latest_response_code": code,
		"latest_error_message": message,
	}

	q := sq.Update("replications").SetMap(updates).Where(sq.Eq{"id": id}).Suffix("RETURNING id")

	query, args, err := q.ToSql()
	if err != nil {
		return err
	}

	var d platform.ID
	if err := s.sqlStore.DB.GetContext(ctx, &d, query, args...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return errReplicationNotFound
		}
		return err
	}

	return nil
}

// DeleteReplication deletes a replication by ID from the database.  Caller is responsible for managing locks.
func (s *Store) DeleteReplication(ctx context.Context, id platform.ID) error {
	q := sq.Delete("replications").Where(sq.Eq{"id": id}).Suffix("RETURNING id")
	query, args, err := q.ToSql()
	if err != nil {
		return err
	}

	var d platform.ID
	if err := s.sqlStore.DB.GetContext(ctx, &d, query, args...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return errReplicationNotFound
		}
		return err
	}

	return nil
}

// DeleteBucketReplications deletes the replications for the provided localBucketID from the database.  Caller is
// responsible for managing locks. A list of deleted IDs is returned for further processing by the caller.
func (s *Store) DeleteBucketReplications(ctx context.Context, localBucketID platform.ID) ([]platform.ID, error) {
	q := sq.Delete("replications").Where(sq.Eq{"local_bucket_id": localBucketID}).Suffix("RETURNING id")
	query, args, err := q.ToSql()
	if err != nil {
		return nil, err
	}

	var deleted []platform.ID
	if err := s.sqlStore.DB.SelectContext(ctx, &deleted, query, args...); err != nil {
		return nil, err
	}

	return deleted, nil
}

func (s *Store) GetFullHTTPConfig(ctx context.Context, id platform.ID) (*influxdb.ReplicationHTTPConfig, error) {
	q := sq.Select("c.remote_url", "c.remote_api_token", "c.remote_org_id", "c.allow_insecure_tls", "r.remote_bucket_id", "r.remote_bucket_name", "r.drop_non_retryable_data").
		From("replications r").InnerJoin("remotes c ON r.remote_id = c.id AND r.id = ?", id)

	query, args, err := q.ToSql()
	if err != nil {
		return nil, err
	}

	var rc influxdb.ReplicationHTTPConfig
	if err := s.sqlStore.DB.GetContext(ctx, &rc, query, args...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errReplicationNotFound
		}
		return nil, err
	}
	return &rc, nil
}

func (s *Store) PopulateRemoteHTTPConfig(ctx context.Context, id platform.ID, target *influxdb.ReplicationHTTPConfig) error {
	q := sq.Select("remote_url", "remote_api_token", "remote_org_id", "allow_insecure_tls").
		From("remotes").Where(sq.Eq{"id": id})
	query, args, err := q.ToSql()
	if err != nil {
		return err
	}

	if err := s.sqlStore.DB.GetContext(ctx, target, query, args...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return errRemoteNotFound(id, nil)
		}
		return err
	}

	return nil
}

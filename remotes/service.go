package remotes

import (
	"context"
	"database/sql"
	"errors"

	sq "github.com/Masterminds/squirrel"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	ierrors "github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/snowflake"
	"github.com/influxdata/influxdb/v2/sqlite"
	"go.uber.org/zap"
)

var (
	errNotImplemented = &ierrors.Error{
		Code: ierrors.ENotImplemented,
		Msg:  "remote connection APIs not yet implemented",
	}
	errRemoteNotFound = &ierrors.Error{
		Code: ierrors.ENotFound,
		Msg:  "remote connection not found",
	}
)

type RemoteConnectionValidator interface {
	ValidateRemoteConnectionHTTPConfig(context.Context, *influxdb.RemoteConnectionHTTPConfig) error
}

func NewService(logger *zap.Logger, store *sqlite.SqlStore) *service {
	return &service{
		store:       store,
		log:         logger,
		idGenerator: snowflake.NewIDGenerator(),
		validator:   &stubValidator{},
	}
}

type service struct {
	store       *sqlite.SqlStore
	log         *zap.Logger
	idGenerator platform.IDGenerator
	validator   RemoteConnectionValidator
}

var _ influxdb.RemoteConnectionService = (*service)(nil)

func (s service) ListRemoteConnections(ctx context.Context, filter influxdb.RemoteConnectionListFilter) (*influxdb.RemoteConnections, error) {
	q := sq.Select("id", "org_id", "name", "description", "remote_url", "remote_org_id").
		From("remotes").
		Where(sq.Eq{"org_id": filter.OrgID})

	if filter.Name != nil {
		q = q.Where(sq.Eq{"name": *filter.Name})
	}
	if filter.RemoteURL != nil {
		q = q.Where(sq.Eq{"remote_url": *filter.RemoteURL})
	}

	query, args, err := q.ToSql()
	if err != nil {
		return nil, err
	}

	var rcs influxdb.RemoteConnections
	if err := s.store.DB.GetContext(ctx, &rcs.Remotes, query, args...); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}

	return &rcs, nil
}

func (s service) CreateRemoteConnection(ctx context.Context, request influxdb.CreateRemoteConnectionRequest) (*influxdb.RemoteConnection, error) {
	s.store.Mu.Lock()
	defer s.store.Mu.Unlock()

	q := sq.Insert("remotes").
		SetMap(sq.Eq{
			"id":                 s.idGenerator.ID(),
			"org_id":             request.OrgID,
			"name":               request.Name,
			"description":        request.Description,
			"remote_url":         request.RemoteURL,
			"remote_api_token":   request.RemoteToken,
			"remote_org_id":      request.RemoteOrgID,
			"allow_insecure_tls": request.AllowInsecureTLS,
			"created_at":         "datetime('now')",
			"updated_at":         "datetime('now')",
		}).
		Suffix("RETURNING id, org_id, name, description, remote_url, remote_org_id, allow_insecure_tls")

	query, args, err := q.ToSql()
	if err != nil {
		return nil, err
	}

	var rc influxdb.RemoteConnection
	if err := s.store.DB.GetContext(ctx, &rc, query, args...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errRemoteNotFound
		}
		return nil, err
	}
	return &rc, nil
}

func (s service) ValidateNewRemoteConnection(ctx context.Context, request influxdb.CreateRemoteConnectionRequest) error {
	config := influxdb.RemoteConnectionHTTPConfig{
		RemoteURL:        request.RemoteURL,
		RemoteToken:      request.RemoteToken,
		RemoteOrgID:      request.RemoteOrgID,
		AllowInsecureTLS: request.AllowInsecureTLS,
	}

	return s.validator.ValidateRemoteConnectionHTTPConfig(ctx, &config)
}

func (s service) GetRemoteConnection(ctx context.Context, id platform.ID) (*influxdb.RemoteConnection, error) {
	q := sq.Select("id", "org_id", "name", "description", "remote_url", "remote_org_id").
		From("remotes").
		Where(sq.Eq{"id": id})

	query, args, err := q.ToSql()
	if err != nil {
		return nil, err
	}

	var rc influxdb.RemoteConnection
	if err := s.store.DB.GetContext(ctx, &rc, query, args...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errRemoteNotFound
		}
		return nil, err
	}
	return &rc, nil
}

func (s service) UpdateRemoteConnection(ctx context.Context, id platform.ID, request influxdb.UpdateRemoteConnectionRequest) (*influxdb.RemoteConnection, error) {
	s.store.Mu.Lock()
	defer s.store.Mu.Unlock()

	updates := sq.Eq{"updated_at": sq.Expr("datetime('now')")}
	if request.AllowInsecureTLS != nil {
		updates["allow_insecure_tls"] = *request.AllowInsecureTLS
	}
	if request.RemoteOrgID != nil {
		updates["remote_org_id"] = *request.RemoteOrgID
	}
	if request.RemoteURL != nil {
		updates["remote_url"] = *request.RemoteURL
	}
	if request.RemoteToken != nil {
		updates["remote_api_token"] = *request.RemoteToken
	}
	if request.Name != nil {
		updates["name"] = *request.Name
	}
	if request.Description != nil {
		updates["description"] = *request.Description
	}

	q := sq.Update("remotes").SetMap(updates).Where(sq.Eq{"id": id}).
		Suffix("RETURNING id, org_id, name, description, remote_url, remote_org_id, allow_insecure_tls")

	query, args, err := q.ToSql()
	if err != nil {
		return nil, err
	}

	var rc influxdb.RemoteConnection
	if err := s.store.DB.GetContext(ctx, &rc, query, args...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errRemoteNotFound
		}
		return nil, err
	}
	return &rc, nil
}

func (s service) ValidateUpdatedRemoteConnection(ctx context.Context, id platform.ID, request influxdb.UpdateRemoteConnectionRequest) error {
	config, err := s.getConnectionHTTPConfig(ctx, id)
	if err != nil {
		return err
	}
	if request.AllowInsecureTLS != nil {
		config.AllowInsecureTLS = *request.AllowInsecureTLS
	}
	if request.RemoteOrgID != nil {
		config.RemoteOrgID = *request.RemoteOrgID
	}
	if request.RemoteToken != nil {
		config.RemoteToken = *request.RemoteToken
	}
	if request.RemoteURL != nil {
		config.RemoteURL = *request.RemoteURL
	}

	return s.validator.ValidateRemoteConnectionHTTPConfig(ctx, config)
}

func (s service) DeleteRemoteConnection(ctx context.Context, id platform.ID) error {
	s.store.Mu.Lock()
	defer s.store.Mu.Unlock()

	q := sq.Delete("remotes").Where(sq.Eq{"id": id}).Suffix("RETURNING id")
	query, args, err := q.ToSql()
	if err != nil {
		return err
	}

	var d platform.ID
	if err := s.store.DB.GetContext(ctx, &d, query, args...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return errRemoteNotFound
		}
		return err
	}
	return nil
}

func (s service) ValidateRemoteConnection(ctx context.Context, id platform.ID) error {
	config, err := s.getConnectionHTTPConfig(ctx, id)
	if err != nil {
		return err
	}
	return s.validator.ValidateRemoteConnectionHTTPConfig(ctx, config)
}

func (s service) getConnectionHTTPConfig(ctx context.Context, id platform.ID) (*influxdb.RemoteConnectionHTTPConfig, error) {
	q := sq.Select("remote_url", "remote_api_token", "remote_org_id", "allow_insecure_tls").
		From("remotes").
		Where(sq.Eq{"id": id})

	query, args, err := q.ToSql()
	if err != nil {
		return nil, err
	}

	var rc influxdb.RemoteConnectionHTTPConfig
	if err := s.store.DB.GetContext(ctx, &rc, query, args...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errRemoteNotFound
		}
		return nil, err
	}
	return &rc, nil
}

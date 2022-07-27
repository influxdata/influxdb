package instance

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
)

var (
	errInstanceNotFound = &ierrors.Error{
		Code: ierrors.ENotFound,
		Msg:  "instance not found",
	}
)

func NewService(store *sqlite.SqlStore) *service {
	return &service{
		store:       store,
		idGenerator: snowflake.NewIDGenerator(),
	}
}

type service struct {
	store       *sqlite.SqlStore
	idGenerator platform.IDGenerator
}

func (s service) CreateInstance(ctx context.Context) (*influxdb.Instance, error) {
	s.store.Mu.Lock()
	defer s.store.Mu.Unlock()

	q := sq.Insert("instance").
		SetMap(sq.Eq{
			"id":         s.idGenerator.ID(),
			"created_at": "datetime('now')",
			"updated_at": "datetime('now')",
		}).
		Suffix("RETURNING id")

	query, args, err := q.ToSql()
	if err != nil {
		return nil, err
	}

	var is influxdb.Instance
	if err := s.store.DB.GetContext(ctx, &is, query, args...); err != nil {
		return nil, err
	}
	return &is, nil
}

func (s service) GetInstance(ctx context.Context) (*influxdb.Instance, error) {
	q := sq.Select("id").
		From("instance")

	query, args, err := q.ToSql()
	if err != nil {
		return nil, err
	}

	var is influxdb.Instance
	if err := s.store.DB.GetContext(ctx, &is, query, args...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errInstanceNotFound
		}
		return nil, err
	}
	return &is, nil
}

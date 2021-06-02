package notebooks

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/snowflake"
	"github.com/influxdata/influxdb/v2/sqlite"
	"go.uber.org/zap"
)

var _ influxdb.NotebookService = (*Service)(nil)

type Service struct {
	store       *sqlite.SqlStore
	log         *zap.Logger
	idGenerator platform.IDGenerator
}

func NewService(logger *zap.Logger, store *sqlite.SqlStore) *Service {
	return &Service{
		store:       store,
		log:         logger,
		idGenerator: snowflake.NewIDGenerator(),
	}
}

func (s *Service) GetNotebook(ctx context.Context, id platform.ID) (*influxdb.Notebook, error) {
	var n influxdb.Notebook

	query := `
		SELECT id, org_id, name, spec, created_at, updated_at
		FROM notebooks WHERE id = $1`

	if err := s.store.DB.GetContext(ctx, &n, query, id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, influxdb.ErrNotebookNotFound
		}

		return nil, err
	}

	return &n, nil
}

// CreateNotebook creates a notebook. Note that this and all "write" operations on the database need to use the Mutex lock,
// since sqlite can only handle 1 concurrent write operation at a time.
func (s *Service) CreateNotebook(ctx context.Context, create *influxdb.NotebookReqBody) (*influxdb.Notebook, error) {
	s.store.Mu.Lock()
	defer s.store.Mu.Unlock()

	nowTime := time.Now().UTC()
	n := influxdb.Notebook{
		ID:        s.idGenerator.ID(),
		OrgID:     create.OrgID,
		Name:      create.Name,
		Spec:      create.Spec,
		CreatedAt: nowTime,
		UpdatedAt: nowTime,
	}

	query := `
		INSERT INTO notebooks (id, org_id, name, spec, created_at, updated_at)
		VALUES (:id, :org_id, :name, :spec, :created_at, :updated_at)`

	_, err := s.store.DB.NamedExecContext(ctx, query, &n)
	if err != nil {
		return nil, err
	}

	// Ideally, the create query would use "RETURNING" in order to avoid making a separate query.
	// Unfortunately this breaks the scanning of values into the result struct, so we have to make a separate
	// SELECT request to return the result from the database.
	return s.GetNotebook(ctx, n.ID)
}

// UpdateNotebook updates a notebook.
func (s *Service) UpdateNotebook(ctx context.Context, id platform.ID, update *influxdb.NotebookReqBody) (*influxdb.Notebook, error) {
	s.store.Mu.Lock()
	defer s.store.Mu.Unlock()

	nowTime := time.Now().UTC()
	n := influxdb.Notebook{
		ID:        id,
		OrgID:     update.OrgID,
		Name:      update.Name,
		Spec:      update.Spec,
		UpdatedAt: nowTime,
	}

	query := `
		UPDATE notebooks SET org_id = :org_id, name = :name, spec = :spec, updated_at = :updated_at
		WHERE id = :id`

	_, err := s.store.DB.NamedExecContext(ctx, query, &n)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, influxdb.ErrNotebookNotFound
		}

		return nil, err
	}

	return s.GetNotebook(ctx, n.ID)
}

// DeleteNotebook deletes a notebook.
func (s *Service) DeleteNotebook(ctx context.Context, id platform.ID) error {
	s.store.Mu.Lock()
	defer s.store.Mu.Unlock()

	query := `
		DELETE FROM notebooks
		WHERE id = $1`

	res, err := s.store.DB.ExecContext(ctx, query, id.String())
	if err != nil {
		return err
	}

	r, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if r == 0 {
		return influxdb.ErrNotebookNotFound
	}

	return nil
}

// ListNotebooks lists notebooks matching the provided filter. Currently, only org_id is used in the filter.
// Future uses may support pagination via this filter as well.
func (s *Service) ListNotebooks(ctx context.Context, filter influxdb.NotebookListFilter) ([]*influxdb.Notebook, error) {
	ns := []*influxdb.Notebook{}

	query := `
		SELECT id, org_id, name, spec, created_at, updated_at
		FROM notebooks
		WHERE org_id = $1`

	if err := s.store.DB.SelectContext(ctx, &ns, query, filter.OrgID); err != nil {
		return nil, err
	}

	return ns, nil
}

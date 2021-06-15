package annotations

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	ierrors "github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/snowflake"
	"github.com/influxdata/influxdb/v2/sqlite"
	"go.uber.org/zap"
)

var (
	errAnnotationNotFound = &ierrors.Error{
		Code: ierrors.EInvalid,
		Msg:  "annotation not found",
	}
	errStreamNotFound = &ierrors.Error{
		Code: ierrors.EInvalid,
		Msg:  "stream not found",
	}
)

var _ influxdb.AnnotationService = (*Service)(nil)

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

// CreateAnnotations creates annotations in the database for the provided orgID as defined by the provided list
// Streams corresponding to the StreamTag property of each annotation are created if they don't already exist
// as part of a transaction
func (s *Service) CreateAnnotations(ctx context.Context, orgID platform.ID, creates []influxdb.AnnotationCreate) ([]influxdb.AnnotationEvent, error) {
	// Guard clause - an empty list was provided for some reason, immediately return an empty result
	// set without doing the transaction
	if len(creates) == 0 {
		return []influxdb.AnnotationEvent{}, nil
	}

	s.store.Mu.Lock()
	defer s.store.Mu.Unlock()

	// store a unique list of stream names first. the invalid ID is a placeholder for the real id,
	// which will be obtained separately
	streams := make(map[string]platform.ID)
	for _, c := range creates {
		streams[c.StreamTag] = platform.InvalidID()
	}

	tx, err := s.store.DB.BeginTxx(ctx, nil)
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	// upsert each stream individually. a possible enhancement might be to do this as a single batched query
	// it is unlikely that this would offer much benefit since there is currently no mechanism for creating large numbers
	// of annotations simultaneously
	now := time.Now()
	for name := range streams {
		query, args, err := newUpsertStreamQuery(orgID, s.idGenerator.ID(), now, influxdb.Stream{Name: name})
		if err != nil {
			tx.Rollback()
			return nil, err
		}

		var streamID platform.ID
		if err = tx.GetContext(ctx, &streamID, query, args...); err != nil {
			tx.Rollback()
			return nil, err
		}

		streams[name] = streamID
	}

	// bulk insert for the creates. this also is unlikely to offer much performance benefit, but since the query
	// is only used here it is easy enough to form to bulk query.
	q := sq.Insert("annotations").
		Columns("id", "org_id", "stream_id", "stream", "summary", "message", "stickers", "duration", "lower", "upper").
		Suffix("RETURNING *")

	for _, create := range creates {
		// double check that we have a valid name for this stream tag - error if we don't. this should never be an error.
		streamID, ok := streams[create.StreamTag]
		if !ok {
			tx.Rollback()
			return nil, &ierrors.Error{
				Code: ierrors.EInternal,
				Msg:  fmt.Sprintf("unable to find id for stream %q", create.StreamTag),
			}
		}

		// add the row to the query
		newID := s.idGenerator.ID()
		lower := create.StartTime.Format(time.RFC3339Nano)
		upper := create.EndTime.Format(time.RFC3339Nano)
		duration := timesToDuration(*create.StartTime, *create.EndTime)
		q = q.Values(newID, orgID, streamID, create.StreamTag, create.Summary, create.Message, create.Stickers, duration, lower, upper)
	}

	// get the query string and args list for the bulk insert
	query, args, err := q.ToSql()
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	// run the bulk insert and store the result
	var res []influxdb.StoredAnnotation
	if err := tx.SelectContext(ctx, &res, query, args...); err != nil {
		tx.Rollback()
		return nil, err
	}

	if err = tx.Commit(); err != nil {
		return nil, err
	}

	// convert the StoredAnnotation structs to AnnotationEvent structs before returning
	return storedAnnotationsToEvents(res)
}

// ListAnnotations returns a list of annotations from the database matching the filter
// For time range matching, sqlite is able to compare times with millisecond accuracy
func (s *Service) ListAnnotations(ctx context.Context, orgID platform.ID, filter influxdb.AnnotationListFilter) ([]influxdb.StoredAnnotation, error) {
	// we need to explicitly format time strings here and elsewhere to ensure they are
	// interpreted by the database consistently
	sf := filter.StartTime.Format(time.RFC3339Nano)
	ef := filter.EndTime.Format(time.RFC3339Nano)

	q := sq.Select("annotations.*").
		Distinct().
		From("annotations, json_each(stickers) AS json").
		Where(sq.Eq{"org_id": orgID}).
		Where(sq.GtOrEq{"lower": sf}).
		Where(sq.LtOrEq{"upper": ef})

	// Add stream name filters to the query
	if len(filter.StreamIncludes) > 0 {
		q = q.Where(sq.Eq{"stream": filter.StreamIncludes})
	}

	// Add sticker filters to the query
	for k, v := range filter.StickerIncludes {
		q = q.Where(sq.And{sq.Eq{"json.value": fmt.Sprintf("%s=%s", k, v)}})
	}

	sql, args, err := q.ToSql()
	if err != nil {
		return nil, err
	}

	ans := []influxdb.StoredAnnotation{}
	if err := s.store.DB.SelectContext(ctx, &ans, sql, args...); err != nil {
		return nil, err
	}

	return ans, nil
}

// GetAnnotation gets a single annotation by ID
func (s *Service) GetAnnotation(ctx context.Context, id platform.ID) (*influxdb.StoredAnnotation, error) {
	q := sq.Select("*").
		From("annotations").
		Where(sq.Eq{"id": id})

	query, args, err := q.ToSql()
	if err != nil {
		return nil, err
	}

	var a influxdb.StoredAnnotation
	if err := s.store.DB.GetContext(ctx, &a, query, args...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errAnnotationNotFound
		}

		return nil, err
	}

	return &a, nil
}

// DeleteAnnotations deletes multiple annotations according to the provided filter
func (s *Service) DeleteAnnotations(ctx context.Context, orgID platform.ID, delete influxdb.AnnotationDeleteFilter) error {
	s.store.Mu.Lock()
	defer s.store.Mu.Unlock()

	sf := delete.StartTime.Format(time.RFC3339Nano)
	ef := delete.EndTime.Format(time.RFC3339Nano)

	// This is a subquery that will be as part of a DELETE FROM ... WHERE id IN (subquery)
	// A subquery is used because the json_each virtual table can only be used in a SELECT
	subQ := sq.Select("annotations.id").
		Distinct().
		From("annotations, json_each(stickers) AS json").
		Where(sq.Eq{"org_id": orgID}).
		Where(sq.GtOrEq{"lower": sf}).
		Where(sq.LtOrEq{"upper": ef})

	// Add the stream name filter to the subquery (if present)
	if len(delete.StreamTag) > 0 {
		subQ = subQ.Where(sq.Eq{"stream": delete.StreamTag})
	}

	// Add the stream ID filter to the subquery (if present)
	if delete.StreamID.Valid() {
		subQ = subQ.Where(sq.Eq{"stream_id": delete.StreamID})
	}

	// Add any sticker filters to the subquery
	for k, v := range delete.Stickers {
		subQ = subQ.Where(sq.And{sq.Eq{"json.value": fmt.Sprintf("%s=%s", k, v)}})
	}

	// Parse the subquery into a string and list of args
	subQuery, subArgs, err := subQ.ToSql()
	if err != nil {
		return err
	}
	// Convert the subquery into a sq.Sqlizer so that it can be used in the actual DELETE
	// operation. This is a bit of a hack since squirrel doesn't have great support for subqueries
	// outside of SELECT statements
	subExpr := sq.Expr("("+subQuery+")", subArgs...)

	q := sq.
		Delete("annotations").
		Suffix("WHERE annotations.id IN").
		SuffixExpr(subExpr)

	query, args, err := q.ToSql()

	if err != nil {
		return err
	}

	if _, err := s.store.DB.ExecContext(ctx, query, args...); err != nil {
		return err
	}

	return nil
}

// DeleteAnnoation deletes a single annotation by ID
func (s *Service) DeleteAnnotation(ctx context.Context, id platform.ID) error {
	s.store.Mu.Lock()
	defer s.store.Mu.Unlock()

	q := sq.Delete("annotations").
		Where(sq.Eq{"id": id}).
		Suffix("RETURNING id")

	query, args, err := q.ToSql()
	if err != nil {
		return err
	}

	var d platform.ID
	if err := s.store.DB.GetContext(ctx, &d, query, args...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return errAnnotationNotFound
		}

		return err
	}

	return nil
}

// UpdateAnnotation updates a single annotation by ID.
func (s *Service) UpdateAnnotation(ctx context.Context, id platform.ID, update influxdb.AnnotationCreate) (*influxdb.AnnotationEvent, error) {
	s.store.Mu.Lock()
	defer s.store.Mu.Unlock()

	q := sq.Update("annotations").
		SetMap(sq.Eq{
			"stream":   update.StreamTag,
			"summary":  update.Summary,
			"message":  update.Message,
			"stickers": update.Stickers,
			"duration": timesToDuration(*update.StartTime, *update.EndTime),
			"lower":    update.StartTime.Format(time.RFC3339Nano),
			"upper":    update.EndTime.Format(time.RFC3339Nano),
		}).
		Where(sq.Eq{"id": id}).
		Suffix("RETURNING *")

	query, args, err := q.ToSql()
	if err != nil {
		return nil, err
	}

	var st influxdb.StoredAnnotation
	err = s.store.DB.GetContext(ctx, &st, query, args...)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, errAnnotationNotFound
		}

		return nil, err
	}

	return st.ToEvent()
}

// ListStreams returns a list of streams matching the filter for the provided orgID.
func (s *Service) ListStreams(ctx context.Context, orgID platform.ID, filter influxdb.StreamListFilter) ([]influxdb.StoredStream, error) {
	q := sq.Select("id", "org_id", "name", "description", "created_at", "updated_at").
		From("streams").
		Where(sq.Eq{"org_id": orgID})

	// Add stream name filters to the query
	if len(filter.StreamIncludes) > 0 {
		q = q.Where(sq.Eq{"name": filter.StreamIncludes})
	}

	sql, args, err := q.ToSql()
	if err != nil {
		return nil, err
	}

	sts := []influxdb.StoredStream{}
	err = s.store.DB.SelectContext(ctx, &sts, sql, args...)
	if err != nil {
		return nil, err
	}

	return sts, nil
}

// GetStream gets a single stream by ID
func (s *Service) GetStream(ctx context.Context, id platform.ID) (*influxdb.StoredStream, error) {
	q := sq.Select("id", "org_id", "name", "description", "created_at", "updated_at").
		From("streams").
		Where(sq.Eq{"id": id})

	query, args, err := q.ToSql()
	if err != nil {
		return nil, err
	}

	var st influxdb.StoredStream
	if err := s.store.DB.GetContext(ctx, &st, query, args...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errStreamNotFound
		}

		return nil, err
	}

	return &st, nil
}

// CreateOrUpdateStream creates a new stream, or updates the description of an existing stream.
// Doesn't support updating a stream desctription to "". For that use the UpdateStream method.
func (s *Service) CreateOrUpdateStream(ctx context.Context, orgID platform.ID, stream influxdb.Stream) (*influxdb.ReadStream, error) {
	s.store.Mu.Lock()
	defer s.store.Mu.Unlock()

	newID := s.idGenerator.ID()
	now := time.Now()
	query, args, err := newUpsertStreamQuery(orgID, newID, now, stream)
	if err != nil {
		return nil, err
	}

	var id platform.ID
	if err = s.store.DB.GetContext(ctx, &id, query, args...); err != nil {
		return nil, err
	}

	// do a separate query to read the stream back from the database and return it.
	// this is necessary because the sqlite driver does not support scanning time values from
	// a RETURNING clause back into time.Time
	return s.getReadStream(ctx, id)
}

// UpdateStream updates a stream name and/or a description. It is strictly used for updating an existing stream.
func (s *Service) UpdateStream(ctx context.Context, id platform.ID, stream influxdb.Stream) (*influxdb.ReadStream, error) {
	s.store.Mu.Lock()
	defer s.store.Mu.Unlock()

	q := sq.Update("streams").
		SetMap(sq.Eq{
			"name":        stream.Name,
			"description": stream.Description,
			"updated_at":  sq.Expr(`datetime('now')`),
		}).
		Where(sq.Eq{"id": id}).
		Suffix(`RETURNING id`)

	query, args, err := q.ToSql()
	if err != nil {
		return nil, err
	}

	var newID platform.ID
	err = s.store.DB.GetContext(ctx, &newID, query, args...)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, errStreamNotFound
		}

		return nil, err
	}

	// do a separate query to read the stream back from the database and return it.
	// this is necessary because the sqlite driver does not support scanning time values from
	// a RETURNING clause back into time.Time
	return s.getReadStream(ctx, newID)
}

// DeleteStreams is used for deleting multiple streams by name
func (s *Service) DeleteStreams(ctx context.Context, orgID platform.ID, delete influxdb.BasicStream) error {
	s.store.Mu.Lock()
	defer s.store.Mu.Unlock()

	q := sq.Delete("streams").
		Where(sq.Eq{"org_id": orgID}).
		Where(sq.Eq{"name": delete.Names})

	query, args, err := q.ToSql()
	if err != nil {
		return err
	}

	_, err = s.store.DB.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}

	return nil
}

// DeleteStreamByID deletes a single stream by ID. Returns an error if the ID could not be found.
func (s *Service) DeleteStreamByID(ctx context.Context, id platform.ID) error {
	s.store.Mu.Lock()
	defer s.store.Mu.Unlock()

	q := sq.Delete("streams").
		Where(sq.Eq{"id": id}).
		Suffix("RETURNING id")

	query, args, err := q.ToSql()
	if err != nil {
		return err
	}

	var d platform.ID
	if err := s.store.DB.GetContext(ctx, &d, query, args...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return errStreamNotFound
		}

		return err
	}

	return nil
}

func newUpsertStreamQuery(orgID, newID platform.ID, t time.Time, stream influxdb.Stream) (string, []interface{}, error) {
	q := sq.Insert("streams").
		Columns("id", "org_id", "name", "description", "created_at", "updated_at").
		Values(newID, orgID, stream.Name, stream.Description, t, t).
		Suffix(`ON CONFLICT(org_id, name) DO UPDATE
		SET 
			updated_at = excluded.updated_at,
			description = IIF(length(excluded.description) = 0, description, excluded.description)`).
		Suffix("RETURNING id")

	return q.ToSql()
}

// getReadStream is a helper which should only be called when the stream has been verified to exist
// via an update or insert.
func (s *Service) getReadStream(ctx context.Context, id platform.ID) (*influxdb.ReadStream, error) {
	q := sq.Select("id", "name", "description", "created_at", "updated_at").
		From("streams").
		Where(sq.Eq{"id": id})

	query, args, err := q.ToSql()
	if err != nil {
		return nil, err
	}

	r := &influxdb.ReadStream{}
	if err := s.store.DB.GetContext(ctx, r, query, args...); err != nil {
		return nil, err
	}

	return r, nil
}

func storedAnnotationsToEvents(stored []influxdb.StoredAnnotation) ([]influxdb.AnnotationEvent, error) {
	events := make([]influxdb.AnnotationEvent, 0, len(stored))
	for _, s := range stored {
		c, err := s.ToCreate()
		if err != nil {
			return nil, err
		}

		events = append(events, influxdb.AnnotationEvent{
			ID:               s.ID,
			AnnotationCreate: *c,
		})
	}

	return events, nil
}

func timesToDuration(l, u time.Time) string {
	return fmt.Sprintf("[%s, %s]", l.Format(time.RFC3339Nano), u.Format(time.RFC3339Nano))
}

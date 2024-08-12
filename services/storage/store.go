package storage

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/storage/reads"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/cursors"
	"github.com/influxdata/influxql"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

var (
	ErrMissingReadSource = errors.New("missing ReadSource")
)

// GetReadSource will attempt to unmarshal a ReadSource from the ReadRequest or
// return an error if no valid resource is present.
func GetReadSource(any *anypb.Any) (*ReadSource, error) {
	if any == nil {
		return nil, ErrMissingReadSource
	}
	var source ReadSource
	if err := any.UnmarshalTo(&source); err != nil {
		return nil, err
	}
	return &source, nil
}

type MetaClient interface {
	Database(name string) *meta.DatabaseInfo
	ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []meta.ShardGroupInfo, err error)
}

type Store struct {
	TSDBStore  *tsdb.Store
	MetaClient MetaClient
	Logger     *zap.Logger
}

func NewStore(store *tsdb.Store, metaClient MetaClient) *Store {
	return &Store{
		TSDBStore:  store,
		MetaClient: metaClient,
		Logger:     zap.NewNop(),
	}
}

// WithLogger sets the logger for the service.
func (s *Store) WithLogger(log *zap.Logger) {
	s.Logger = log.With(zap.String("service", "store"))
}

func (s *Store) findShardIDs(database, rp string, desc bool, start, end int64) ([]uint64, error) {
	groups, err := s.MetaClient.ShardGroupsByTimeRange(database, rp, time.Unix(0, start), time.Unix(0, end))
	if err != nil {
		return nil, err
	}

	if len(groups) == 0 {
		return nil, nil
	}

	if desc {
		sort.Sort(sort.Reverse(meta.ShardGroupInfos(groups)))
	} else {
		sort.Sort(meta.ShardGroupInfos(groups))
	}

	shardIDs := make([]uint64, 0, len(groups[0].Shards)*len(groups))
	for _, g := range groups {
		for _, si := range g.Shards {
			shardIDs = append(shardIDs, si.ID)
		}
	}
	return shardIDs, nil
}

func (s *Store) validateArgs(database, rp string, start, end int64) (string, string, int64, int64, error) {
	di := s.MetaClient.Database(database)
	if di == nil {
		return "", "", 0, 0, errors.New("no database")
	}

	if rp == "" {
		rp = di.DefaultRetentionPolicy
	}

	rpi := di.RetentionPolicy(rp)
	if rpi == nil {
		return "", "", 0, 0, errors.New("invalid retention policy")
	}

	if start <= models.MinNanoTime {
		start = models.MinNanoTime
	}
	if end >= models.MaxNanoTime {
		end = models.MaxNanoTime
	}
	return database, rp, start, end, nil
}

func limitToShardIDs(list []uint64, allowed []uint64) []uint64 {
	shardOk := make(map[uint64]struct{}, len(allowed))
	for _, id := range allowed {
		shardOk[id] = struct{}{}
	}
	newShardIDs := make([]uint64, 0, len(list))
	for _, id := range list {
		if _, ok := shardOk[id]; ok {
			newShardIDs = append(newShardIDs, id)
		}
	}
	return newShardIDs
}

func (s *Store) ReadFilterLimit(ctx context.Context, req *datatypes.ReadFilterRequest, limitShardIDs []uint64) (reads.ResultSet, error) {
	if req.ReadSource == nil {
		return nil, errors.New("missing read source")
	}

	var database, rp string
	var start, end int64
	var shardIDs []uint64
	source, err := GetReadSource(req.ReadSource)

	if err != nil {
		return nil, err
	}
	database, rp, start, end, err = s.validateArgs(source.Database, source.RetentionPolicy, req.Range.GetStart(), req.Range.GetEnd())
	if err != nil {
		return nil, err
	}

	shardIDs, err = s.findShardIDs(database, rp, false, start, end)
	if err != nil {
		return nil, err
	}

	if len(limitShardIDs) > 0 {
		shardIDs = limitToShardIDs(shardIDs, limitShardIDs)
	}
	if len(shardIDs) == 0 { // TODO(jeff): this was a typed nil
		return nil, nil
	}

	var cur reads.SeriesCursor
	if ic, err := newIndexSeriesCursor(ctx, req.Predicate, s.TSDBStore.Shards(shardIDs)); err != nil {
		return nil, err
	} else if ic == nil { // TODO(jeff): this was a typed nil
		return nil, nil
	} else {
		cur = ic
	}

	req.Range = &datatypes.TimestampRange{
		Start: start,
		End:   end,
	}

	return reads.NewFilteredResultSet(ctx, req.Range.GetStart(), req.Range.GetEnd(), cur), nil
}

func (s *Store) ReadFilter(ctx context.Context, req *datatypes.ReadFilterRequest) (reads.ResultSet, error) {
	return s.ReadFilterLimit(ctx, req, nil)
}

func (s *Store) ReadGroupLimit(ctx context.Context, req *datatypes.ReadGroupRequest, limitShardIDs []uint64) (reads.GroupResultSet, error) {
	if req.ReadSource == nil {
		return nil, errors.New("missing read source")
	}

	source, err := GetReadSource(req.ReadSource)
	if err != nil {
		return nil, err
	}

	database, rp, start, end, err := s.validateArgs(source.Database, source.RetentionPolicy, req.Range.GetStart(), req.Range.GetEnd())
	if err != nil {
		return nil, err
	}

	// Due to some optimizations around how flux's `last()` function is implemented with the
	// storage engine, we need to detect if the read request requires a descending
	// cursor or not.
	descending := !reads.IsAscendingGroupAggregate(req)
	shardIDs, err := s.findShardIDs(database, rp, descending, start, end)
	if err != nil {
		return nil, err
	}
	if len(limitShardIDs) > 0 {
		shardIDs = limitToShardIDs(shardIDs, limitShardIDs)
	}
	if len(shardIDs) == 0 {
		return nil, nil
	}

	shards := s.TSDBStore.Shards(shardIDs)

	req.Range = &datatypes.TimestampRange{
		Start: start,
		End:   end,
	}

	newCursor := func() (reads.SeriesCursor, error) {
		cur, err := newIndexSeriesCursor(ctx, req.Predicate, shards)
		if cur == nil || err != nil {
			return nil, err
		}
		return cur, nil
	}

	rs := reads.NewGroupResultSet(ctx, req, newCursor)
	if rs == nil {
		return nil, nil
	}

	return rs, nil
}

func (s *Store) ReadGroup(ctx context.Context, req *datatypes.ReadGroupRequest) (reads.GroupResultSet, error) {
	return s.ReadGroupLimit(ctx, req, nil)
}

func (s *Store) WindowAggregateLimit(ctx context.Context, req *datatypes.ReadWindowAggregateRequest, limitShardIDs []uint64) (reads.ResultSet, error) {
	if req.ReadSource == nil {
		return nil, errors.New("missing read source")
	}

	source, err := GetReadSource(req.ReadSource)
	if err != nil {
		return nil, err
	}

	database, rp, start, end, err := s.validateArgs(source.Database, source.RetentionPolicy, req.Range.GetStart(), req.Range.GetEnd())
	if err != nil {
		return nil, err
	}

	// Due to some optimizations around how flux's `last()` function is implemented with the
	// storage engine, we need to detect if the read request requires a descending
	// cursor or not.
	descending := !reads.IsAscendingWindowAggregate(req)
	shardIDs, err := s.findShardIDs(database, rp, descending, start, end)
	if err != nil {
		return nil, err
	}
	if len(limitShardIDs) > 0 {
		shardIDs = limitToShardIDs(shardIDs, limitShardIDs)
	}
	if len(shardIDs) == 0 { // TODO(jeff): this was a typed nil
		return nil, nil
	}

	var cur reads.SeriesCursor
	if ic, err := newIndexSeriesCursor(ctx, req.Predicate, s.TSDBStore.Shards(shardIDs)); err != nil {
		return nil, err
	} else if ic == nil { // TODO(jeff): this was a typed nil
		return nil, nil
	} else {
		cur = ic
	}

	return reads.NewWindowAggregateResultSet(ctx, req, cur)
}

func (s *Store) WindowAggregate(ctx context.Context, req *datatypes.ReadWindowAggregateRequest) (reads.ResultSet, error) {
	return s.WindowAggregateLimit(ctx, req, nil)
}

func (s *Store) TagKeys(ctx context.Context, req *datatypes.TagKeysRequest) (cursors.StringIterator, error) {
	if req.TagsSource == nil {
		return nil, errors.New("missing read source")
	}

	source, err := GetReadSource(req.TagsSource)
	if err != nil {
		return nil, err
	}

	database, rp, start, end, err := s.validateArgs(source.Database, source.RetentionPolicy, req.Range.GetStart(), req.Range.GetEnd())
	if err != nil {
		return nil, err
	}

	shardIDs, err := s.findShardIDs(database, rp, false, start, end)
	if err != nil {
		return nil, err
	}
	if len(shardIDs) == 0 {
		return cursors.NewStringSliceIterator(nil), nil
	}

	var expr influxql.Expr
	if root := req.Predicate.GetRoot(); root != nil {
		var err error
		expr, err = reads.NodeToExpr(root, measurementRemap)
		if err != nil {
			return nil, err
		}

		if found := reads.HasFieldValueKey(expr); found {
			return nil, errors.New("field values unsupported")
		}
		expr = influxql.Reduce(influxql.CloneExpr(expr), nil)
		if reads.IsTrueBooleanLiteral(expr) {
			expr = nil
		}
	}

	// TODO(jsternberg): Use a real authorizer.
	auth := query.OpenAuthorizer
	keys, err := s.TSDBStore.TagKeys(ctx, auth, shardIDs, expr)
	if err != nil {
		return nil, err
	}

	m := map[string]bool{
		measurementKey: true,
		fieldKey:       true,
	}
	for _, ks := range keys {
		for _, k := range ks.Keys {
			m[k] = true
		}
	}

	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	sort.Strings(names)
	return cursors.NewStringSliceIterator(names), nil
}

func (s *Store) TagValues(ctx context.Context, req *datatypes.TagValuesRequest) (cursors.StringIterator, error) {
	if tagKey, ok := measurementRemap[req.TagKey]; ok {
		switch tagKey {
		case "_name":
			return s.MeasurementNames(ctx, &MeasurementNamesRequest{
				MeasurementsSource: req.TagsSource,
				Predicate:          req.Predicate,
			})
		}
	}

	if req.TagsSource == nil {
		return nil, errors.New("missing read source")
	}

	source, err := GetReadSource(req.TagsSource)
	if err != nil {
		return nil, err
	}

	database, rp, start, end, err := s.validateArgs(source.Database, source.RetentionPolicy, req.Range.GetStart(), req.Range.GetEnd())
	if err != nil {
		return nil, err
	}

	shardIDs, err := s.findShardIDs(database, rp, false, start, end)
	if err != nil {
		return nil, err
	}
	if len(shardIDs) == 0 {
		return cursors.NewStringSliceIterator(nil), nil
	}

	var expr influxql.Expr
	if root := req.Predicate.GetRoot(); root != nil {
		var err error
		expr, err = reads.NodeToExpr(root, measurementRemap)
		if err != nil {
			return nil, err
		}

		if found := reads.HasFieldValueKey(expr); found {
			return nil, errors.New("field values unsupported")
		}
		expr = influxql.Reduce(influxql.CloneExpr(expr), nil)
		if reads.IsTrueBooleanLiteral(expr) {
			expr = nil
		}
	}

	tagKeyExpr := &influxql.BinaryExpr{
		Op: influxql.EQ,
		LHS: &influxql.VarRef{
			Val: "_tagKey",
		},
		RHS: &influxql.StringLiteral{
			Val: req.TagKey,
		},
	}
	if expr != nil {
		expr = &influxql.BinaryExpr{
			Op:  influxql.AND,
			LHS: tagKeyExpr,
			RHS: &influxql.ParenExpr{
				Expr: expr,
			},
		}
	} else {
		expr = tagKeyExpr
	}

	// TODO(jsternberg): Use a real authorizer.
	auth := query.OpenAuthorizer
	values, err := s.TSDBStore.TagValues(ctx, auth, shardIDs, expr)
	if err != nil {
		return nil, err
	}

	m := make(map[string]struct{})
	for _, kvs := range values {
		for _, kv := range kvs.Values {
			m[kv.Value] = struct{}{}
		}
	}

	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	sort.Strings(names)
	return cursors.NewStringSliceIterator(names), nil
}

type MeasurementNamesRequest struct {
	MeasurementsSource *anypb.Any
	Predicate          *datatypes.Predicate
}

func (s *Store) MeasurementNames(ctx context.Context, req *MeasurementNamesRequest) (cursors.StringIterator, error) {
	if req.MeasurementsSource == nil {
		return nil, errors.New("missing read source")
	}

	source, err := GetReadSource(req.MeasurementsSource)
	if err != nil {
		return nil, err
	}

	database, _, _, _, err := s.validateArgs(source.Database, source.RetentionPolicy, -1, -1)
	if err != nil {
		return nil, err
	}

	var expr influxql.Expr
	if root := req.Predicate.GetRoot(); root != nil {
		var err error
		expr, err = reads.NodeToExpr(root, nil)
		if err != nil {
			return nil, err
		}

		if found := reads.HasFieldValueKey(expr); found {
			return nil, errors.New("field values unsupported")
		}
		expr = influxql.Reduce(influxql.CloneExpr(expr), nil)
		if reads.IsTrueBooleanLiteral(expr) {
			expr = nil
		}
	}

	// TODO(jsternberg): Use a real authorizer.
	auth := query.OpenAuthorizer
	// NOTE: this preserves the existing flux behaviour of ignoring the retention policy here
	values, err := s.TSDBStore.MeasurementNames(ctx, auth, database, "", expr)
	if err != nil {
		return nil, err
	}

	m := make(map[string]struct{})
	for _, name := range values {
		m[string(name)] = struct{}{}
	}

	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	sort.Strings(names)
	return cursors.NewStringSliceIterator(names), nil
}

func (s *Store) GetSource(db, rp string) proto.Message {
	return &ReadSource{Database: db, RetentionPolicy: rp}
}

func (s *Store) Delete(database string, sources []influxql.Source, condition influxql.Expr) error {
	return s.TSDBStore.DeleteSeries(database, sources, condition)
}

func (s *Store) DeleteRetentionPolicy(database, name string) error {
	return s.TSDBStore.DeleteRetentionPolicy(database, name)
}

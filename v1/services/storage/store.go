package storage

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/influxql/query"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/pkg/slices"
	"github.com/influxdata/influxdb/v2/storage/reads"
	"github.com/influxdata/influxdb/v2/storage/reads/datatypes"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/influxdata/influxdb/v2/tsdb/cursors"
	"github.com/influxdata/influxdb/v2/v1/services/meta"
	"github.com/influxdata/influxql"
	"go.uber.org/zap"
)

var (
	ErrMissingReadSource = errors.New("missing ReadSource")
)

type TSDBStore interface {
	MeasurementNames(ctx context.Context, auth query.Authorizer, database string, cond influxql.Expr) ([][]byte, error)
	ShardGroup(ids []uint64) tsdb.ShardGroup
	Shards(ids []uint64) []*tsdb.Shard
	TagKeys(ctx context.Context, auth query.Authorizer, shardIDs []uint64, cond influxql.Expr) ([]tsdb.TagKeys, error)
	TagValues(ctx context.Context, auth query.Authorizer, shardIDs []uint64, cond influxql.Expr) ([]tsdb.TagValues, error)
}

type MetaClient interface {
	Database(name string) *meta.DatabaseInfo
	ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []meta.ShardGroupInfo, err error)
}

// getReadSource will attempt to unmarshal a ReadSource from the ReadRequest or
// return an error if no valid resource is present.
func GetReadSource(any types.Any) (*ReadSource, error) {
	var source ReadSource
	if err := types.UnmarshalAny(&any, &source); err != nil {
		return nil, err
	}
	return &source, nil
}

type Store struct {
	TSDBStore  TSDBStore
	MetaClient MetaClient
	Logger     *zap.Logger
}

func (s *Store) WindowAggregate(ctx context.Context, req *datatypes.ReadWindowAggregateRequest) (reads.ResultSet, error) {
	if req.ReadSource == nil {
		return nil, errors.New("missing read source")
	}

	source, err := getReadSource(*req.ReadSource)
	if err != nil {
		return nil, err
	}

	database, rp, start, end, err := s.validateArgs(source.OrganizationID, source.BucketID, req.Range.Start, req.Range.End)
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

func NewStore(store TSDBStore, metaClient MetaClient) *Store {
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

func (s *Store) validateArgs(orgID, bucketID uint64, start, end int64) (string, string, int64, int64, error) {
	database := influxdb.ID(bucketID).String()
	rp := meta.DefaultRetentionPolicyName

	di := s.MetaClient.Database(database)
	if di == nil {
		return "", "", 0, 0, errors.New("no database")
	}

	rpi := di.RetentionPolicy(rp)
	if rpi == nil {
		return "", "", 0, 0, errors.New("invalid retention policy")
	}

	if start <= 0 {
		start = models.MinNanoTime
	}
	if end <= 0 {
		end = models.MaxNanoTime
	}
	return database, rp, start, end, nil
}

func (s *Store) ReadFilter(ctx context.Context, req *datatypes.ReadFilterRequest) (reads.ResultSet, error) {
	if req.ReadSource == nil {
		return nil, errors.New("missing read source")
	}

	source, err := getReadSource(*req.ReadSource)
	if err != nil {
		return nil, err
	}

	database, rp, start, end, err := s.validateArgs(source.OrganizationID, source.BucketID, req.Range.Start, req.Range.End)
	if err != nil {
		return nil, err
	}

	shardIDs, err := s.findShardIDs(database, rp, false, start, end)
	if err != nil {
		return nil, err
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

	req.Range.Start = start
	req.Range.End = end

	return reads.NewFilteredResultSet(ctx, req.Range.Start, req.Range.End, cur), nil
}

func (s *Store) ReadGroup(ctx context.Context, req *datatypes.ReadGroupRequest) (reads.GroupResultSet, error) {
	if req.ReadSource == nil {
		return nil, errors.New("missing read source")
	}

	source, err := getReadSource(*req.ReadSource)
	if err != nil {
		return nil, err
	}

	database, rp, start, end, err := s.validateArgs(source.OrganizationID, source.BucketID, req.Range.Start, req.Range.End)
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
	if len(shardIDs) == 0 {
		return nil, nil
	}

	shards := s.TSDBStore.Shards(shardIDs)

	req.Range.Start = start
	req.Range.End = end

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

type metaqueryAttributes struct {
	orgID      influxdb.ID
	db, rp     string
	start, end int64
	pred       influxql.Expr
}

func (s *Store) tagKeysWithFieldPredicate(ctx context.Context, mqAttrs *metaqueryAttributes, shardIDs []uint64) (cursors.StringIterator, error) {
	var cur reads.SeriesCursor
	if ic, err := newIndexSeriesCursorInfluxQLPred(ctx, mqAttrs.pred, s.TSDBStore.Shards(shardIDs)); err != nil {
		return nil, err
	} else if ic == nil {
		return cursors.EmptyStringIterator, nil
	} else {
		cur = ic
	}
	m := make(map[string]struct{})
	rs := reads.NewFilteredResultSet(ctx, mqAttrs.start, mqAttrs.end, cur)
	for rs.Next() {
		func() {
			c := rs.Cursor()
			if c == nil {
				// no data for series key + field combination
				return
			}
			defer c.Close()
			if cursorHasData(c) {
				tags := rs.Tags()
				for i := range tags {
					m[string(tags[i].Key)] = struct{}{}
				}
			}
		}()
	}

	arr := make([]string, 0, len(m))
	for tag := range m {
		arr = append(arr, tag)
	}
	sort.Strings(arr)
	return cursors.NewStringSliceIterator(arr), nil
}

func (s *Store) TagKeys(ctx context.Context, req *datatypes.TagKeysRequest) (cursors.StringIterator, error) {
	if req.TagsSource == nil {
		return nil, errors.New("missing read source")
	}
	source, err := getReadSource(*req.TagsSource)
	if err != nil {
		return nil, err
	}
	db, rp, start, end, err := s.validateArgs(source.OrganizationID, source.BucketID, req.Range.Start, req.Range.End)
	if err != nil {
		return nil, err
	}

	shardIDs, err := s.findShardIDs(db, rp, false, start, end)
	if err != nil {
		return nil, err
	}
	if len(shardIDs) == 0 {
		return cursors.EmptyStringIterator, nil
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
		if found := reads.ExprHasKey(expr, fieldKey); found {
			mqAttrs := &metaqueryAttributes{
				orgID: source.GetOrgID(),
				db:    db,
				rp:    rp,
				start: start,
				end:   end,
				pred:  expr,
			}
			return s.tagKeysWithFieldPredicate(ctx, mqAttrs, shardIDs)
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
		return cursors.EmptyStringIterator, err
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
	if req.TagsSource == nil {
		return nil, errors.New("missing read source")
	}

	source, err := getReadSource(*req.TagsSource)
	if err != nil {
		return nil, err
	}

	db, rp, start, end, err := s.validateArgs(source.OrganizationID, source.BucketID, req.Range.Start, req.Range.End)
	if err != nil {
		return nil, err
	}

	var influxqlPred influxql.Expr
	if root := req.Predicate.GetRoot(); root != nil {
		var err error
		influxqlPred, err = reads.NodeToExpr(root, measurementRemap)
		if err != nil {
			return nil, err
		}

		if found := reads.HasFieldValueKey(influxqlPred); found {
			return nil, errors.New("field values unsupported")
		}

		influxqlPred = influxql.Reduce(influxql.CloneExpr(influxqlPred), nil)
		if reads.IsTrueBooleanLiteral(influxqlPred) {
			influxqlPred = nil
		}
	}

	mqAttrs := &metaqueryAttributes{
		orgID: source.GetOrgID(),
		db:    db,
		rp:    rp,
		start: start,
		end:   end,
		pred:  influxqlPred,
	}

	tagKey, ok := measurementRemap[req.TagKey]
	if !ok {
		tagKey = req.TagKey
	}

	// Getting values of _measurement or _field are handled specially
	switch tagKey {
	case "_name":
		return s.MeasurementNames(ctx, mqAttrs)

	case "_field":
		return s.measurementFields(ctx, mqAttrs)
	}

	return s.tagValues(ctx, mqAttrs, tagKey)
}

func (s *Store) tagValues(ctx context.Context, mqAttrs *metaqueryAttributes, tagKey string) (cursors.StringIterator, error) {
	// If there are any references to _field, we need to use the slow path
	// since we cannot rely on the index alone.
	if mqAttrs.pred != nil {
		if hasFieldKey := reads.ExprHasKey(mqAttrs.pred, fieldKey); hasFieldKey {
			return s.tagValuesSlow(ctx, mqAttrs, tagKey)
		}
	}

	shardIDs, err := s.findShardIDs(mqAttrs.db, mqAttrs.rp, false, mqAttrs.start, mqAttrs.end)
	if err != nil {
		return nil, err
	}
	if len(shardIDs) == 0 {
		return cursors.EmptyStringIterator, nil
	}

	tagKeyExpr := &influxql.BinaryExpr{
		Op: influxql.EQ,
		LHS: &influxql.VarRef{
			Val: "_tagKey",
		},
		RHS: &influxql.StringLiteral{
			Val: tagKey,
		},
	}

	if mqAttrs.pred != nil {
		mqAttrs.pred = &influxql.BinaryExpr{
			Op:  influxql.AND,
			LHS: tagKeyExpr,
			RHS: &influxql.ParenExpr{
				Expr: mqAttrs.pred,
			},
		}
	} else {
		mqAttrs.pred = tagKeyExpr
	}

	// TODO(jsternberg): Use a real authorizer.
	auth := query.OpenAuthorizer
	values, err := s.TSDBStore.TagValues(ctx, auth, shardIDs, mqAttrs.pred)
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

func (s *Store) MeasurementNames(ctx context.Context, mqAttrs *metaqueryAttributes) (cursors.StringIterator, error) {
	if mqAttrs.pred != nil {
		if hasFieldKey := reads.ExprHasKey(mqAttrs.pred, fieldKey); hasFieldKey {
			// If there is a predicate on _field, we cannot use the index
			// to filter out unwanted measurement names. Use a slower
			// block scan instead.
			return s.tagValuesSlow(ctx, mqAttrs, measurementKey)
		}
	}

	// TODO(jsternberg): Use a real authorizer.
	auth := query.OpenAuthorizer
	values, err := s.TSDBStore.MeasurementNames(ctx, auth, mqAttrs.db, mqAttrs.pred)
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

func (s *Store) GetSource(orgID, bucketID uint64) proto.Message {
	return &readSource{
		BucketID:       bucketID,
		OrganizationID: orgID,
	}
}

func (s *Store) measurementFields(ctx context.Context, mqAttrs *metaqueryAttributes) (cursors.StringIterator, error) {
	if mqAttrs.pred != nil {
		if hasFieldKey := reads.ExprHasKey(mqAttrs.pred, fieldKey); hasFieldKey {
			return s.tagValuesSlow(ctx, mqAttrs, fieldKey)
		}

		// If there predicates on anything besides _measurement, we can't
		// use the index and need to use the slow path.
		if hasTagKey(mqAttrs.pred) {
			return s.tagValuesSlow(ctx, mqAttrs, fieldKey)
		}
	}

	shardIDs, err := s.findShardIDs(mqAttrs.db, mqAttrs.rp, false, mqAttrs.start, mqAttrs.end)
	if err != nil {
		return nil, err
	}
	if len(shardIDs) == 0 {
		return cursors.EmptyStringIterator, nil
	}

	sg := s.TSDBStore.ShardGroup(shardIDs)
	ms := &influxql.Measurement{
		Database:        mqAttrs.db,
		RetentionPolicy: mqAttrs.rp,
		SystemIterator:  "_fieldKeys",
	}
	opts := query.IteratorOptions{
		OrgID:      mqAttrs.orgID,
		Condition:  mqAttrs.pred,
		Authorizer: query.OpenAuthorizer,
	}
	iter, err := sg.CreateIterator(ctx, ms, opts)
	if err != nil {
		return nil, err
	}
	defer func() { _ = iter.Close() }()

	var fieldNames []string
	fitr := iter.(query.FloatIterator)
	for p, _ := fitr.Next(); p != nil; p, _ = fitr.Next() {
		if len(p.Aux) >= 1 {
			fieldNames = append(fieldNames, p.Aux[0].(string))
		}
	}

	sort.Strings(fieldNames)
	fieldNames = slices.MergeSortedStrings(fieldNames)

	return cursors.NewStringSliceIterator(fieldNames), nil
}

func cursorHasData(c cursors.Cursor) bool {
	var l int
	switch typedCur := c.(type) {
	case cursors.IntegerArrayCursor:
		ia := typedCur.Next()
		l = ia.Len()
	case cursors.FloatArrayCursor:
		ia := typedCur.Next()
		l = ia.Len()
	case cursors.UnsignedArrayCursor:
		ia := typedCur.Next()
		l = ia.Len()
	case cursors.BooleanArrayCursor:
		ia := typedCur.Next()
		l = ia.Len()
	case cursors.StringArrayCursor:
		ia := typedCur.Next()
		l = ia.Len()
	default:
		panic(fmt.Sprintf("unreachable: %T", typedCur))
	}
	return l != 0
}

// tagValuesSlow will determine the tag values for the given tagKey.
// It's generally faster to use tagValues, measurementFields or
// MeasurementNames, but those methods will only use the index and metadata
// stored in the shard. Because fields are not themselves indexed, we have no way
// of correlating fields to tag values, so we sometimes need to consult tsm to
// provide an accurate answer.
func (s *Store) tagValuesSlow(ctx context.Context, mqAttrs *metaqueryAttributes, tagKey string) (cursors.StringIterator, error) {
	shardIDs, err := s.findShardIDs(mqAttrs.db, mqAttrs.rp, false, mqAttrs.start, mqAttrs.end)
	if err != nil {
		return nil, err
	}
	if len(shardIDs) == 0 {
		return cursors.EmptyStringIterator, nil
	}

	var cur reads.SeriesCursor
	if ic, err := newIndexSeriesCursorInfluxQLPred(ctx, mqAttrs.pred, s.TSDBStore.Shards(shardIDs)); err != nil {
		return nil, err
	} else if ic == nil {
		return cursors.EmptyStringIterator, nil
	} else {
		cur = ic
	}
	m := make(map[string]struct{})

	rs := reads.NewFilteredResultSet(ctx, mqAttrs.start, mqAttrs.end, cur)
	for rs.Next() {
		func() {
			c := rs.Cursor()
			if c == nil {
				// no data for series key + field combination?
				// It seems that even when there is no data for this series key + field
				// combo that the cursor may be not nil. We need to
				// request invoke an array cursor to be sure.
				// This is the reason for the call to cursorHasData below.
				return
			}
			defer c.Close()

			if cursorHasData(c) {
				f := rs.Tags().Get([]byte(tagKey))
				m[string(f)] = struct{}{}
			}
		}()
	}

	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	sort.Strings(names)
	return cursors.NewStringSliceIterator(names), nil
}

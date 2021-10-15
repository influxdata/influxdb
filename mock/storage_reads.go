package mock

import (
	"context"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/storage"
	"github.com/influxdata/influxdb/storage/reads"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
	"github.com/influxdata/influxdb/tsdb/cursors"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

type ResponseStream struct {
	SendFunc       func(*datatypes.ReadResponse) error
	SetTrailerFunc func(metadata.MD)
}

func NewResponseStream() *ResponseStream {
	return &ResponseStream{
		SendFunc:       func(*datatypes.ReadResponse) error { return nil },
		SetTrailerFunc: func(mds metadata.MD) {},
	}
}

func (s *ResponseStream) Send(r *datatypes.ReadResponse) error {
	return s.SendFunc(r)
}

func (s *ResponseStream) SetTrailer(m metadata.MD) {
	s.SetTrailerFunc(m)
}

type ResultSet struct {
	NextFunc   func() bool
	CursorFunc func() cursors.Cursor
	TagsFunc   func() models.Tags
	CloseFunc  func()
	ErrFunc    func() error
	StatsFunc  func() cursors.CursorStats
}

func NewResultSet() *ResultSet {
	return &ResultSet{
		NextFunc:   func() bool { return false },
		CursorFunc: func() cursors.Cursor { return nil },
		TagsFunc:   func() models.Tags { return nil },
		CloseFunc:  func() {},
		ErrFunc:    func() error { return nil },
		StatsFunc:  func() cursors.CursorStats { return cursors.CursorStats{} },
	}
}

func (rs *ResultSet) Next() bool {
	return rs.NextFunc()
}

func (rs *ResultSet) Cursor() cursors.Cursor {
	return rs.CursorFunc()
}

func (rs *ResultSet) Tags() models.Tags {
	return rs.TagsFunc()
}

func (rs *ResultSet) Close() {
	rs.CloseFunc()
}

func (rs *ResultSet) Err() error {
	return rs.ErrFunc()
}

func (rs *ResultSet) Stats() cursors.CursorStats {
	return rs.StatsFunc()
}

type GroupResultSet struct {
	NextFunc  func() reads.GroupCursor
	CloseFunc func()
	ErrFunc   func() error
}

func NewGroupResultSet() *GroupResultSet {
	return &GroupResultSet{
		NextFunc:  func() reads.GroupCursor { return nil },
		CloseFunc: func() {},
		ErrFunc:   func() error { return nil },
	}
}

func (rs *GroupResultSet) Next() reads.GroupCursor {
	return rs.NextFunc()
}

func (rs *GroupResultSet) Close() {
	rs.CloseFunc()
}

func (rs *GroupResultSet) Err() error {
	return rs.ErrFunc()
}

type FloatArrayCursor struct {
	CloseFunc func()
	Errfunc   func() error
	StatsFunc func() cursors.CursorStats
	NextFunc  func() *cursors.FloatArray
}

func NewFloatArrayCursor() *FloatArrayCursor {
	return &FloatArrayCursor{
		CloseFunc: func() {},
		Errfunc:   func() error { return nil },
		StatsFunc: func() cursors.CursorStats { return cursors.CursorStats{} },
		NextFunc:  func() *cursors.FloatArray { return &cursors.FloatArray{} },
	}
}

func (c *FloatArrayCursor) Close() {
	c.CloseFunc()
}

func (c *FloatArrayCursor) Err() error {
	return c.Errfunc()
}

func (c *FloatArrayCursor) Stats() cursors.CursorStats {
	return c.StatsFunc()
}

func (c *FloatArrayCursor) Next() *cursors.FloatArray {
	return c.NextFunc()
}

type IntegerArrayCursor struct {
	CloseFunc func()
	Errfunc   func() error
	StatsFunc func() cursors.CursorStats
	NextFunc  func() *cursors.IntegerArray
}

func NewIntegerArrayCursor() *IntegerArrayCursor {
	return &IntegerArrayCursor{
		CloseFunc: func() {},
		Errfunc:   func() error { return nil },
		StatsFunc: func() cursors.CursorStats { return cursors.CursorStats{} },
		NextFunc:  func() *cursors.IntegerArray { return &cursors.IntegerArray{} },
	}
}

func (c *IntegerArrayCursor) Close() {
	c.CloseFunc()
}

func (c *IntegerArrayCursor) Err() error {
	return c.Errfunc()
}

func (c *IntegerArrayCursor) Stats() cursors.CursorStats {
	return c.StatsFunc()
}

func (c *IntegerArrayCursor) Next() *cursors.IntegerArray {
	return c.NextFunc()
}

type GroupCursor struct {
	NextFunc             func() bool
	CursorFunc           func() cursors.Cursor
	TagsFunc             func() models.Tags
	KeysFunc             func() [][]byte
	PartitionKeyValsFunc func() [][]byte
	CloseFunc            func()
	ErrFunc              func() error
	StatsFunc            func() cursors.CursorStats
	AggregateFunc        func() *datatypes.Aggregate
}

func NewGroupCursor() *GroupCursor {
	return &GroupCursor{
		NextFunc:             func() bool { return false },
		CursorFunc:           func() cursors.Cursor { return nil },
		TagsFunc:             func() models.Tags { return nil },
		KeysFunc:             func() [][]byte { return nil },
		PartitionKeyValsFunc: func() [][]byte { return nil },
		CloseFunc:            func() {},
		ErrFunc:              func() error { return nil },
		StatsFunc:            func() cursors.CursorStats { return cursors.CursorStats{} },
		AggregateFunc:        func() *datatypes.Aggregate { return nil },
	}
}

func (c *GroupCursor) Next() bool {
	return c.NextFunc()
}

func (c *GroupCursor) Cursor() cursors.Cursor {
	return c.CursorFunc()
}

func (c *GroupCursor) Tags() models.Tags {
	return c.TagsFunc()
}

func (c *GroupCursor) Keys() [][]byte {
	return c.KeysFunc()
}

func (c *GroupCursor) PartitionKeyVals() [][]byte {
	return c.PartitionKeyValsFunc()
}

func (c *GroupCursor) Close() {
	c.CloseFunc()
}

func (c *GroupCursor) Err() error {
	return c.ErrFunc()
}

func (c *GroupCursor) Stats() cursors.CursorStats {
	return c.StatsFunc()
}

func (c *GroupCursor) Aggregate() *datatypes.Aggregate {
	return c.AggregateFunc()
}

type StoreReader struct {
	ReadFilterFunc      func(ctx context.Context, req *datatypes.ReadFilterRequest) (reads.ResultSet, error)
	ReadGroupFunc       func(ctx context.Context, req *datatypes.ReadGroupRequest) (reads.GroupResultSet, error)
	WindowAggregateFunc func(ctx context.Context, req *datatypes.ReadWindowAggregateRequest) (reads.ResultSet, error)
	TagKeysFunc         func(ctx context.Context, req *datatypes.TagKeysRequest) (cursors.StringIterator, error)
	TagValuesFunc       func(ctx context.Context, req *datatypes.TagValuesRequest) (cursors.StringIterator, error)
}

func NewStoreReader() *StoreReader {
	return &StoreReader{}
}

func (s *StoreReader) ReadFilter(ctx context.Context, req *datatypes.ReadFilterRequest) (reads.ResultSet, error) {
	return s.ReadFilterFunc(ctx, req)
}

func (s *StoreReader) ReadGroup(ctx context.Context, req *datatypes.ReadGroupRequest) (reads.GroupResultSet, error) {
	return s.ReadGroupFunc(ctx, req)
}

func (s *StoreReader) WindowAggregate(ctx context.Context, req *datatypes.ReadWindowAggregateRequest) (reads.ResultSet, error) {
	return s.WindowAggregateFunc(ctx, req)
}

func (s *StoreReader) TagKeys(ctx context.Context, req *datatypes.TagKeysRequest) (cursors.StringIterator, error) {
	return s.TagKeysFunc(ctx, req)
}

func (s *StoreReader) TagValues(ctx context.Context, req *datatypes.TagValuesRequest) (cursors.StringIterator, error) {
	return s.TagValuesFunc(ctx, req)
}

func (*StoreReader) GetSource(db, rp string) proto.Message {
	return &storage.ReadSource{Database: db, RetentionPolicy: rp}
}

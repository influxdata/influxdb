package mock

import (
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/storage/reads"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
	"github.com/influxdata/influxdb/tsdb/cursors"
	"google.golang.org/grpc/metadata"
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

package wal

import (
	"code.google.com/p/goprotobuf/proto"
	"configuration"
	. "launchpad.net/gocheck"
	"os"
	"path"
	"protocol"
	"testing"
	"time"
)

func Test(t *testing.T) {
	TestingT(t)
}

type WalSuite struct{}

var _ = Suite(&WalSuite{})

func generateSeries(numberOfPoints int) *protocol.Series {
	timestamp := proto.Int64(int64(time.Now().Second()))

	points := []*protocol.Point{}
	for i := 0; i < numberOfPoints; i++ {
		points = append(points, &protocol.Point{
			Timestamp: timestamp,
			Values: []*protocol.FieldValue{
				&protocol.FieldValue{Int64Value: proto.Int64(int64(i))},
			},
		})
	}

	return &protocol.Series{
		Name:   proto.String("some_series"),
		Fields: []string{"columns"},
		Points: points,
	}
}

func generateRequest(numberOfPoints int) *protocol.Request {
	requestType := protocol.Request_PROXY_WRITE
	return &protocol.Request{
		Id:       proto.Uint32(1),
		Database: proto.String("db"),
		Type:     &requestType,
		Series:   generateSeries(numberOfPoints),
	}
}

func newWal(c *C) *WAL {
	dir := c.MkDir()
	config := &configuration.Configuration{
		WalDir: dir,
		WalBookmarkAfterRequests: 1000,
		WalIndexAfterRequests:    1000,
		WalFlushAfterRequests:    1000,
	}
	wal, err := NewWAL(config)
	c.Assert(err, IsNil)
	wal.SetServerId(1)
	return wal
}

func (_ *WalSuite) TestRequestNumberAssignment(c *C) {
	wal := newWal(c)
	request := generateRequest(2)
	id, err := wal.AssignSequenceNumbersAndLog(request, &MockShard{id: 1})
	c.Assert(err, IsNil)
	c.Assert(id, Equals, uint32(1))
	id, err = wal.AssignSequenceNumbersAndLog(request, &MockShard{id: 1})
	c.Assert(err, IsNil)
	c.Assert(id, Equals, uint32(2))
}

func (_ *WalSuite) TestRequestNumberAssignmentRecovery(c *C) {
	wal := newWal(c)
	request := generateRequest(2)
	id, err := wal.AssignSequenceNumbersAndLog(request, &MockShard{id: 1})
	c.Assert(err, IsNil)
	c.Assert(id, Equals, uint32(1))
	c.Assert(wal.Close(), IsNil)
	wal, err = NewWAL(wal.config)
	c.Assert(err, IsNil)
	request = generateRequest(2)
	id, err = wal.AssignSequenceNumbersAndLog(request, &MockShard{id: 1})
	c.Assert(err, IsNil)
	c.Assert(id, Equals, uint32(2))

	// test recovery from wal replay
	c.Assert(wal.log.closeWithoutBookmark(), IsNil)
	wal, err = NewWAL(wal.config)
	c.Assert(err, IsNil)
	request = generateRequest(2)
	id, err = wal.AssignSequenceNumbersAndLog(request, &MockShard{id: 1})
	c.Assert(err, IsNil)
	c.Assert(id, Equals, uint32(3))
}

func (_ *WalSuite) TestAutoBookmark(c *C) {
	wal := newWal(c)
	wal.config.WalBookmarkAfterRequests = 2
	wal.log.bookmarkSize = 2
	for i := 0; i < 2; i++ {
		request := generateRequest(2)
		id, err := wal.AssignSequenceNumbersAndLog(request, &MockShard{id: 1})
		c.Assert(err, IsNil)
		c.Assert(id, Equals, uint32(i+1))
	}
	// make sure the bookmark exist
	bookmarkPath := path.Join(wal.config.WalDir, "bookmark")
	f, err := os.Open(bookmarkPath)
	c.Assert(err, IsNil)
	s := &state{}
	err = s.read(f)
	c.Assert(err, IsNil)
	c.Assert(s.ShardLastSequenceNumber[1], Equals, uint64(4))
	c.Assert(s.CurrentRequestNumber, Equals, uint32(2))
}

func (_ *WalSuite) TestAutoBookmarkAfterRecovery(c *C) {
	wal := newWal(c)
	wal.config.WalBookmarkAfterRequests = 2
	wal.log.bookmarkSize = 2
	request := generateRequest(2)
	id, err := wal.AssignSequenceNumbersAndLog(request, &MockShard{id: 1})
	c.Assert(err, IsNil)
	c.Assert(id, Equals, uint32(1))
	// close andn reopen the wal
	wal.Close()
	for i := 0; i < 2; i++ {
		wal, err = NewWAL(wal.config)
		c.Assert(err, IsNil)
		request = generateRequest(2)
		id, err = wal.AssignSequenceNumbersAndLog(request, &MockShard{id: 1})
		c.Assert(err, IsNil)
		c.Assert(id, Equals, uint32(i+2))
	}
	// make sure the bookmark exist
	bookmarkPath := path.Join(wal.config.WalDir, "bookmark")
	f, err := os.Open(bookmarkPath)
	c.Assert(err, IsNil)
	s := &state{}
	err = s.read(f)
	c.Assert(err, IsNil)
	c.Assert(s.ShardLastSequenceNumber[1], Equals, uint64(6))
	c.Assert(s.CurrentRequestNumber, Equals, uint32(3))
}

func (_ *WalSuite) TestAutoBookmarkShouldntHappenTooOften(c *C) {
	wal := newWal(c)
	wal.config.WalBookmarkAfterRequests = 3
	wal.log.bookmarkSize = 3
	for i := 0; i < 2; i++ {
		request := generateRequest(2)
		id, err := wal.AssignSequenceNumbersAndLog(request, &MockShard{id: 1})
		c.Assert(err, IsNil)
		c.Assert(id, Equals, uint32(i+1))
	}
	// make sure the bookmark exist
	bookmarkPath := path.Join(wal.config.WalDir, "bookmark")
	_, err := os.Open(bookmarkPath)
	c.Assert(os.IsNotExist(err), Equals, true)
}

func (_ *WalSuite) TestReplay(c *C) {
	wal := newWal(c)
	request := generateRequest(1)
	id, err := wal.AssignSequenceNumbersAndLog(request, &MockShard{id: 1})
	c.Assert(err, IsNil)
	c.Assert(id, Equals, uint32(1))
	request = generateRequest(2)
	id, err = wal.AssignSequenceNumbersAndLog(request, &MockShard{id: 2})
	c.Assert(err, IsNil)
	c.Assert(id, Equals, uint32(2))
	request = generateRequest(3)
	id, err = wal.AssignSequenceNumbersAndLog(request, &MockShard{id: 1})
	c.Assert(err, IsNil)
	c.Assert(id, Equals, uint32(3))

	requests := []*protocol.Request{}
	wal.RecoverServerFromRequestNumber(uint32(2), []uint32{1}, func(req *protocol.Request, shardId uint32) error {
		requests = append(requests, req)
		return nil
	})
	c.Assert(requests, HasLen, 1)
	c.Assert(requests[0].Series.Points, HasLen, 3)
	c.Assert(err, IsNil)
}

func (_ *WalSuite) TestIndexAfterRecovery(c *C) {
	wal := newWal(c)
	for i := 0; i < 1500; i++ {
		request := generateRequest(2)
		id, err := wal.AssignSequenceNumbersAndLog(request, &MockShard{id: 1})
		c.Assert(err, IsNil)
		c.Assert(id, Equals, uint32(i+1))
	}

	wal.log.closeWithoutBookmark()

	wal, err := NewWAL(wal.config)
	c.Assert(err, IsNil)

	for i := 0; i < 500; i++ {
		request := generateRequest(2)
		id, err := wal.AssignSequenceNumbersAndLog(request, &MockShard{id: 1})
		c.Assert(err, IsNil)
		c.Assert(id, Equals, uint32(1500+i+1))
	}
	c.Assert(wal.log.state.Index.Entries, HasLen, 2)
	c.Assert(wal.log.state.Index.Entries[0].Size, Equals, uint32(1000))
	c.Assert(wal.log.state.Index.Entries[0].StartRequestNumber, Equals, uint32(1))
	c.Assert(wal.log.state.Index.Entries[1].Size, Equals, uint32(1000))
	c.Assert(wal.log.state.Index.Entries[1].StartRequestNumber, Equals, uint32(1001))
}

func (_ *WalSuite) TestIndex(c *C) {
	wal := newWal(c)
	for i := 0; i < 3000; i++ {
		request := generateRequest(1)
		id, err := wal.AssignSequenceNumbersAndLog(request, &MockShard{id: 1})
		c.Assert(err, IsNil)
		c.Assert(id, Equals, uint32(i+1))
	}

	c.Assert(wal.log.state.Index.Entries, HasLen, 3)
	requestOffset := wal.log.state.Index.requestOffset(2001)
	c.Assert(requestOffset > 0, Equals, true)
	// request 2000 should be in the second block not the third block
	c.Assert(requestOffset > wal.log.state.Index.requestOffset(2000), Equals, true)
}

func (_ *WalSuite) TestSequenceNumberAssignment(c *C) {
	wal := newWal(c)
	request := generateRequest(2)
	_, err := wal.AssignSequenceNumbersAndLog(request, &MockShard{id: 1})
	c.Assert(err, IsNil)
	c.Assert(request.Series.Points[0].GetSequenceNumber(), Equals, uint64(1))
	c.Assert(request.Series.Points[1].GetSequenceNumber(), Equals, uint64(2))
	request = generateRequest(2)
	_, err = wal.AssignSequenceNumbersAndLog(request, &MockShard{id: 1})
	c.Assert(err, IsNil)
	c.Assert(request.Series.Points[0].GetSequenceNumber(), Equals, uint64(3))
	c.Assert(request.Series.Points[1].GetSequenceNumber(), Equals, uint64(4))
}

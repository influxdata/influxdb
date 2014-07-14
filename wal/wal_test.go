package wal

import (
	"fmt"
	"math"
	"os"
	"path"
	"testing"
	"time"

	"code.google.com/p/goprotobuf/proto"
	logger "code.google.com/p/log4go"
	. "github.com/influxdb/influxdb/checkers"
	"github.com/influxdb/influxdb/configuration"
	"github.com/influxdb/influxdb/protocol"
	. "launchpad.net/gocheck"
)

func Test(t *testing.T) {
	TestingT(t)
}

type WalSuite struct{}

var _ = Suite(&WalSuite{})

func (_ *WalSuite) SetUpSuite(c *C) {
	for _, filter := range logger.Global {
		filter.Level = logger.INFO
	}
}

func generateSeries(numberOfPoints int) *protocol.Series {
	timestamp := proto.Int64(int64(time.Now().Second()))

	points := []*protocol.Point{}
	for i := 0; i < numberOfPoints; i++ {
		points = append(points, &protocol.Point{
			Timestamp: timestamp,
			Values: []*protocol.FieldValue{
				{Int64Value: proto.Int64(int64(i))},
			},
		})
	}

	return &protocol.Series{
		Name:   proto.String("some_series"),
		Fields: []string{"columns"},
		Points: points,
	}
}

func generateSerieses(numberOfPoints int) []*protocol.Series {
	return []*protocol.Series{generateSeries(numberOfPoints)}
}

func generateRequest(numberOfPoints int) *protocol.Request {
	requestType := protocol.Request_WRITE
	return &protocol.Request{
		Id:          proto.Uint32(1),
		Database:    proto.String("db"),
		Type:        &requestType,
		MultiSeries: generateSerieses(numberOfPoints),
	}
}

func newWal(c *C) *WAL {
	dir := c.MkDir()
	config := &configuration.Configuration{
		WalDir: dir,
		WalBookmarkAfterRequests: 1000,
		WalIndexAfterRequests:    1000,
		WalFlushAfterRequests:    1000,
		WalRequestsPerLogFile:    10000,
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
	c.Assert(wal.closeWithoutBookmarking(), IsNil)
	wal, err = NewWAL(wal.config)
	wal.SetServerId(1)
	c.Assert(err, IsNil)
	request = generateRequest(2)
	id, err = wal.AssignSequenceNumbersAndLog(request, &MockShard{id: 1})
	c.Assert(err, IsNil)
	c.Assert(id, Equals, uint32(2))

	// test recovery from wal replay
	c.Assert(wal.closeWithoutBookmarking(), IsNil)
	wal, err = NewWAL(wal.config)
	wal.SetServerId(1)
	c.Assert(err, IsNil)
	request = generateRequest(2)
	id, err = wal.AssignSequenceNumbersAndLog(request, &MockShard{id: 1})
	c.Assert(err, IsNil)
	c.Assert(id, Equals, uint32(3))
}

func (_ *WalSuite) TestLogFilesReplay(c *C) {
	wal := newWal(c)
	wal.config.WalRequestsPerLogFile = 1000
	wal.Commit(1, 0)
	for i := 0; i < 4000; i++ {
		request := generateRequest(2)
		id, err := wal.AssignSequenceNumbersAndLog(request, &MockShard{id: 1})
		c.Assert(err, IsNil)
		c.Assert(id, Equals, uint32(i+1))
	}
	requests := 0
	err := wal.RecoverServerFromLastCommit(1, []uint32{1}, func(req *protocol.Request, shardId uint32) error {
		requests++
		return wal.Commit(req.GetRequestNumber(), 1)
	})
	c.Assert(err, IsNil)
	c.Assert(requests, Equals, 4000)
}

func (_ *WalSuite) TestLogFilesCompaction(c *C) {
	wal := newWal(c)
	wal.config.WalRequestsPerLogFile = 2000
	wal.Commit(1, 1)
	wal.Commit(1, 2)
	for i := 0; i < 2500; i++ {
		request := generateRequest(2)
		id, err := wal.AssignSequenceNumbersAndLog(request, &MockShard{id: 1})
		c.Assert(err, IsNil)
		c.Assert(id, Equals, uint32(i+1))
	}
	c.Assert(wal.logFiles, HasLen, 2)
	suffix := wal.logFiles[0].suffix()
	c.Assert(wal.Commit(2001, 1), IsNil)
	c.Assert(wal.logFiles, HasLen, 2)
	_, err := os.Stat(path.Join(wal.config.WalDir, fmt.Sprintf("log.%d", suffix)))
	c.Assert(err, IsNil)
	c.Assert(wal.Commit(2001, 2), IsNil)
	c.Assert(wal.logFiles, HasLen, 1)
	_, err = os.Stat(path.Join(wal.config.WalDir, fmt.Sprintf("log.%d", suffix)))
	c.Assert(os.IsNotExist(err), Equals, true)
}

func (_ *WalSuite) TestMultipleLogFiles(c *C) {
	wal := newWal(c)
	wal.config.WalRequestsPerLogFile = 2000
	for i := 0; i < 2500; i++ {
		request := generateRequest(2)
		id, err := wal.AssignSequenceNumbersAndLog(request, &MockShard{id: 1})
		c.Assert(err, IsNil)
		c.Assert(id, Equals, uint32(i+1))
	}
	c.Assert(wal.logFiles, HasLen, 2)
	wal.Close()

	wal, err := NewWAL(wal.config)
	wal.SetServerId(1)
	c.Assert(err, IsNil)
	requests := []*protocol.Request{}
	err = wal.RecoverServerFromRequestNumber(2001, []uint32{1}, func(request *protocol.Request, _ uint32) error {
		requests = append(requests, request)
		return nil
	})
	c.Assert(len(requests), Equals, 500)

	requests = []*protocol.Request{}
	err = wal.RecoverServerFromRequestNumber(501, []uint32{1}, func(request *protocol.Request, _ uint32) error {
		requests = append(requests, request)
		return nil
	})
	c.Assert(len(requests), Equals, 2000)
}

func (_ *WalSuite) TestAutoBookmark(c *C) {
	wal := newWal(c)
	wal.config.WalBookmarkAfterRequests = 2
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
	s, err := newGlobalState(f.Name())
	c.Assert(err, IsNil)
	c.Assert(s.ShardLastSequenceNumber[1], Equals, uint64(4))
	c.Assert(s.LargestRequestNumber, Equals, uint32(2))
}

func (_ *WalSuite) TestSequenceNumberRecovery(c *C) {
	wal := newWal(c)
	serverId := uint32(10)
	wal.SetServerId(serverId)
	request := generateRequest(2)
	id, err := wal.AssignSequenceNumbersAndLog(request, &MockShard{id: 1})
	c.Assert(err, IsNil)
	c.Assert(id, Equals, uint32(1))
	c.Assert(request.MultiSeries[0].Points[1].GetSequenceNumber(), Equals, 2*HOST_ID_OFFSET+uint64(serverId))
	wal.closeWithoutBookmarking()
	wal, err = NewWAL(wal.config)
	wal.SetServerId(1)
	wal.SetServerId(serverId)
	c.Assert(err, IsNil)
	request = generateRequest(2)
	id, err = wal.AssignSequenceNumbersAndLog(request, &MockShard{id: 1})
	c.Assert(err, IsNil)
	c.Assert(request.MultiSeries[0].Points[1].GetSequenceNumber(), Equals, 4*HOST_ID_OFFSET+uint64(serverId))
}

func (_ *WalSuite) TestAutoBookmarkAfterRecovery(c *C) {
	wal := newWal(c)
	wal.config.WalBookmarkAfterRequests = 2
	request := generateRequest(2)
	id, err := wal.AssignSequenceNumbersAndLog(request, &MockShard{id: 1})
	c.Assert(err, IsNil)
	c.Assert(id, Equals, uint32(1))
	// close and reopen the wal
	wal.Close()
	wal, err = NewWAL(wal.config)
	wal.SetServerId(1)
	c.Assert(err, IsNil)
	for i := 0; i < 2; i++ {
		request = generateRequest(2)
		id, err = wal.AssignSequenceNumbersAndLog(request, &MockShard{id: 1})
		c.Assert(err, IsNil)
		c.Assert(id, Equals, uint32(i+2))
	}
	// make sure the bookmark exist
	bookmarkPath := path.Join(wal.config.WalDir, "bookmark")
	f, err := os.Open(bookmarkPath)
	c.Assert(err, IsNil)
	s, err := newGlobalState(f.Name())
	c.Assert(err, IsNil)
	c.Assert(s.ShardLastSequenceNumber[1], Equals, uint64(6))
	c.Assert(s.LargestRequestNumber, Equals, uint32(3))
}

func (_ *WalSuite) TestAutoBookmarkShouldntHappenTooOften(c *C) {
	wal := newWal(c)
	wal.config.WalBookmarkAfterRequests = 3
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
	err = wal.RecoverServerFromRequestNumber(uint32(2), []uint32{1}, func(req *protocol.Request, shardId uint32) error {
		requests = append(requests, req)
		return nil
	})
	c.Assert(err, IsNil)
	c.Assert(requests, HasLen, 1)
	c.Assert(requests[0].MultiSeries[0].Points, HasLen, 3)
	c.Assert(*requests[0].RequestNumber, Equals, uint32(3))
	c.Assert(err, IsNil)
}

// TODO: test roll over with multiple log files (this will test
// sorting of the log files)
func (_ *WalSuite) TestRequestNumberRollOver(c *C) {
	wal := newWal(c)
	firstRequestNumber := uint32(math.MaxUint32 - 10)
	wal.state.LargestRequestNumber = firstRequestNumber
	var i uint32
	for i = 0; i < 20; i++ {
		req := generateRequest(2)
		n, err := wal.AssignSequenceNumbersAndLog(req, &MockShard{id: 1})
		c.Assert(err, IsNil)
		c.Assert(n, Equals, firstRequestNumber+i+1)
	}
	wal.Close()
	requests := []*protocol.Request{}
	wal.RecoverServerFromRequestNumber(firstRequestNumber+1, []uint32{1}, func(req *protocol.Request, shardId uint32) error {
		requests = append(requests, req)
		return nil
	})
	c.Assert(requests, HasLen, 20)
}

func (_ *WalSuite) TestRequestNumberRollOverAcrossMultipleFiles(c *C) {
	wal := newWal(c)
	firstRequestNumber := uint32(math.MaxUint32 - 5000)
	wal.state.LargestRequestNumber = firstRequestNumber
	var i uint32
	for i = 0; i < 20000; i++ {
		req := generateRequest(2)
		n, err := wal.AssignSequenceNumbersAndLog(req, &MockShard{id: 1})
		c.Assert(err, IsNil)
		c.Assert(n, Equals, firstRequestNumber+i+1)
	}
	wal.Close()
	requests := []*protocol.Request{}
	wal.RecoverServerFromRequestNumber(firstRequestNumber+1, []uint32{1}, func(req *protocol.Request, shardId uint32) error {
		requests = append(requests, req)
		return nil
	})
	c.Assert(len(requests), Equals, 20000)
	wal, err := NewWAL(wal.config)
	wal.SetServerId(1)
	c.Assert(err, IsNil)
	requests = []*protocol.Request{}
	wal.RecoverServerFromRequestNumber(firstRequestNumber+1, []uint32{1}, func(req *protocol.Request, shardId uint32) error {
		requests = append(requests, req)
		return nil
	})
	c.Assert(len(requests), Equals, 20000)
}

func (_ *WalSuite) TestRequestNumberRollOverAndIndexing(c *C) {
	wal := newWal(c)
	firstRequestNumber := uint32(math.MaxUint32 - 5000)
	wal.state.LargestRequestNumber = firstRequestNumber
	var i uint32
	for i = 0; i < 20000; i++ {
		req := generateRequest(2)
		n, err := wal.AssignSequenceNumbersAndLog(req, &MockShard{id: 1})
		c.Assert(err, IsNil)
		c.Assert(n, Equals, firstRequestNumber+i+1)
	}
	wal.Close()
	requests := []*protocol.Request{}
	wal.RecoverServerFromRequestNumber(0, []uint32{1}, func(req *protocol.Request, shardId uint32) error {
		requests = append(requests, req)
		return nil
	})
	c.Assert(len(requests), Equals, 15000)
}

func (_ *WalSuite) TestRecoveryAfterStartup(c *C) {
	wal := newWal(c)
	requests := []*protocol.Request{}
	wal.RecoverServerFromRequestNumber(0, []uint32{1}, func(req *protocol.Request, shardId uint32) error {
		requests = append(requests, req)
		return nil
	})
	c.Assert(requests, HasLen, 0)
}

// issue #637
func (_ *WalSuite) TestInvalidData(c *C) {
	wal := newWal(c)
	req := generateRequest(2)
	_, err := wal.AssignSequenceNumbersAndLog(req, &MockShard{id: 1})
	c.Assert(err, IsNil)
	c.Assert(wal.Close(), IsNil)
	filePath := path.Join(wal.config.WalDir, "log.1")
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	c.Assert(err, IsNil)
	defer file.Close()
	bytes := []byte{'a', 'b', 'c', 'd'}
	hdr := &entryHeader{1, 1, 4}
	_, err = hdr.Write(file)
	c.Assert(err, IsNil)
	_, err = file.Write(bytes)
	c.Assert(err, IsNil)

	// the WAL should trucate to just the first request
	wal, err = NewWAL(wal.config)
	c.Assert(err, IsNil)
	wal.SetServerId(1)

	count := 0
	err = wal.RecoverServerFromRequestNumber(1, nil, func(req *protocol.Request, shardId uint32) error {
		count++
		return nil
	})
	c.Assert(count != 0, Equals, true)
	c.Assert(err, IsNil)
}

func (_ *WalSuite) TestRecoveryFromCrash(c *C) {
	wal := newWal(c)
	req := generateRequest(2)
	_, err := wal.AssignSequenceNumbersAndLog(req, &MockShard{id: 1})
	c.Assert(err, IsNil)
	c.Assert(wal.Close(), IsNil)
	filePath := path.Join(wal.config.WalDir, "log.1")
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	c.Assert(err, IsNil)
	hdr := &entryHeader{1, 1, 500}
	_, err = hdr.Write(file)
	c.Assert(err, IsNil)
	// write an incomplete request, 200 bytes as opposed to 500 bytes in
	// the hdr
	_, err = file.Write(make([]byte, 200))
	c.Assert(err, IsNil)
	defer file.Close()

	// the WAL should trucate to just the first request
	wal, err = NewWAL(wal.config)
	wal.SetServerId(1)
	c.Assert(err, IsNil)
	requests := []*protocol.Request{}
	wal.RecoverServerFromRequestNumber(1, []uint32{1}, func(req *protocol.Request, shardId uint32) error {
		requests = append(requests, req)
		return nil
	})
	c.Assert(requests, HasLen, 1)

	// make sure the file is truncated
	info, err := file.Stat()
	c.Assert(err, IsNil)
	c.Assert(info.Size(), Equals, int64(69))
	// make sure appending a new request will increase the size of the
	// file by just that request
	_, err = wal.AssignSequenceNumbersAndLog(req, &MockShard{id: 1})
	c.Assert(err, IsNil)
	info, err = file.Stat()
	c.Assert(err, IsNil)
	c.Assert(info.Size(), Equals, int64(69*2))

	requests = []*protocol.Request{}
	wal.RecoverServerFromRequestNumber(1, []uint32{1}, func(req *protocol.Request, shardId uint32) error {
		requests = append(requests, req)
		return nil
	})
	c.Assert(requests, HasLen, 2)
}

func (_ *WalSuite) TestAnotherRecoveryFromCrash(c *C) {
	wal := newWal(c)
	req := generateRequest(2)
	_, err := wal.AssignSequenceNumbersAndLog(req, &MockShard{id: 1})
	c.Assert(err, IsNil)
	c.Assert(wal.Close(), IsNil)
	filePath := path.Join(wal.config.WalDir, "log.1")
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	c.Assert(err, IsNil)
	hdr := &entryHeader{0, 0, 0}
	_, err = hdr.Write(file)
	c.Assert(err, IsNil)
	defer file.Close()

	// the WAL should trucate to just the first request
	wal, err = NewWAL(wal.config)
	wal.SetServerId(1)
	c.Assert(err, IsNil)
	requests := []*protocol.Request{}
	wal.RecoverServerFromRequestNumber(1, []uint32{1}, func(req *protocol.Request, shardId uint32) error {
		requests = append(requests, req)
		return nil
	})
	c.Assert(requests, HasLen, 1)
}

func (_ *WalSuite) TestRecoverWithNonWriteRequests(c *C) {
	wal := newWal(c)
	requestType := protocol.Request_QUERY
	request := &protocol.Request{
		Type:     &requestType,
		Database: protocol.String("some_db"),
	}
	wal.AssignSequenceNumbersAndLog(request, &MockServer{id: 1})
	c.Assert(wal.Close(), IsNil)
	wal, err := NewWAL(wal.config)
	c.Assert(err, IsNil)
	wal.SetServerId(1)
}

func (_ *WalSuite) TestAnotherSimultaneousReplay(c *C) {
	wal := newWal(c)
	signalChan := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			request := generateRequest(1000)
			wal.AssignSequenceNumbersAndLog(request, &MockShard{id: 1})
		}
		signalChan <- struct{}{}
	}()

outer:
	for {
		select {
		case <-signalChan:
			break outer
		default:
			wal.RecoverServerFromRequestNumber(uint32(1), []uint32{1}, func(req *protocol.Request, shardId uint32) error {
				return nil
			})
		}
	}
	c.Assert(wal.Close(), IsNil)
	wal, err := NewWAL(wal.config)
	c.Assert(err, IsNil)
	wal.SetServerId(1)
	requests := []*protocol.Request{}
	wal.RecoverServerFromRequestNumber(uint32(1), []uint32{1}, func(req *protocol.Request, shardId uint32) error {
		requests = append(requests, req)
		return nil
	})
	c.Assert(len(requests), Equals, 1000)
}

func (_ *WalSuite) TestSimultaneousReplay(c *C) {
	wal := newWal(c)
	signalChan := make(chan struct{})
	go func() {
		count := 0
		for i := 0; i < 10000; i++ {
			request := generateRequest(2)
			wal.AssignSequenceNumbersAndLog(request, &MockShard{id: 1})
			count++
			if count == 100 {
				signalChan <- struct{}{}
			}
		}
		signalChan <- struct{}{}
	}()

	<-signalChan
	requests := []*protocol.Request{}
	wal.RecoverServerFromRequestNumber(uint32(1), []uint32{1}, func(req *protocol.Request, shardId uint32) error {
		requests = append(requests, req)
		return nil
	})
	<-signalChan
	c.Assert(wal.Close(), IsNil)
	wal, err := NewWAL(wal.config)
	c.Assert(err, IsNil)
	wal.SetServerId(1)
	c.Assert(len(requests), InRange, 100, 200)
}

func (_ *WalSuite) TestErrorInReplay(c *C) {
	wal := newWal(c)
	request := generateRequest(1)
	id, err := wal.AssignSequenceNumbersAndLog(request, &MockShard{id: 1})
	c.Assert(err, IsNil)
	c.Assert(id, Equals, uint32(1))

	err = wal.RecoverServerFromRequestNumber(uint32(1), []uint32{1}, func(req *protocol.Request, shardId uint32) error {
		return fmt.Errorf("end replay")
	})
	c.Assert(err, NotNil)
}

func (_ *WalSuite) TestErrorInReplayWithManyRequests(c *C) {
	wal := newWal(c)
	for i := 1; i <= 100; i++ {
		request := generateRequest(i)
		_, err := wal.AssignSequenceNumbersAndLog(request, &MockShard{id: 1})
		c.Assert(err, IsNil)
	}

	count := 0
	err := wal.RecoverServerFromRequestNumber(uint32(1), []uint32{1}, func(req *protocol.Request, shardId uint32) error {
		count++
		if count > 50 {
			return fmt.Errorf("end replay")
		}
		return nil
	})
	c.Assert(err, NotNil)
}

func (_ *WalSuite) TestIndexAfterRecovery(c *C) {
	wal := newWal(c)
	for i := 0; i < 1500; i++ {
		request := generateRequest(2)
		id, err := wal.AssignSequenceNumbersAndLog(request, &MockShard{id: 1})
		c.Assert(err, IsNil)
		c.Assert(id, Equals, uint32(i+1))
	}

	wal.closeWithoutBookmarking()

	wal, err := NewWAL(wal.config)
	wal.SetServerId(1)
	c.Assert(err, IsNil)

	for i := 0; i < 500; i++ {
		request := generateRequest(2)
		id, err := wal.AssignSequenceNumbersAndLog(request, &MockShard{id: 1})
		c.Assert(err, IsNil)
		c.Assert(id, Equals, uint32(1500+i+1))
	}
	c.Assert(wal.logIndex[0].Entries, HasLen, 2)
	c.Assert(wal.logIndex[0].Entries[0].NumberOfRequests(), Equals, 1000)
	c.Assert(wal.logIndex[0].Entries[0].FirstRequestNumber, Equals, uint32(1))
	c.Assert(wal.logIndex[0].Entries[1].NumberOfRequests(), Equals, 1000)
	c.Assert(wal.logIndex[0].Entries[1].FirstRequestNumber, Equals, uint32(1001))
}

func (_ *WalSuite) TestIndex(c *C) {
	wal := newWal(c)
	for i := 0; i < 3000; i++ {
		request := generateRequest(1)
		id, err := wal.AssignSequenceNumbersAndLog(request, &MockShard{id: 1})
		c.Assert(err, IsNil)
		c.Assert(id, Equals, uint32(i+1))
	}

	c.Assert(wal.logIndex[0].Entries, HasLen, 3)
	requestOffset := wal.logIndex[0].requestOffset(2001)
	c.Assert(requestOffset > 0, Equals, true)
	// request 2000 should be in the second block not the third block
	c.Assert(requestOffset > wal.logIndex[0].requestOffset(2000), Equals, true)
}

func (_ *WalSuite) TestSequenceNumberAssignment(c *C) {
	wal := newWal(c)
	request := generateRequest(2)
	_, err := wal.AssignSequenceNumbersAndLog(request, &MockShard{id: 1})
	c.Assert(err, IsNil)
	c.Assert(request.MultiSeries[0].Points[0].GetSequenceNumber(), Equals, uint64(1*HOST_ID_OFFSET+1))
	c.Assert(request.MultiSeries[0].Points[1].GetSequenceNumber(), Equals, uint64(2*HOST_ID_OFFSET+1))
	request = generateRequest(2)
	_, err = wal.AssignSequenceNumbersAndLog(request, &MockShard{id: 1})
	c.Assert(err, IsNil)
	c.Assert(request.MultiSeries[0].Points[0].GetSequenceNumber(), Equals, uint64(3*HOST_ID_OFFSET+1))
	c.Assert(request.MultiSeries[0].Points[1].GetSequenceNumber(), Equals, uint64(4*HOST_ID_OFFSET+1))
}

func (_ *WalSuite) TestSequenceNumberAssignmentPerServer(c *C) {
	wal := newWal(c)
	wal.SetServerId(1)
	request := generateRequest(1)
	_, err := wal.AssignSequenceNumbersAndLog(request, &MockShard{id: 1})
	c.Assert(err, IsNil)

	anotherWal := newWal(c)
	anotherWal.SetServerId(2)
	anotherRequest := generateRequest(1)
	_, err = anotherWal.AssignSequenceNumbersAndLog(anotherRequest, &MockShard{id: 1})
	c.Assert(err, IsNil)
	c.Assert(request.MultiSeries[0].Points[0].GetSequenceNumber(), Not(Equals), anotherRequest.MultiSeries[0].Points[0].GetSequenceNumber())
}

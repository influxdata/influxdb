package wal

import (
	"os"
	"path/filepath"

	. "launchpad.net/gocheck"

	"github.com/influxdb/influxdb/protocol"
)

func (_ *WalSuite) TestCheckLogFileIsEmpty(c *C) {
	logfile := filepath.Join(c.MkDir(), "log.1")
	f, e := os.OpenFile(logfile, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0)
	c.Assert(e, IsNil)
	f.Close()

	_, e = checkAndRepairLogFile(logfile)
	c.Assert(e, IsNil)
}

func (_ *WalSuite) TestCheckLogFileIsOK(c *C) {
	logfile := filepath.Join(c.MkDir(), "log.1")
	f, e := os.OpenFile(logfile, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0)
	c.Assert(e, IsNil)

	tt := protocol.Request_QUERY
	db := "abc"
	req := &protocol.Request{Type: &tt,
		Database:    &db,
		MultiSeries: []*protocol.Series{&protocol.Series{Name: &db}}}
	bs, e := req.Encode()
	c.Assert(e, IsNil)

	hdr := &entryHeader{requestNumber: 1,
		shardId: 1, length: uint32(len(bs))}
	_, e = hdr.Write(f)
	c.Assert(e, IsNil)
	_, e = f.Write(bs)
	c.Assert(e, IsNil)
	f.Close()

	size, e := checkAndRepairLogFile(logfile)
	c.Assert(e, IsNil)
	c.Assert(size, Equals, uint64(12+len(bs)))
}

func (_ *WalSuite) TestCheckLogFileIsErrorRequest(c *C) {
	logfile := filepath.Join(c.MkDir(), "log.1")
	f, e := os.OpenFile(logfile, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0)
	c.Assert(e, IsNil)
	hdr := &entryHeader{requestNumber: 1,
		shardId: 1, length: 12}
	_, e = hdr.Write(f)
	c.Assert(e, IsNil)

	f.Write([]byte("0123456789012345678"))
	f.Close()
	size, e := checkAndRepairLogFile(logfile)
	c.Assert(e, IsNil)
	c.Assert(size, Equals, uint64(0))
}

func (_ *WalSuite) TestCheckLogFileHasInvalidData(c *C) {
	logfile := filepath.Join(c.MkDir(), "log.1")
	f, e := os.OpenFile(logfile, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0)
	c.Assert(e, IsNil)

	tt := protocol.Request_QUERY
	db := "abc"
	req := &protocol.Request{Type: &tt,
		Database:    &db,
		MultiSeries: []*protocol.Series{&protocol.Series{Name: &db}}}
	bs, e := req.Encode()
	c.Assert(e, IsNil)

	hdr := &entryHeader{requestNumber: 1,
		shardId: 1, length: uint32(len(bs))}
	_, e = hdr.Write(f)
	c.Assert(e, IsNil)
	_, e = f.Write(bs)
	c.Assert(e, IsNil)
	_, e = f.Write([]byte("012345678901234567801234567890123456780123456789012345678"))
	c.Assert(e, IsNil)
	f.Close()

	size, e := checkAndRepairLogFile(logfile)
	c.Assert(e, IsNil)
	c.Assert(size, Equals, uint64(12+len(bs)))
}

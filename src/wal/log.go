package wal

import (
	"code.google.com/p/goprotobuf/proto"
	"fmt"
	"io"
	"os"
	"protocol"
	"syscall"
)

type log struct {
	entries  chan *entry
	state    *state
	file     *os.File
	serverId uint32
}

func newLog(file *os.File) (*log, error) {
	l := &log{
		entries: make(chan *entry, 10),
		file:    file,
		state:   newState(),
	}

	l.recover()
	go l.processEntries()
	return l, nil
}

func (self *log) recover() error {
	return nil
}

func (self *log) setServerId(serverId uint32) {
	self.serverId = serverId
}

func (self *log) assignSequenceNumbers(shardId uint32, request *protocol.Request) {
	sequenceNumber := self.state.getCurrentSequenceNumber(shardId)
	for _, p := range request.Series.Points {
		if p.SequenceNumber != nil {
			continue
		}
		sequenceNumber++
		p.SequenceNumber = proto.Uint64(sequenceNumber)
	}
	self.state.setCurrentSequenceNumber(shardId, sequenceNumber)
}

func (self *log) processEntries() {
	for {
		select {
		case x := <-self.entries:
			self.assignSequenceNumbers(x.shardId, x.request)
			bytes, err := x.request.Encode()
			if err != nil {
				x.confirmation <- &confirmation{0, err}
				continue
			}
			requestNumber := self.state.getNextRequestNumber()
			// every request is preceded with the length, shard id and the request number
			hdr := &entryHeader{
				shardId:       x.shardId,
				requestNumber: requestNumber,
				length:        uint32(len(bytes)),
			}
			err = hdr.Write(self.file)
			if err != nil {
				x.confirmation <- &confirmation{0, err}
				continue
			}
			written, err := self.file.Write(bytes)
			if err != nil {
				x.confirmation <- &confirmation{0, err}
				continue
			}
			if written < len(bytes) {
				x.confirmation <- &confirmation{0, fmt.Errorf("Couldn't write entire request")}
				continue
			}
			x.confirmation <- &confirmation{requestNumber, nil}
		}
	}
}

func (self *log) appendRequest(request *protocol.Request, shardId uint32) (uint32, error) {
	entry := &entry{make(chan *confirmation), request, shardId}
	self.entries <- entry
	confirmation := <-entry.confirmation
	return confirmation.requestNumber, confirmation.err
}

func (self *log) replayFromRequestNumber(shardIds []uint32, requestNumber uint32) (chan *replayRequest, chan struct{}) {
	stopChan := make(chan struct{})
	replayChan := make(chan *replayRequest, 10)
	go func() {
		fd, err := syscall.Dup(int(self.file.Fd()))
		if err != nil {
			replayChan <- &replayRequest{nil, uint32(0), err}
			return
		}
		file := os.NewFile(uintptr(fd), self.file.Name())
		defer file.Close()
		// TODO: create a request number to file location index that we
		// can use for fast seeks
		_, err = file.Seek(0, os.SEEK_SET)
		if err != nil {
			replayChan <- &replayRequest{nil, uint32(0), err}
			return
		}
		shardIdsSet := map[uint32]struct{}{}
		for _, shardId := range shardIds {
			shardIdsSet[shardId] = struct{}{}
		}
		for {
			hdr := &entryHeader{}
			err := hdr.Read(file)

			if err == io.EOF {
				close(replayChan)
				return
			}

			if err != nil {
				replayChan <- &replayRequest{nil, uint32(0), err}
				return
			}

			if _, ok := shardIdsSet[hdr.shardId]; !ok {
				continue
			}

			if hdr.requestNumber < requestNumber {
				continue
			}

			bytes := make([]byte, hdr.length)
			read, err := self.file.Read(bytes)
			if err != nil {
				replayChan <- &replayRequest{nil, uint32(0), err}
				return
			}

			if uint32(read) != hdr.length {
				replayChan <- &replayRequest{nil, uint32(0), err}
				return
			}
			req := &protocol.Request{}
			err = req.Decode(bytes)
			if err != nil {
				replayChan <- &replayRequest{nil, uint32(0), err}
				return
			}
			replayChan <- &replayRequest{req, hdr.shardId, nil}
		}
	}()
	return replayChan, stopChan
}

func (self *log) forceBookmark() error {
	return fmt.Errorf("not implemented yet")
}

package wal

import (
	"code.google.com/p/goprotobuf/proto"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"protocol"
	"syscall"
)

type log struct {
	fileSize uint64
	entries  chan *entry
	state    *state
	file     *os.File
	serverId uint32
}

func newLog(file *os.File) (*log, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}

	size := uint64(info.Size())

	l := &log{
		entries:  make(chan *entry, 10),
		file:     file,
		state:    newState(),
		fileSize: size,
	}

	if err := l.recover(); err != nil {
		return nil, err
	}
	go l.processEntries()
	return l, nil
}

// this is for testing only
func (self *log) closeWithoutBookmark() error {
	return self.file.Close()
}

func (self *log) close() error {
	dir := filepath.Dir(self.file.Name())
	bookmarkPath := filepath.Join(dir, "bookmark.new")
	bookmarkFile, err := os.OpenFile(bookmarkPath, os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer bookmarkFile.Close()
	self.state.setFileOffset(int64(self.fileSize))
	if err := self.state.write(bookmarkFile); err != nil {
		return err
	}
	if err := bookmarkFile.Close(); err != nil {
		return err
	}
	if err := os.Rename(bookmarkPath, filepath.Join(dir, "bookmark")); err != nil {
		return err
	}
	return self.file.Close()
}

func (self *log) recover() error {
	dir := filepath.Dir(self.file.Name())
	bookmarkPath := filepath.Join(dir, "bookmark")
	_, err := os.Stat(bookmarkPath)
	if os.IsNotExist(err) {
		return nil
	}

	if err != nil {
		return err
	}

	// read the state from the bookmark file
	bookmark, err := os.OpenFile(bookmarkPath, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	if err := self.state.read(bookmark); err != nil {
		return err
	}

	// replay the rest of the wal
	if _, err := self.file.Seek(self.state.FileOffset, os.SEEK_SET); err != nil {
		return err
	}

	replayChan := make(chan *replayRequest, 10)
	stopChan := make(chan struct{})

	go func() {
		self.replayFromFile(self.file, map[uint32]struct{}{}, 0, replayChan, stopChan)
	}()

	for {
		x := <-replayChan
		if x == nil {
			break
		}

		if x.err != nil {
			return x.err
		}

		self.state.recover(x)
	}

	info, err := self.file.Stat()
	if err != nil {
		return err
	}
	self.state.setFileOffset(info.Size())

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
			writtenHdrBytes, err := hdr.Write(self.file)
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
			self.fileSize += uint64(writtenHdrBytes + written)
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

func (self *log) dupLogFile() (*os.File, error) {
	fd, err := syscall.Dup(int(self.file.Fd()))
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), self.file.Name()), nil
}

// replay requests starting at the given requestNumber and for the
// given shard ids. Return all requests if shardIds is empty
func (self *log) replayFromRequestNumber(shardIds []uint32, requestNumber uint32) (chan *replayRequest, chan struct{}) {
	stopChan := make(chan struct{})
	replayChan := make(chan *replayRequest, 10)
	go func() {
		file, err := self.dupLogFile()
		if err != nil {
			replayChan <- newErrorReplayRequest(err)
			return
		}
		defer file.Close()
		// TODO: create a request number to file location index that we
		// can use for fast seeks
		_, err = file.Seek(0, os.SEEK_SET)
		if err != nil {
			replayChan <- newErrorReplayRequest(err)
			return
		}
		shardIdsSet := map[uint32]struct{}{}
		for _, shardId := range shardIds {
			shardIdsSet[shardId] = struct{}{}
		}
		self.replayFromFile(file, shardIdsSet, requestNumber, replayChan, stopChan)
	}()
	return replayChan, stopChan
}

func (self *log) replayFromFile(file *os.File, shardIdsSet map[uint32]struct{}, requestNumber uint32, replayChan chan *replayRequest, stopChan chan struct{}) {
	for {
		hdr := &entryHeader{}
		_, err := hdr.Read(file)

		if err == io.EOF {
			close(replayChan)
			return
		}

		if err != nil {
			// TODO: the following line is all over the place. DRY
			replayChan <- newErrorReplayRequest(err)
			return
		}

		ok := false
		if len(shardIdsSet) == 0 {
			ok = true
		} else {
			_, ok = shardIdsSet[hdr.shardId]
		}
		if !ok {
			_, err = file.Seek(int64(hdr.length), os.SEEK_CUR)
			if err != nil {
				replayChan <- newErrorReplayRequest(err)
				return
			}
			continue
		}

		if hdr.requestNumber < requestNumber {
			_, err = file.Seek(int64(hdr.length), os.SEEK_CUR)
			if err != nil {
				replayChan <- newErrorReplayRequest(err)
				return
			}
			continue
		}

		bytes := make([]byte, hdr.length)
		read, err := self.file.Read(bytes)
		if err != nil {
			replayChan <- newErrorReplayRequest(err)
			return
		}

		if uint32(read) != hdr.length {
			replayChan <- newErrorReplayRequest(err)
			return
		}
		req := &protocol.Request{}
		err = req.Decode(bytes)
		if err != nil {
			replayChan <- newErrorReplayRequest(err)
			return
		}
		replayChan <- &replayRequest{hdr.requestNumber, req, hdr.shardId, nil}
	}
}

func (self *log) forceBookmark() error {
	return fmt.Errorf("not implemented yet")
}

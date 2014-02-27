package wal

import (
	"code.google.com/p/goprotobuf/proto"
	logger "code.google.com/p/log4go"
	"configuration"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"protocol"
	"strconv"
	"strings"
)

type log struct {
	closed                 bool
	fileSize               uint64
	state                  *state
	file                   *os.File
	serverId               uint64
	requestsSinceLastFlush int
	config                 *configuration.Configuration
	cachedSuffix           int
}

func newLog(file *os.File, config *configuration.Configuration) (*log, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}

	size := uint64(info.Size())
	suffixString := strings.TrimLeft(path.Base(file.Name()), "log.")
	suffix, err := strconv.Atoi(suffixString)
	if err != nil {
		return nil, err
	}

	l := &log{
		file:         file,
		state:        newState(),
		fileSize:     size,
		closed:       false,
		config:       config,
		cachedSuffix: suffix,
	}

	if err := l.recover(); err != nil {
		logger.Error("Error while recovering from file %s", file.Name())
		return nil, err
	}
	return l, nil
}

func (self *log) suffix() int {
	return self.cachedSuffix
}

func (self *log) firstRequestNumber() uint32 {
	return self.state.LargestRequestNumber - uint32(self.state.TotalNumberOfRequests)
}

func (self *log) internalFlush() error {
	logger.Debug("Fsyncing the log file to disk")
	self.requestsSinceLastFlush = 0
	return self.file.Sync()
}

func (self *log) requestsSinceLastBookmark() int {
	return self.state.RequestsSinceLastBookmark
}

// this is for testing only
func (self *log) closeWithoutBookmark() error {
	return self.file.Close()
}

func (self *log) close() error {
	if self.closed {
		return nil
	}
	self.forceIndex()
	self.forceBookmark()
	self.internalFlush()
	return self.file.Close()
}

func (self *log) recover() error {
	dir := filepath.Dir(self.file.Name())
	bookmarkPath := filepath.Join(dir, fmt.Sprintf("bookmark.%d", self.suffix()))
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
	defer bookmark.Close()
	if err := self.state.read(bookmark); err != nil {
		return err
	}

	self.state.RequestsSinceLastBookmark = 0
	self.state.RequestsSinceLastIndex = 0

	logger.Debug("Recovering from previous state from file offset: %d", self.state.FileOffset)

	// replay the rest of the wal
	if _, err := self.file.Seek(self.state.FileOffset, os.SEEK_SET); err != nil {
		logger.Error("Cannot seek to %d. Error: %s", self.state.FileOffset, err)
		return err
	}

	replayChan := make(chan *replayRequest, 10)
	stopChan := make(chan struct{})

	go func() {
		self.replayFromFileLocation(self.file, map[uint32]struct{}{}, replayChan, stopChan)
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
		self.conditionalBookmarkAndIndex()
	}

	info, err := self.file.Stat()
	if err != nil {
		return err
	}
	self.state.setFileOffset(info.Size())

	return nil
}

func (self *log) setServerId(serverId uint32) {
	self.serverId = uint64(serverId)
}

func (self *log) assignSequenceNumbers(shardId uint32, request *protocol.Request) {
	if request.Series == nil {
		return
	}
	sequenceNumber := self.state.getCurrentSequenceNumber(shardId)
	for _, p := range request.Series.Points {
		if p.SequenceNumber != nil {
			continue
		}
		sequenceNumber++
		p.SequenceNumber = proto.Uint64(sequenceNumber*HOST_ID_OFFSET + self.serverId)
	}
	self.state.setCurrentSequenceNumber(shardId, sequenceNumber)
}

func (self *log) conditionalBookmarkAndIndex() {
	self.state.TotalNumberOfRequests++

	shouldFlush := false
	self.state.RequestsSinceLastIndex++
	if self.state.RequestsSinceLastIndex >= uint32(self.config.WalIndexAfterRequests) {
		shouldFlush = true
		self.forceIndex()
	}

	self.state.RequestsSinceLastBookmark++
	if self.state.RequestsSinceLastBookmark >= self.config.WalBookmarkAfterRequests {
		shouldFlush = true
		self.forceBookmark()
	}

	self.requestsSinceLastFlush++
	if self.requestsSinceLastFlush > self.config.WalFlushAfterRequests || shouldFlush {
		self.internalFlush()
	}
}

func (self *log) appendRequest(request *protocol.Request, shardId uint32) (uint32, error) {
	self.assignSequenceNumbers(shardId, request)
	bytes, err := request.Encode()

	if err != nil {
		return 0, err
	}
	requestNumber := self.state.getNextRequestNumber()
	// every request is preceded with the length, shard id and the request number
	hdr := &entryHeader{
		shardId:       shardId,
		requestNumber: requestNumber,
		length:        uint32(len(bytes)),
	}
	writtenHdrBytes, err := hdr.Write(self.file)
	if err != nil {
		logger.Error("Error while writing header: %s", err)
		return 0, err
	}
	written, err := self.file.Write(bytes)
	if err != nil {
		logger.Error("Error while writing request: %s", err)
		return 0, err
	}
	if written < len(bytes) {
		err = fmt.Errorf("Couldn't write entire request")
		logger.Error("Error while writing request: %s", err)
		return 0, err
	}
	self.fileSize += uint64(writtenHdrBytes + written)
	self.conditionalBookmarkAndIndex()
	return requestNumber, nil
}

func (self *log) dupLogFile() (*os.File, error) {
	return os.OpenFile(self.file.Name(), os.O_RDONLY, 0)
}

// replay requests starting at the given requestNumber and for the
// given shard ids. Return all requests if shardIds is empty
func (self *log) replayFromRequestNumber(shardIds []uint32, requestNumber uint32, order RequestNumberOrder) (chan *replayRequest, chan struct{}) {
	// this channel needs to be buffered in case the last request in the
	// log file caused an error in the yield function
	stopChan := make(chan struct{}, 1)
	replayChan := make(chan *replayRequest, 10)

	go func() {
		file, err := self.dupLogFile()
		if err != nil {
			sendOrStop(newErrorReplayRequest(err), replayChan, stopChan)
			close(replayChan)
			return
		}
		defer file.Close()
		offset := self.state.Index.requestOffset(order, requestNumber)
		logger.Debug("Replaying from file offset %d", offset)
		_, err = file.Seek(int64(offset), os.SEEK_SET)
		if err != nil {
			sendOrStop(newErrorReplayRequest(err), replayChan, stopChan)
			close(replayChan)
			return
		}
		shardIdsSet := map[uint32]struct{}{}
		for _, shardId := range shardIds {
			shardIdsSet[shardId] = struct{}{}
		}
		if err := self.skipToRequest(file, requestNumber, order); err != nil {
			sendOrStop(newErrorReplayRequest(err), replayChan, stopChan)
			return
		}
		self.replayFromFileLocation(file, shardIdsSet, replayChan, stopChan)
	}()
	return replayChan, stopChan
}

func (self *log) getNextHeader(file *os.File) (int, *entryHeader, error) {
	hdr := &entryHeader{}
	numberOfBytes, err := hdr.Read(file)
	if err == io.EOF {
		return 0, nil, nil
	}
	return numberOfBytes, hdr, err
}

func (self *log) skipRequest(file *os.File, hdr *entryHeader) (err error) {
	_, err = file.Seek(int64(hdr.length), os.SEEK_CUR)
	return
}

func (self *log) skipToRequest(file *os.File, requestNumber uint32, order RequestNumberOrder) error {
	for {
		n, hdr, err := self.getNextHeader(file)
		if n == 0 {
			// EOF
			return nil
		}
		if err != nil {
			return err
		}
		if order.isBefore(hdr.requestNumber, requestNumber) {
			if err := self.skipRequest(file, hdr); err != nil {
				return err
			}
			continue
		}
		// seek back to the beginning of the request header
		_, err = file.Seek(int64(-n), os.SEEK_CUR)
		return err
	}
}

func (self *log) replayFromFileLocation(file *os.File,
	shardIdsSet map[uint32]struct{},
	replayChan chan *replayRequest,
	stopChan chan struct{}) {

	defer func() { close(replayChan) }()
	for {
		numberOfBytes, hdr, err := self.getNextHeader(file)
		if numberOfBytes == 0 {
			break
		}

		if err != nil {
			// TODO: the following line is all over the place. DRY
			sendOrStop(newErrorReplayRequest(err), replayChan, stopChan)
			return
		}

		ok := false
		if len(shardIdsSet) == 0 {
			ok = true
		} else {
			_, ok = shardIdsSet[hdr.shardId]
		}
		if !ok {
			err = self.skipRequest(file, hdr)
			if err != nil {
				sendOrStop(newErrorReplayRequest(err), replayChan, stopChan)
				return
			}
			continue
		}

		bytes := make([]byte, hdr.length)
		read, err := file.Read(bytes)
		if err == io.EOF {
			// file ends prematurely, truncate to the previous request
			logger.Warn("%s ends prematurely, truncating to last known good request", file.Name())
			offset, err := file.Seek(int64(-numberOfBytes), os.SEEK_CUR)
			if err != nil {
				sendOrStop(newErrorReplayRequest(err), replayChan, stopChan)
				return
			}
			err = file.Truncate(offset)
			if err != nil {
				sendOrStop(newErrorReplayRequest(err), replayChan, stopChan)
			}
			return
		}
		if err != nil {
			sendOrStop(newErrorReplayRequest(err), replayChan, stopChan)
			return
		}

		if uint32(read) != hdr.length {
			sendOrStop(newErrorReplayRequest(err), replayChan, stopChan)
			return
		}
		req := &protocol.Request{}
		err = req.Decode(bytes)
		if err != nil {
			sendOrStop(newErrorReplayRequest(err), replayChan, stopChan)
			return
		}

		req.RequestNumber = proto.Uint32(hdr.requestNumber)
		replayRequest := &replayRequest{hdr.requestNumber, req, hdr.shardId, nil}
		if sendOrStop(replayRequest, replayChan, stopChan) {
			return
		}
	}
}

func sendOrStop(req *replayRequest, replayChan chan *replayRequest, stopChan chan struct{}) bool {
	if req.err != nil {
		logger.Error("Error in replay: %s", req.err)
	}

	select {
	case replayChan <- req:
	case _, ok := <-stopChan:
		logger.Debug("Stopping replay")
		return ok
	}
	return false
}

func (self *log) forceBookmark() error {
	logger.Debug("Creating bookmark at file offset %d", self.fileSize)
	dir := filepath.Dir(self.file.Name())
	bookmarkPath := filepath.Join(dir, fmt.Sprintf("bookmark.%d.new", self.suffix()))
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
	err = os.Rename(bookmarkPath, filepath.Join(dir, fmt.Sprintf("bookmark.%d", self.suffix())))
	if err != nil {
		return err
	}
	self.state.RequestsSinceLastBookmark = 0
	return nil
}

func (self *log) forceIndex() error {
	// don't do anything if the number of requests writtern since the
	// last index update is 0
	if self.state.RequestsSinceLastIndex == 0 {
		return nil
	}

	startRequestNumber := self.state.LargestRequestNumber - uint32(self.state.RequestsSinceLastIndex) + 1
	logger.Debug("Creating new index entry [%d,%d]", startRequestNumber, self.state.RequestsSinceLastIndex)
	self.state.Index.addEntry(startRequestNumber, self.state.RequestsSinceLastIndex, self.fileSize)
	self.state.RequestsSinceLastIndex = 0
	return nil
}

func (self *log) delete() {
	filePath := path.Join(self.config.WalDir, fmt.Sprintf("bookmark.%d", self.suffix()))
	os.Remove(filePath)
	filePath = path.Join(self.config.WalDir, fmt.Sprintf("log.%d", self.suffix()))
	os.Remove(filePath)
}

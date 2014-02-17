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
	"syscall"
)

type log struct {
	closed                 bool
	fileSize               uint64
	state                  *state
	file                   *os.File
	serverId               uint32
	requestsSinceLastFlush int
	config                 *configuration.Configuration
	cachedSuffix           int
	// channels used to process entries, force a bookmark or create an
	// index entry
	entries      chan *entry
	bookmarkChan chan *bookmarkEvent
	indexChan    chan struct{}
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
		entries:      make(chan *entry, 10),
		bookmarkChan: make(chan *bookmarkEvent),
		indexChan:    make(chan struct{}),
		file:         file,
		state:        newState(),
		fileSize:     size,
		closed:       false,
		config:       config,
		cachedSuffix: suffix,
	}

	if err := l.recover(); err != nil {
		return nil, err
	}
	go l.processEntries()
	return l, nil
}

func (self *log) suffix() int {
	return self.cachedSuffix
}

func (self *log) firstRequestNumber() uint32 {
	return self.state.LargestRequestNumber - uint32(self.state.TotalNumberOfRequests)
}

func (self *log) internalIndex() error {
	// don't do anything if the number of requests writtern since the
	// last index update is 0
	if self.state.RequestsSinceLastIndex == 0 {
		return nil
	}

	startRequestNumber := self.state.LargestRequestNumber - uint32(self.state.RequestsSinceLastIndex) + 1
	logger.Info("Creating new index entry [%d,%d]", startRequestNumber, self.state.RequestsSinceLastIndex)
	self.state.Index.addEntry(startRequestNumber, self.state.RequestsSinceLastIndex, self.fileSize)
	self.state.RequestsSinceLastIndex = 0
	return nil
}

func (self *log) internalFlush() error {
	logger.Info("Fsyncing the log file to disk")
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
	self.forceBookmark(true)
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
	if err := self.state.read(bookmark); err != nil {
		return err
	}

	self.state.RequestsSinceLastBookmark = 0
	self.state.RequestsSinceLastIndex = 0
	self.state.setFileOffset(self.state.FileOffset)

	// replay the rest of the wal
	if _, err := self.file.Seek(self.state.FileOffset, os.SEEK_SET); err != nil {
		return err
	}

	replayChan := make(chan *replayRequest, 10)
	stopChan := make(chan struct{})

	go func() {
		self.replayFromFileLocation(self.file, map[uint32]struct{}{}, 0, replayChan, stopChan)
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
			self.internalAppendRequest(x)
		case _ = <-self.indexChan:
			self.internalIndex()
		case x := <-self.bookmarkChan:
			err := self.internalFlush()
			// only if we could flush successfully create a bookmark
			if err == nil {
				err = self.internalBookmark()
			}
			x.confirmationChan <- &confirmation{0, err}
			if x.shutdown {
				self.closed = true
				return
			}
		}
	}
}

func (self *log) conditionalBookmarkAndIndex() {
	self.state.TotalNumberOfRequests++

	shouldFlush := false
	self.state.RequestsSinceLastIndex++
	if self.state.RequestsSinceLastIndex >= uint32(self.config.WalIndexAfterRequests) {
		shouldFlush = true
		self.internalIndex()
	}

	self.state.RequestsSinceLastBookmark++
	if self.state.RequestsSinceLastBookmark >= self.config.WalBookmarkAfterRequests {
		shouldFlush = true
		self.internalBookmark()
	}

	self.requestsSinceLastFlush++
	if self.requestsSinceLastFlush > self.config.WalFlushAfterRequests || shouldFlush {
		self.internalFlush()
	}
}

func (self *log) internalAppendRequest(x *entry) {
	self.assignSequenceNumbers(x.shardId, x.request)
	bytes, err := x.request.Encode()

	// declare some variables so we can goto returnError without go
	// complaining
	var requestNumber uint32
	var written, writtenHdrBytes int
	var hdr *entryHeader

	if err != nil {
		goto returnError
	}
	requestNumber = self.state.getNextRequestNumber()
	// every request is preceded with the length, shard id and the request number
	hdr = &entryHeader{
		shardId:       x.shardId,
		requestNumber: requestNumber,
		length:        uint32(len(bytes)),
	}
	writtenHdrBytes, err = hdr.Write(self.file)
	if err != nil {
		logger.Error("Error while writing header: %s", err)
		goto returnError
	}
	written, err = self.file.Write(bytes)
	if err != nil {
		logger.Error("Error while writing request: %s", err)
		goto returnError
	}
	if written < len(bytes) {
		err = fmt.Errorf("Couldn't write entire request")
		logger.Error("Error while writing request: %s", err)
		goto returnError
	}
	self.fileSize += uint64(writtenHdrBytes + written)
	self.conditionalBookmarkAndIndex()
	x.confirmation <- &confirmation{requestNumber, nil}
	return
returnError:
	x.confirmation <- &confirmation{0, err}
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
		offset := self.state.Index.requestOffset(requestNumber)
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
		self.replayFromFileLocation(file, shardIdsSet, requestNumber, replayChan, stopChan)
	}()
	return replayChan, stopChan
}

func (self *log) replayFromFileLocation(file *os.File,
	shardIdsSet map[uint32]struct{},
	requestNumber uint32,
	replayChan chan *replayRequest,
	stopChan chan struct{}) {

	defer func() { close(replayChan) }()
	for {
		hdr := &entryHeader{}
		_, err := hdr.Read(file)

		if err == io.EOF {
			return
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
		if !ok || hdr.requestNumber < requestNumber {
			_, err = file.Seek(int64(hdr.length), os.SEEK_CUR)
			if err != nil {
				sendOrStop(newErrorReplayRequest(err), replayChan, stopChan)
				return
			}
			continue
		}

		bytes := make([]byte, hdr.length)
		read, err := self.file.Read(bytes)
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

		replayRequest := &replayRequest{hdr.requestNumber, req, hdr.shardId, nil}
		if sendOrStop(replayRequest, replayChan, stopChan) {
			return
		}
	}
}

func sendOrStop(req *replayRequest, replayChan chan *replayRequest, stopChan chan struct{}) bool {
	select {
	case replayChan <- req:
	case <-stopChan:
		return true
	}
	return false
}

func (self *log) forceBookmark(shutdown bool) error {
	confirmationChan := make(chan *confirmation)
	self.bookmarkChan <- &bookmarkEvent{shutdown, confirmationChan}
	confirmation := <-confirmationChan
	return confirmation.err
}

func (self *log) forceIndex() error {
	self.indexChan <- struct{}{}
	return nil
}

func (self *log) internalBookmark() error {
	logger.Info("Creating bookmark at file offset %d", self.fileSize)
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

func (self *log) delete() {
	filePath := path.Join(self.config.WalDir, fmt.Sprintf("bookmark.%d", self.suffix()))
	os.Remove(filePath)
	filePath = path.Join(self.config.WalDir, fmt.Sprintf("log.%d", self.suffix()))
	os.Remove(filePath)
}

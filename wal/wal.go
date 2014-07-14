package wal

import (
	"fmt"
	"math"
	"os"
	"path"
	"sort"
	"strings"

	"code.google.com/p/goprotobuf/proto"
	logger "code.google.com/p/log4go"
	"github.com/influxdb/influxdb/configuration"
	"github.com/influxdb/influxdb/protocol"
)

type WAL struct {
	state             *GlobalState
	config            *configuration.Configuration
	logFiles          []*log
	logIndex          []*index
	serverId          uint32
	nextLogFileSuffix uint32
	entries           chan interface{}

	// counters to force index creation, bookmark and flushing
	requestsSinceLastFlush    int
	requestsSinceLastBookmark int
	requestsSinceLastIndex    int
	requestsSinceRotation     int
}

const HOST_ID_OFFSET = uint64(10000)

func NewWAL(config *configuration.Configuration) (*WAL, error) {
	if config.WalDir == "" {
		return nil, fmt.Errorf("wal directory cannot be empty")
	}

	logger.Info("Opening wal in %s", config.WalDir)
	_, err := os.Stat(config.WalDir)

	if os.IsNotExist(err) {
		err = os.MkdirAll(config.WalDir, 0755)
	}

	if err != nil {
		return nil, err
	}

	dir, err := os.Open(config.WalDir)
	if err != nil {
		return nil, err
	}
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, err
	}

	state, err := newGlobalState(path.Join(config.WalDir, "bookmark"))
	if err != nil {
		logger.Error("Cannot open global state. Error: %s", err)
		return nil, err
	}

	// sort the logfiles by the first request number in the log
	wal := &WAL{
		config:   config,
		logFiles: []*log{},
		logIndex: []*index{},
		state:    state,
		entries:  make(chan interface{}, 10),
	}

	for _, name := range names {
		if !strings.HasPrefix(name, "log.") {
			continue
		}
		log, _, err := wal.openLog(path.Join(config.WalDir, name))
		if err != nil {
			return nil, err
		}
		if suffix := log.suffix(); suffix > wal.nextLogFileSuffix {
			wal.nextLogFileSuffix = suffix
		}
	}

	// sort the log files by suffix first
	sort.Sort(sortableLogSlice{wal.logFiles, wal.logIndex})

	for idx, logFile := range wal.logFiles {
		logger.Debug("suffix: %d, first suffix: %d", logFile.suffix(), wal.state.FirstSuffix)
		if logFile.suffix() < wal.state.FirstSuffix {
			continue
		}

		wal.logFiles = append(wal.logFiles[idx:], wal.logFiles[:idx]...)
		wal.logIndex = append(wal.logIndex[idx:], wal.logIndex[:idx]...)
		wal.state.FirstSuffix = logFile.suffix()
		break
	}

	go wal.processEntries()

	return wal, err
}

func (self *WAL) SetServerId(id uint32) {
	logger.Info("Setting server id to %d and recovering", id)
	self.serverId = id

	if err := self.recover(); err != nil {
		panic(err)
	}
}

// Marks a given request for a given server as committed
func (self *WAL) Commit(requestNumber uint32, serverId uint32) error {
	confirmationChan := make(chan *confirmation)
	self.entries <- &commitEntry{confirmationChan, serverId, requestNumber}
	confirmation := <-confirmationChan
	return confirmation.err
}

func (self *WAL) RecoverServerFromLastCommit(serverId uint32, shardIds []uint32, yield func(request *protocol.Request, shardId uint32) error) error {
	requestNumber, ok := self.state.ServerLastRequestNumber[serverId]
	requestNumber += 1
	if !ok {
		requestNumber = uint32(self.state.FirstSuffix)
	}
	logger.Info("Recovering server %d from request %d", serverId, requestNumber)
	return self.RecoverServerFromRequestNumber(requestNumber, shardIds, yield)
}

func (self *WAL) isInRange(requestNumber uint32) bool {
	rn := requestNumber
	largestRequestNumber := self.state.LargestRequestNumber
	if self.state.FirstSuffix > largestRequestNumber {
		return rn <= largestRequestNumber || rn >= self.state.FirstSuffix
	}
	return rn >= self.state.FirstSuffix && rn <= largestRequestNumber
}

// In the case where this server is running and another one in the
// cluster stops responding, at some point this server will have to
// just write requests to disk. When the downed server comes back up,
// it's this server's responsibility to send out any writes that were
// queued up. If the yield function returns nil then the request is
// committed.
func (self *WAL) RecoverServerFromRequestNumber(requestNumber uint32, shardIds []uint32, yield func(request *protocol.Request, shardId uint32) error) error {
	// don't replay if we don't have any log files yet
	if len(self.logFiles) == 0 {
		return nil
	}

	firstIndex := 0
	firstOffset := int64(-1)
	// find the log file from which replay will start if the request
	// number is in range, otherwise replay from all log files
	if !self.isInRange(requestNumber) {
		return nil
	}

	for idx, logIndex := range self.logIndex {
		logger.Debug("Trying to find request %d in %s", requestNumber, self.logFiles[idx].file.Name())
		if firstOffset = logIndex.requestOffset(requestNumber); firstOffset != -1 {
			logger.Debug("Found reqeust %d in %s at offset %d", requestNumber, self.logFiles[idx].file.Name(), firstOffset)
			firstIndex = idx
			break
		}
	}

	// the request must be at the end of the current log file
	if firstOffset == -1 {
		firstIndex = len(self.logIndex) - 1
		firstOffset = self.logIndex[firstIndex].requestOrLastOffset(requestNumber)
	}

	// issue #522. Copy the log files, otherwise a commit may cause
	// self.logFiles to be shifted to the left and `idx` in the loop
	// will be off by one, then by two, etc.
	logFiles := make([]*log, len(self.logFiles))
	copy(logFiles, self.logFiles)

outer:
	for idx := firstIndex; idx < len(logFiles); idx++ {
		logFile := logFiles[idx]
		if idx > firstIndex {
			firstOffset = -1
		}
		logger.Info("Replaying from %s:%d", logFile.file.Name(), firstOffset)
		count := 0
		ch, stopChan := logFile.dupAndReplayFromOffset(shardIds, firstOffset, requestNumber)
		defer close(stopChan)
		for {
			x := <-ch
			if x == nil {
				logger.Info("%s yielded %d requests", logFile.file.Name(), count)
				continue outer
			}

			if x.err != nil {
				return x.err
			}

			logger.Debug("Yielding request %d", x.request.GetRequestNumber())
			if err := yield(x.request, x.shardId); err != nil {
				logger.Debug("Stopping replay due to error: %s", err)
				stopChan <- struct{}{}
				return err
			}
			count++
		}
	}
	return nil
}

func (self *WAL) Close() error {
	return self.closeCommon(true)
}

func (self *WAL) closeWithoutBookmarking() error {
	return self.closeCommon(false)
}

func (self *WAL) closeCommon(shouldBookmark bool) error {
	confirmationChan := make(chan *confirmation)
	self.entries <- &closeEntry{confirmationChan, shouldBookmark}
	confirmation := <-confirmationChan
	return confirmation.err
}

func (self *WAL) processClose(shouldBookmark bool) error {
	logger.Info("Closing WAL")
	for idx, logFile := range self.logFiles {
		logFile.syncFile()
		logFile.close()
		self.logIndex[idx].syncFile()
		self.logIndex[idx].close()
	}
	if shouldBookmark {
		self.bookmark()
	}
	logger.Info("Closed WAL")
	return nil
}

// PRIVATE functions

func (self *WAL) processEntries() {
	for {
		e := <-self.entries
		switch x := e.(type) {
		case *commitEntry:
			self.processCommitEntry(x)
		case *appendEntry:
			self.processAppendEntry(x)
		case *bookmarkEntry:
			err := self.bookmark()
			if err != nil {
				x.confirmation <- &confirmation{0, err}
				continue
			}
			x.confirmation <- &confirmation{0, self.index()}
		case *closeEntry:
			x.confirmation <- &confirmation{0, self.processClose(x.shouldBookmark)}
			logger.Info("Closing wal")
			return
		default:
			panic(fmt.Errorf("unknown entry type %T", e))
		}
	}
}

func (self *WAL) assignSequenceNumbers(shardId uint32, request *protocol.Request) {
	if len(request.MultiSeries) == 0 {
		return
	}
	sequenceNumber := self.state.getCurrentSequenceNumber(shardId)
	for _, s := range request.MultiSeries {
		for _, p := range s.Points {
			if p.SequenceNumber != nil {
				continue
			}
			sequenceNumber++
			p.SequenceNumber = proto.Uint64(sequenceNumber*HOST_ID_OFFSET + uint64(self.serverId))
		}
		self.state.setCurrentSequenceNumber(shardId, sequenceNumber)
	}
}

func (self *WAL) processAppendEntry(e *appendEntry) {
	nextRequestNumber := self.state.getNextRequestNumber()
	e.request.RequestNumber = proto.Uint32(nextRequestNumber)
	self.assignSequenceNumbers(e.shardId, e.request)

	if e.assignSeqOnly {
		e.confirmation <- &confirmation{e.request.GetRequestNumber(), nil}
		return
	}

	if len(self.logFiles) == 0 {
		if _, err := self.createNewLog(nextRequestNumber); err != nil {
			e.confirmation <- &confirmation{0, err}
			return
		}
		self.state.FirstSuffix = nextRequestNumber
	}

	lastLogFile := self.logFiles[len(self.logFiles)-1]
	logger.Debug("appending request %d", e.request.GetRequestNumber())
	err := lastLogFile.appendRequest(e.request, e.shardId)
	if err != nil {
		e.confirmation <- &confirmation{0, err}
		return
	}
	self.state.CurrentFileOffset = self.logFiles[len(self.logFiles)-1].offset()

	self.requestsSinceLastIndex++
	self.requestsSinceLastBookmark++
	self.requestsSinceLastFlush++
	self.requestsSinceRotation++
	logger.Debug("requestsSinceRotation: %d", self.requestsSinceRotation)
	if rotated, err := self.rotateTheLogFile(nextRequestNumber); err != nil || rotated {
		e.confirmation <- &confirmation{e.request.GetRequestNumber(), err}
		return
	}

	self.conditionalBookmarkAndIndex()
	e.confirmation <- &confirmation{e.request.GetRequestNumber(), nil}
}

func (self *WAL) processCommitEntry(e *commitEntry) {
	logger.Debug("commiting %d for server %d", e.requestNumber, e.serverId)
	self.state.commitRequestNumber(e.serverId, e.requestNumber)
	idx := self.firstLogFile()
	if idx == 0 {
		e.confirmation <- &confirmation{0, nil}
		return
	}

	var unusedLogFiles []*log
	var unusedLogIndex []*index

	logger.Debug("Removing some unneeded log files: %d", idx)
	unusedLogFiles, self.logFiles = self.logFiles[:idx], self.logFiles[idx:]
	unusedLogIndex, self.logIndex = self.logIndex[:idx], self.logIndex[idx:]
	for logIdx, logFile := range unusedLogFiles {
		logger.Info("Deleting %s", logFile.file.Name())
		logFile.close()
		logFile.delete()
		logIndex := unusedLogIndex[logIdx]
		logIndex.close()
		logIndex.delete()
	}
	self.state.FirstSuffix = self.logFiles[0].suffix()
	e.confirmation <- &confirmation{0, nil}
}

// creates a new log file using the next suffix and initializes its
// state with the state of the last log file
func (self *WAL) createNewLog(firstRequestNumber uint32) (*log, error) {
	self.nextLogFileSuffix++
	logFileName := path.Join(self.config.WalDir, fmt.Sprintf("log.%d", firstRequestNumber))
	log, _, err := self.openLog(logFileName)
	if err != nil {
		return nil, err
	}
	self.state.CurrentFileSuffix = log.suffix()
	self.state.CurrentFileOffset = 0
	return log, nil
}

func (self *WAL) openLog(logFileName string) (*log, *index, error) {
	logger.Info("Opening log file %s", logFileName)

	logFile, err := os.OpenFile(logFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, nil, err
	}
	log, err := newLog(logFile, self.config)
	if err != nil {
		return nil, nil, err
	}

	self.logFiles = append(self.logFiles, log)
	suffix := strings.TrimPrefix(path.Base(logFileName), "log.")
	indexFileName := path.Join(self.config.WalDir, "index."+suffix)
	logger.Info("Opening index file %s", indexFileName)
	index, err := newIndex(indexFileName)
	if err != nil {
		logger.Error("Cannot open index file %s", err)
		log.close()
		return nil, nil, err
	}
	self.logIndex = append(self.logIndex, index)
	return log, index, nil
}

// Will assign sequence numbers if null. Returns a unique id that
// should be marked as committed for each server as it gets confirmed.
func (self *WAL) AssignSequenceNumbersAndLog(request *protocol.Request, shard Shard) (uint32, error) {
	confirmationChan := make(chan *confirmation)
	self.entries <- &appendEntry{confirmationChan, request, shard.Id(), false}
	confirmation := <-confirmationChan

	// we should panic if the wal cannot append the request
	if confirmation.err != nil {
		panic(confirmation.err)
	}
	return confirmation.requestNumber, confirmation.err
}

// Assigns sequence numbers if null.
func (self *WAL) AssignSequenceNumbers(request *protocol.Request) error {
	confirmationChan := make(chan *confirmation)
	self.entries <- &appendEntry{confirmationChan, request, 0, true}
	confirmation := <-confirmationChan

	// we should panic if the wal cannot append the request
	if confirmation.err != nil {
		panic(confirmation.err)
	}
	return nil
}

// returns the first log file that contains the given request number
func (self *WAL) firstLogFile() int {
	for idx, logIndex := range self.logIndex {
		for _, requestNumber := range self.state.ServerLastRequestNumber {
			// if no server needs to keep this log file arround we delete it
			if logIndex.requestOffset(requestNumber) != -1 {
				return idx
			}
		}
	}

	if len(self.logIndex) > 0 {
		return len(self.logIndex) - 1
	}
	return 0
}

func (self *WAL) shouldRotateTheLogFile() bool {
	return self.requestsSinceRotation >= self.config.WalRequestsPerLogFile
}

func (self *WAL) recover() error {
	for idx, logFile := range self.logFiles {
		self.requestsSinceLastIndex = 0
		self.requestsSinceRotation = self.logIndex[idx].getLength()

		lastOffset := self.logIndex[idx].getLastOffset()
		logger.Debug("Getting file size for %s[%d]", logFile.file.Name(), logFile.file.Fd())
		stat, err := logFile.file.Stat()
		if err != nil {
			return err
		}
		logger.Info("Checking %s, last: %d, size: %d", logFile.file.Name(), lastOffset, stat.Size())
		replay, _ := logFile.dupAndReplayFromOffset(nil, lastOffset, 0)
		firstOffset := int64(-1)
		for {
			replayRequest := <-replay

			if replayRequest == nil {
				break
			}

			self.state.LargestRequestNumber = replayRequest.requestNumber
			if err := replayRequest.err; err != nil {
				return err
			}

			for _, s := range replayRequest.request.MultiSeries {
				for _, point := range s.Points {
					sequenceNumber := (point.GetSequenceNumber() - uint64(self.serverId)) / HOST_ID_OFFSET
					self.state.recover(replayRequest.shardId, sequenceNumber)
				}
			}

			if firstOffset == -1 {
				firstOffset = replayRequest.startOffset
			}

			self.requestsSinceLastIndex++
			self.requestsSinceRotation++
			logger.Debug("recovery requestsSinceLastIndex: %d, requestNumber: %d", self.requestsSinceLastIndex, replayRequest.request.GetRequestNumber())
			logger.Debug("largestrequestnumber: %d\n", self.state.LargestRequestNumber)

			if self.requestsSinceLastIndex < self.config.WalIndexAfterRequests {
				continue
			}

			self.logIndex[idx].addEntry(
				replayRequest.requestNumber-uint32(self.requestsSinceLastIndex),
				uint32(replayRequest.requestNumber),
				firstOffset,
				replayRequest.endOffset,
			)
		}
	}

	logger.Debug("Finished wal recovery")
	return nil
}

func (self *WAL) rotateTheLogFile(nextRequestNumber uint32) (bool, error) {
	if !self.shouldRotateTheLogFile() && nextRequestNumber != math.MaxUint32 {
		return false, nil
	}

	self.requestsSinceRotation = 0
	if self.requestsSinceLastIndex > 0 {
		self.index()
	}
	lastEntryIndex := len(self.logFiles) - 1
	lastLogFile := self.logFiles[lastEntryIndex]
	lastIndex := self.logIndex[lastEntryIndex]
	if err := lastLogFile.syncFile(); err != nil {
		return false, err
	}
	if err := lastIndex.syncFile(); err != nil {
		return false, err
	}
	lastLogFile.close()
	lastIndex.close()
	lastLogFile, err := self.createNewLog(nextRequestNumber + 1)
	if err != nil {
		return false, err
	}
	logger.Info("Rotating log. New log file %s", lastLogFile.file.Name())
	return true, nil
}

func (self *WAL) conditionalBookmarkAndIndex() {
	shouldFlush := false
	logger.Debug("requestsSinceLastIndex: %d", self.requestsSinceLastIndex)
	if self.requestsSinceLastIndex >= self.config.WalIndexAfterRequests {
		self.index()
	}

	if self.requestsSinceLastBookmark >= self.config.WalBookmarkAfterRequests {
		self.bookmark()
	}

	if self.requestsSinceLastFlush >= self.config.WalFlushAfterRequests || shouldFlush {
		self.flush()
	}
}

func (self *WAL) flush() error {
	logger.Debug("Fsyncing the log file to disk")
	self.requestsSinceLastFlush = 0
	lastEntryIndex := len(self.logFiles) - 1
	if err := self.logFiles[lastEntryIndex].syncFile(); err != nil {
		return err
	}
	if err := self.logIndex[lastEntryIndex].syncFile(); err != nil {
		return err
	}
	return nil
}

func (self *WAL) CreateCheckpoint() error {
	confirmationChan := make(chan *confirmation)
	self.entries <- &bookmarkEntry{confirmationChan}
	confirmation := <-confirmationChan
	return confirmation.err
}

func (self *WAL) bookmark() error {
	if err := self.state.writeToFile(); err != nil {
		logger.Error("Cannot write bookmark %s", err)
		return err
	}
	self.requestsSinceLastBookmark = 0
	return nil
}

func (self *WAL) index() error {
	if len(self.logFiles) == 0 || self.requestsSinceLastBookmark == 0 {
		return nil
	}

	lastIndex := self.logIndex[len(self.logIndex)-1]
	firstOffset := lastIndex.getLastOffset()
	lastIndex.addEntry(
		self.state.LargestRequestNumber+1-uint32(self.requestsSinceLastIndex),
		self.state.LargestRequestNumber,
		firstOffset,
		self.state.CurrentFileOffset,
	)
	self.requestsSinceLastIndex = 0
	return nil
}

package wal

import (
	"configuration"
	"fmt"
	"os"
	"path"
	"protocol"
	"sort"
	"strings"
)

type WAL struct {
	config             *configuration.Configuration
	logFiles           []*log
	serverId           uint32
	nextLogFileSuffix  int
	requestsPerLogFile int
}

const HOST_ID_OFFSET = uint64(10000)

func NewWAL(config *configuration.Configuration) (*WAL, error) {
	if config.WalDir == "" {
		return nil, fmt.Errorf("wal directory cannot be empty")
	}
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

	nextLogFileSuffix := 0
	logFiles := make([]*log, 0)
	for _, name := range names {
		if !strings.HasPrefix(name, "log.") {
			continue
		}
		f, err := os.OpenFile(path.Join(config.WalDir, name), os.O_RDWR, 0644)
		if err != nil {
			return nil, err
		}
		logFile, err := newLog(f, config)
		if err != nil {
			return nil, err
		}
		if suffix := logFile.suffix(); suffix > nextLogFileSuffix {
			nextLogFileSuffix = suffix
		}
		logFiles = append(logFiles, logFile)
	}

	// sort the logfiles by the first request number in the log
	sort.Sort(sortableLogSlice(logFiles))

	wal := &WAL{config: config, logFiles: logFiles, requestsPerLogFile: config.WalRequestsPerLogFile, nextLogFileSuffix: nextLogFileSuffix}

	// if we don't have any log files open yet, open a new one
	if len(logFiles) == 0 {
		_, err = wal.createNewLog()
	}
	return wal, err
}

func (self *WAL) SetServerId(id uint32) {
	self.serverId = id
}

func (self *WAL) createNewLog() (*log, error) {
	self.nextLogFileSuffix++
	logFileName := path.Join(self.config.WalDir, fmt.Sprintf("log.%d", self.nextLogFileSuffix))
	logFile, err := os.OpenFile(logFileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	log, err := newLog(logFile, self.config)
	if err != nil {
		return nil, err
	}

	// TODO: this is ugly, we have to copy some of the state to the new
	// log. Find a better way to do this, possibly separating the state
	// that we need to keep between log files in their own file
	if len(self.logFiles) > 0 {
		lastLogFile := self.logFiles[len(self.logFiles)-1]
		// update the new state to continue from where the last log file
		// left off
		log.state.continueFromState(lastLogFile.state)
	}
	self.logFiles = append(self.logFiles, log)
	return log, nil
}

// Will assign sequence numbers if null. Returns a unique id that
// should be marked as committed for each server as it gets confirmed.
func (self *WAL) AssignSequenceNumbersAndLog(request *protocol.Request, shard Shard) (uint32, error) {
	lastLogFile := self.logFiles[len(self.logFiles)-1]
	if lastLogFile.state.TotalNumberOfRequests >= self.requestsPerLogFile {
		err := lastLogFile.forceBookmark(true)
		if err != nil {
			return 0, err
		}
		lastLogFile, err = self.createNewLog()
		if err != nil {
			return 0, err
		}
	}

	requestNumber, err := lastLogFile.appendRequest(request, shard.Id())
	if err != nil {
		return 0, err
	}
	return requestNumber, nil
}

// Marks a given request for a given server as committed
func (self *WAL) Commit(requestNumber uint32, server Server) error {
	lastLogFile := self.logFiles[len(self.logFiles)-1]
	lastLogFile.state.commitRequestNumber(server.Id(), requestNumber)
	lowestCommitedRequestNumber := lastLogFile.state.LowestCommitedRequestNumber()

	index := self.firstLogFile(lowestCommitedRequestNumber)
	if index == 0 {
		return nil
	}

	var unusedLogFiles []*log
	unusedLogFiles, self.logFiles = self.logFiles[:index], self.logFiles[index:]
	for _, logFile := range unusedLogFiles {
		logFile.close()
		filePath := path.Join(self.config.WalDir, fmt.Sprintf("bookmark.%d", logFile.suffix()))
		os.Remove(filePath)
		filePath = path.Join(self.config.WalDir, fmt.Sprintf("log.%d", logFile.suffix()))
		os.Remove(filePath)
	}
	return nil
}

func (self *WAL) getFirstLogFile(requestNumber uint32) func(int) bool {
	return func(i int) bool {
		if self.logFiles[i].firstRequestNumber() > requestNumber {
			return true
		}
		return false
	}
}

// returns the first log file that contains the given request number
func (self *WAL) firstLogFile(requestNumber uint32) int {
	lengthLogFiles := len(self.logFiles)
	if requestNumber >= self.logFiles[lengthLogFiles-1].firstRequestNumber() {
		return lengthLogFiles - 1
	} else if requestNumber <= self.logFiles[0].firstRequestNumber() {
		return 0
	}
	return sort.Search(lengthLogFiles, self.getFirstLogFile(requestNumber)) - 1
}

// In the case where this server is running and another one in the cluster stops responding, at some point this server will have to just write
// requests to disk. When the downed server comes back up, it's this server's responsibility to send out any writes that were queued up. If
// the yield function returns nil then the request is committed.
func (self *WAL) RecoverServerFromRequestNumber(requestNumber uint32, shardIds []uint32, yield func(request *protocol.Request, shardId uint32) error) error {
	var firstLogFile int

outer:
	for _, logFile := range self.logFiles[firstLogFile:] {
		ch, stopChan := logFile.replayFromRequestNumber(shardIds, requestNumber)
		for {
			x := <-ch
			if x == nil {
				continue outer
			}

			if x.err != nil {
				return x.err
			}

			if err := yield(x.request, x.shardId); err != nil {
				stopChan <- struct{}{}
				return err
			}
		}
	}
	return nil
}

func (self *WAL) Close() error {
	for _, l := range self.logFiles {
		if err := l.close(); err != nil {
			return err
		}
	}
	return nil
}

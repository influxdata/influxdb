package wal

import (
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"

	"code.google.com/p/goprotobuf/proto"
	logger "code.google.com/p/log4go"
	"github.com/influxdb/influxdb/configuration"
	"github.com/influxdb/influxdb/protocol"
)

type log struct {
	closed                 bool
	fileSize               uint64
	file                   *os.File
	requestsSinceLastFlush int
	config                 *configuration.Configuration
	cachedSuffix           uint32
}

func newLog(file *os.File, config *configuration.Configuration) (*log, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}

	size := uint64(info.Size())
	suffixString := strings.TrimLeft(path.Base(file.Name()), "log.")
	suffix, err := strconv.ParseUint(suffixString, 10, 32)
	if err != nil {
		return nil, err
	}

	l := &log{
		file:         file,
		fileSize:     size,
		closed:       false,
		config:       config,
		cachedSuffix: uint32(suffix),
	}

	return l, l.check()
}

func (self *log) check() error {
	file, err := self.dupLogFile()
	if err != nil {
		return err
	}
	info, err := file.Stat()
	if err != nil {
		return err
	}
	size := info.Size()
	offset, err := file.Seek(0, os.SEEK_SET)
	if err != nil {
		return err
	}
	for {
		n, hdr, err := self.getNextHeader(file)
		if err != nil {
			return err
		}
		if n == 0 || hdr.length == 0 {
			logger.Warn("%s was truncated to %d since the file has a zero size request", self.file.Name(), offset)
			return self.file.Truncate(offset)
		}
		if offset+int64(n)+int64(hdr.length) > size {
			// file is incomplete, truncate
			logger.Warn("%s was truncated to %d since the file ends prematurely", self.file.Name(), offset)
			return self.file.Truncate(offset)
		}
		bytes := make([]byte, hdr.length)
		_, err = file.Read(bytes)
		if err != nil {
			return err
		}

		// this request is invalid truncate file
		req := &protocol.Request{}
		err = req.Decode(bytes)
		if err != nil {
			logger.Warn("%s was truncated to %d since the end of the file contains invalid data", self.file.Name(), offset)
			// truncate file and return
			return self.file.Truncate(offset)
		}

		offset += int64(n) + int64(hdr.length)
	}
}

func (self *log) offset() int64 {
	offset, _ := self.file.Seek(0, os.SEEK_CUR)
	return offset
}

func (self *log) suffix() uint32 {
	return self.cachedSuffix
}

// this is for testing only
func (self *log) syncFile() error {
	return self.file.Sync()
}

func (self *log) close() error {
	logger.Debug("Closing %s", self.file.Name())
	return self.file.Close()
}

func (self *log) delete() error {
	logger.Debug("Deleting %s", self.file.Name())
	return os.Remove(self.file.Name())
}

func (self *log) appendRequest(request *protocol.Request, shardId uint32) error {
	bytes, err := request.Encode()

	if err != nil {
		return err
	}
	// every request is preceded with the length, shard id and the request number
	hdr := &entryHeader{
		shardId:       shardId,
		requestNumber: request.GetRequestNumber(),
		length:        uint32(len(bytes)),
	}
	writtenHdrBytes, err := hdr.Write(self.file)
	if err != nil {
		logger.Error("Error while writing header: %s", err)
		return err
	}
	written, err := self.file.Write(bytes)
	if err != nil {
		logger.Error("Error while writing request: %s", err)
		return err
	}
	if written < len(bytes) {
		err = fmt.Errorf("Couldn't write entire request")
		logger.Error("Error while writing request: %s", err)
		return err
	}
	self.fileSize += uint64(writtenHdrBytes + written)
	return nil
}

func (self *log) dupLogFile() (*os.File, error) {
	return os.OpenFile(self.file.Name(), os.O_RDWR, 0)
}

// replay requests starting at the given requestNumber and for the
// given shard ids. Return all requests if shardIds is empty
func (self *log) dupAndReplayFromOffset(shardIds []uint32, offset int64, rn uint32) (chan *replayRequest, chan struct{}) {
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
		if err = self.skip(file, offset, rn); err != nil {
			sendOrStop(newErrorReplayRequest(err), replayChan, stopChan)
			close(replayChan)
			return
		}
		shardIdsSet := map[uint32]struct{}{}
		for _, shardId := range shardIds {
			shardIdsSet[shardId] = struct{}{}
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

func (self *log) skip(file *os.File, offset int64, rn uint32) error {
	if offset == -1 {
		_, err := file.Seek(0, os.SEEK_SET)
		return err
	}
	logger.Debug("Replaying from file offset %d", offset)
	_, err := file.Seek(int64(offset), os.SEEK_SET)
	if err != nil {
		return err
	}
	return self.skipToRequest(file, rn)
}

func (self *log) skipRequest(file *os.File, hdr *entryHeader) (err error) {
	_, err = file.Seek(int64(hdr.length), os.SEEK_CUR)
	return
}

func (self *log) skipToRequest(file *os.File, requestNumber uint32) error {
	for {
		n, hdr, err := self.getNextHeader(file)
		if n == 0 {
			// EOF
			return nil
		}
		if err != nil {
			return err
		}
		if hdr.requestNumber < requestNumber {
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

	offset, err := file.Seek(0, os.SEEK_CUR)
	logger.Info("replaying from file location %d", offset)
	if err != nil {
		sendOrStop(newErrorReplayRequest(err), replayChan, stopChan)
		return
	}

	defer func() { close(replayChan) }()
	for {
		numberOfBytes, hdr, err := self.getNextHeader(file)
		if numberOfBytes == 0 {
			break
		}

		if err != nil {
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
		if err != nil {
			sendOrStop(newErrorReplayRequest(err), replayChan, stopChan)
			return
		}

		if uint32(read) != hdr.length {
			// file ends prematurely, probably a request is being written
			logger.Debug("%s ends prematurely. Truncating to %d", file.Name(), offset)
			return
		}

		req := &protocol.Request{}
		err = req.Decode(bytes)
		if err != nil {
			sendOrStop(newErrorReplayRequest(err), replayChan, stopChan)
			return
		}

		req.RequestNumber = proto.Uint32(hdr.requestNumber)
		replayRequest := &replayRequest{hdr.requestNumber, req, hdr.shardId, offset, offset + int64(numberOfBytes) + int64(hdr.length), nil}
		if sendOrStop(replayRequest, replayChan, stopChan) {
			return
		}
		offset = replayRequest.endOffset
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

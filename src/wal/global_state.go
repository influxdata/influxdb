package wal

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"io"
	"math"
	"os"
)

type GlobalState struct {
	// used for creating index entries
	CurrentFileSuffix uint32
	CurrentFileOffset int64

	// keep track of the next request number
	LargestRequestNumber uint32

	// used for rollover
	FirstSuffix uint32

	// last seq number used
	ShardLastSequenceNumber map[uint32]uint64

	// committed request number per server
	ServerLastRequestNumber map[uint32]uint32

	// path to the state file
	path string
}

func newGlobalState(path string) (*GlobalState, error) {
	f, err := os.Open(path)
	state := &GlobalState{
		ServerLastRequestNumber: map[uint32]uint32{},
		ShardLastSequenceNumber: map[uint32]uint64{},
		path: path,
	}
	if os.IsNotExist(err) {
		return state, nil
	}
	if err != nil {
		return nil, err
	}
	if err := state.read(f); err != nil {
		return nil, err
	}
	state.path = path
	return state, nil
}

func (self *GlobalState) writeToFile() error {
	newFile, err := os.OpenFile(self.path+".new", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	if _, err := newFile.Seek(0, os.SEEK_SET); err != nil {
		return err
	}

	if err := self.write(newFile); err != nil {
		return err
	}

	if err := newFile.Sync(); err != nil {
		return err
	}

	if err := newFile.Close(); err != nil {
		return err
	}

	os.Remove(self.path)
	return os.Rename(self.path+".new", self.path)
}

func (self *GlobalState) write(w io.Writer) error {
	fmt.Fprintf(w, "%d\n", 1) // write the version
	return gob.NewEncoder(w).Encode(self)
}

func (self *GlobalState) read(r io.Reader) error {
	// skip the version
	reader := bufio.NewReader(r)
	// read the version line
	_, err := reader.ReadString('\n')
	if err != nil {
		return err
	}
	return gob.NewDecoder(reader).Decode(self)
}

func (self *GlobalState) recover(shardId uint32, sequenceNumber uint64) {
	lastSequenceNumber := self.ShardLastSequenceNumber[shardId]

	if sequenceNumber > lastSequenceNumber {
		self.ShardLastSequenceNumber[shardId] = sequenceNumber
	}
}

func (self *GlobalState) getNextRequestNumber() uint32 {
	self.LargestRequestNumber++
	return self.LargestRequestNumber
}

func (self *GlobalState) getCurrentSequenceNumber(shardId uint32) uint64 {
	return self.ShardLastSequenceNumber[shardId]
}

func (self *GlobalState) setCurrentSequenceNumber(shardId uint32, sequenceNumber uint64) {
	self.ShardLastSequenceNumber[shardId] = sequenceNumber
}

func (self *GlobalState) commitRequestNumber(serverId, requestNumber uint32) {
	// TODO: we need a way to verify the request numbers, the following
	// won't work though when the request numbers roll over

	// if currentRequestNumber := self.ServerLastRequestNumber[serverId]; requestNumber < currentRequestNumber {
	// 	panic(fmt.Errorf("Expected rn %d to be >= %d", requestNumber, currentRequestNumber))
	// }
	self.ServerLastRequestNumber[serverId] = requestNumber
}

func (self *GlobalState) LowestCommitedRequestNumber() uint32 {
	requestNumber := uint32(math.MaxUint32)
	for _, number := range self.ServerLastRequestNumber {
		if number < requestNumber {
			requestNumber = number
		}
	}
	return requestNumber
}

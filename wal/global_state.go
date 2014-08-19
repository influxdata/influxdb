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

// from version 0.7 to 0.8 the Suffix variables changed from
// ints to uint32s. We need this struct to convert them.
type oldGlobalState struct {
	GlobalState
	CurrentFileSuffix int
	FirstSuffix       int
}

func newGlobalState(path string) (*GlobalState, error) {
	f, err := os.Open(path)
	if err == nil {
		defer f.Close()
	}

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
	newFile, err := os.OpenFile(self.path+".new", os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	// always close and ignore any errors on exit
	defer newFile.Close()

	if err := self.write(newFile); err != nil {
		return err
	}

	if err := newFile.Sync(); err != nil {
		return err
	}

	if err := newFile.Close(); err != nil {
		return err
	}

	if err := os.Remove(self.path); nil != err && !os.IsNotExist(err) {
		return err
	}
	return os.Rename(self.path+".new", self.path)
}

func (self *GlobalState) write(w io.Writer) error {
	fmt.Fprintf(w, "%d\n", 1) // write the version
	return gob.NewEncoder(w).Encode(self)
}

func (self *GlobalState) read(r *os.File) error {
	// skip the version
	reader := bufio.NewReader(r)
	// read the version line
	_, err := reader.ReadString('\n')
	if err != nil {
		return err
	}
	err = gob.NewDecoder(reader).Decode(self)

	// from version 0.7 to 0.8 the type of the Suffix variables
	// changed to uint32. Catch this and convert to a new GlobalState object.
	if err != nil {
		old := &oldGlobalState{}
		r.Seek(int64(0), 0)
		reader := bufio.NewReader(r)
		// read the version line
		_, err := reader.ReadString('\n')
		if err != nil {
			return err
		}
		err = gob.NewDecoder(reader).Decode(old)
		if err != nil {
			return err
		}
		self.CurrentFileOffset = old.CurrentFileOffset
		self.CurrentFileSuffix = uint32(old.CurrentFileSuffix)
		self.LargestRequestNumber = old.LargestRequestNumber
		self.FirstSuffix = uint32(old.FirstSuffix)
		self.ShardLastSequenceNumber = old.ShardLastSequenceNumber
		self.ServerLastRequestNumber = old.ServerLastRequestNumber
	}
	return nil
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

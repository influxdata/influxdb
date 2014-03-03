package wal

import (
	"encoding/gob"
	"io"
	"math"
)

const (
	CURRENT_VERSION = 1
)

type globalState struct {
}

type state struct {
	// per log file state
	Version                   byte
	RequestsSinceLastBookmark int
	RequestsSinceLastIndex    uint32
	FileOffset                int64 // the file offset at which this bookmark was created
	Index                     *index
	TotalNumberOfRequests     int
	FirstRequestNumber        uint32
	LargestRequestNumber      uint32
	ShardLastSequenceNumber   map[uint32]uint64
	ServerLastRequestNumber   map[uint32]uint32
}

func (self *state) isAfter(left, right uint32) bool {
	if left == right {
		return false
	}
	if left >= self.FirstRequestNumber && right >= self.FirstRequestNumber {
		return left > right
	}
	if left <= self.LargestRequestNumber && right <= self.LargestRequestNumber {
		return left > right
	}
	return left <= self.LargestRequestNumber
}

func (self *state) isAfterOrEqual(left, right uint32) bool {
	return left == right || self.isAfter(left, right)
}

func (self *state) isBefore(left, right uint32) bool {
	return !self.isAfterOrEqual(left, right)
}

func (self *state) isBeforeOrEqual(left, right uint32) bool {
	return !self.isAfter(left, right)
}

func newState() *state {
	return &state{
		Version: CURRENT_VERSION,
		Index: &index{
			Entries: make([]*indexEntry, 0),
		},
		LargestRequestNumber:    0,
		ShardLastSequenceNumber: make(map[uint32]uint64),
		ServerLastRequestNumber: make(map[uint32]uint32),
	}
}

func (self *state) recover(replay *replayRequest) {
	if self.LargestRequestNumber < replay.requestNumber {
		self.LargestRequestNumber = replay.requestNumber
	}

	lastSequenceNumber := self.ShardLastSequenceNumber[replay.shardId]

	for _, p := range replay.request.Series.Points {
		if seq := p.GetSequenceNumber(); seq > lastSequenceNumber {
			lastSequenceNumber = seq
		}
	}

	self.ShardLastSequenceNumber[replay.shardId] = lastSequenceNumber
}

func (self *state) setFileOffset(offset int64) {
	self.FileOffset = offset
}

func (self *state) getNextRequestNumber() uint32 {
	self.LargestRequestNumber++
	return self.LargestRequestNumber
}

func (self *state) continueFromState(state *state) {
	self.FirstRequestNumber = state.FirstRequestNumber
	self.LargestRequestNumber = state.LargestRequestNumber
	self.ShardLastSequenceNumber = state.ShardLastSequenceNumber
	self.ServerLastRequestNumber = state.ServerLastRequestNumber
}

func (self *state) getCurrentSequenceNumber(shardId uint32) uint64 {
	return self.ShardLastSequenceNumber[shardId]
}

func (self *state) setCurrentSequenceNumber(shardId uint32, sequenceNumber uint64) {
	self.ShardLastSequenceNumber[shardId] = sequenceNumber
}

func (self *state) commitRequestNumber(serverId, requestNumber uint32) {
	self.ServerLastRequestNumber[serverId] = requestNumber
}

func (self *state) LowestCommitedRequestNumber() uint32 {
	requestNumber := uint32(math.MaxUint32)
	for _, number := range self.ServerLastRequestNumber {
		if number < requestNumber {
			requestNumber = number
		}
	}
	return requestNumber
}

func (self *state) write(w io.Writer) error {
	return gob.NewEncoder(w).Encode(self)
}

func (self *state) read(r io.Reader) error {
	return gob.NewDecoder(r).Decode(self)
}

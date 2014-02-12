package wal

const (
	CURRENT_VERSION = 1
)

type state struct {
	version                 byte
	currentRequestNumber    uint32
	shardLastSequenceNumber map[uint32]uint64
	serverLastRequestNumber map[uint32]uint32
}

func newState() *state {
	return &state{
		version:                 CURRENT_VERSION,
		currentRequestNumber:    0,
		shardLastSequenceNumber: make(map[uint32]uint64),
		serverLastRequestNumber: make(map[uint32]uint32),
	}
}

func (self *state) getNextRequestNumber() uint32 {
	self.currentRequestNumber++
	return self.currentRequestNumber
}

func (self *state) getCurrentSequenceNumber(shardId uint32) uint64 {
	return self.shardLastSequenceNumber[shardId]
}

func (self *state) setCurrentSequenceNumber(shardId uint32, sequenceNumber uint64) {
	self.shardLastSequenceNumber[shardId] = sequenceNumber
}

func (self *state) commitRequestNumber(serverId, requestNumber uint32) {
	self.serverLastRequestNumber[serverId] = requestNumber
}

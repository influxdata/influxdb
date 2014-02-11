package wal

const (
	CURRENT_VERSION = 1
)

type state struct {
	version                 byte
	currentRequestNumber    uint32
	shardLastSequenceNumber map[uint32]uint32
	serverLastRequestNumber map[uint32]uint32
}

func (self *state) getNextRequestNumber() uint32 {
	self.currentRequestNumber++
	return self.currentRequestNumber
}

func (self *state) getCurrentSequenceNumber(shardId uint32) uint32 {
	return self.shardLastSequenceNumber[shardId]
}

func (self *state) setCurrentSequenceNumber(shardId, sequenceNumber uint32) {
	self.shardLastSequenceNumber[shardId] = sequenceNumber
}

func (self *state) commitRequestNumber(serverId, requestNumber uint32) {
	self.serverLastRequestNumber[serverId] = requestNumber
}

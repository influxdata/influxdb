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

func (s *state) getNextRequestNumber() uint32 {
	s.currentRequestNumber++
	return s.currentRequestNumber
}

func (s *state) getCurrentSequenceNumber(shardId uint32) uint32 {
	return s.shardLastSequenceNumber[shardId]
}

func (s *state) setCurrentSequenceNumber(shardId, sequenceNumber uint32) {
	s.shardLastSequenceNumber[shardId] = sequenceNumber
}

func (s *state) commitRequestNumber(serverId, requestNumber uint32) {
	s.serverLastRequestNumber[serverId] = requestNumber
}

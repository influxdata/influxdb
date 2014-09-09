package datastore

import "fmt"

type rawColumnValue struct {
	time     int64
	sequence uint64
	value    []byte
}

func (rcv rawColumnValue) before(other *rawColumnValue) bool {
	if rcv.time < other.time {
		return true
	}

	if rcv.time == other.time && rcv.sequence < other.sequence {
		return true
	}

	return false
}

func (rcv rawColumnValue) after(other *rawColumnValue) bool {
	if rcv.time > other.time {
		return true
	}

	if rcv.time == other.time && rcv.sequence > other.sequence {
		return true
	}

	return false
}

func (rcv rawColumnValue) String() string {
	return fmt.Sprintf("[time: %d, sequence: %d, value: %v]", rcv.time, rcv.sequence, rcv.value)
}

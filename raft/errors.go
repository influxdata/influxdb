package raft

import "errors"

var (
	// ErrLogEntryTooLarge is returned when a log entry's data is larger than
	// the maximum allowed value specified in MaxLogEntrySize.
	ErrLogEntryTooLarge = errors.New("log entry too large")
)

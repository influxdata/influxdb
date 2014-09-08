package raft

import "errors"

var (
	// ErrClosed is returned when the log is closed.
	ErrClosed = errors.New("log closed")

	// ErrAlreadyOpen is returned when opening a log that is already open.
	ErrAlreadyOpen = errors.New("log already open")

	// ErrURLRequired is returned when opening a log without a URL set.
	ErrURLRequired = errors.New("url required")

	// ErrLogExists is returned when initializing an already existing log.
	ErrLogExists = errors.New("log exists")

	// ErrNotLeader is returned performing leader operations on a non-leader.
	ErrNotLeader = errors.New("not leader")

	// ErrStaleTerm is returned when a term is before the current term.
	ErrStaleTerm = errors.New("stale term")

	// ErrOutOfDateLog is returned when a candidate's log is not up to date.
	ErrOutOfDateLog = errors.New("out of date log")

	// ErrUncommittedIndex is returned when a stream is started from an
	// uncommitted log index.
	ErrUncommittedIndex = errors.New("uncommitted index")

	// ErrAlreadyVoted is returned when a vote has already been cast for
	// a different candidate in the same election term.
	ErrAlreadyVoted = errors.New("already voted")
)

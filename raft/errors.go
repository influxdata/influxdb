package raft

import "errors"

var (
	// ErrClosed is returned when the log is closed.
	ErrClosed = errors.New("log closed")

	// ErrOpen is returned when opening a log that is already open.
	ErrOpen = errors.New("log already open")

	// ErrInitialized is returned when initializing a log that is already a
	// member of a cluster.
	ErrInitialized = errors.New("log already initialized")

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

	// ErrNodeNotFound is returned when referencing a non-existent node.
	ErrNodeNotFound = errors.New("node not found")

	// ErrInvalidNodeID is returned when using a node id of zero.
	ErrInvalidNodeID = errors.New("invalid node id")

	// ErrNodeURLRequired is returned a node config has no URL set.
	ErrNodeURLRequired = errors.New("node url required")

	// ErrDuplicateNodeID is returned when adding a node with an existing id.
	ErrDuplicateNodeID = errors.New("duplicate node id")

	// ErrDuplicateNodeURL is returned when adding a node with an existing URL.
	ErrDuplicateNodeURL = errors.New("duplicate node url")
)

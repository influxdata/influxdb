package raft

import "errors"

// The version of the protocol currently used by the Raft library. This
// can change as the library evolves and future versions should be
// backwards compatible.
const protocolVersion uint32 = 1

var errUnsupportedLogVersion = errors.New("Unsupported log version")

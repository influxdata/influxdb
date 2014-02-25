package wal

type confirmation struct {
	requestNumber uint32
	err           error
}

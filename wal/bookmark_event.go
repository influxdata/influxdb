package wal

type bookmarkEvent struct {
	shutdown         bool
	confirmationChan chan *confirmation
}

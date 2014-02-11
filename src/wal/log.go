package wal

import (
	"fmt"
	"os"
	"protocol"
)

type log struct {
	entries chan *entry
	state   *state
	file    *os.File
}

func newLog(file *os.File) *log {
	l := &log{
		entries: make(chan *entry, 10),
		file:    file,
		state:   &state{},
	}

	go l.processEntries()
	return l
}

func (log *log) recover() error {
	return nil
}

func (log *log) processEntries() {
	for {
		select {
		case x := <-log.entries:
			// append the entry to the log file
			x.confirmation <- nil
		}
	}
}

func (log *log) appendRequest(request *protocol.Request) (uint32, error) {
	entry := &entry{make(chan *confirmation), request}
	log.entries <- entry
	confirmation := <-entry.confirmation
	return confirmation.requestNumber, confirmation.err
}

func (log *log) forceBookmark() error {
	return fmt.Errorf("not implemented yet")
}

package raft

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

// A log is a collection of log entries that are persisted to durable storage.
type Log struct {
	ApplyFunc   func(Command) (interface{}, error)
	file        *os.File
	path        string
	entries     []*LogEntry
	errors      []error
	commitIndex uint64
	mutex       sync.Mutex
	startIndex  uint64 // the index before the first entry in the Log entries
	startTerm   uint64
}

//------------------------------------------------------------------------------
//
// Constructor
//
//------------------------------------------------------------------------------

// Creates a new log.
func NewLog() *Log {
	return &Log{}
}

//------------------------------------------------------------------------------
//
// Accessors
//
//------------------------------------------------------------------------------

func (l *Log) SetStartIndex(i uint64) {
	l.startIndex = i
}

func (l *Log) StartIndex() uint64 {
	return l.startIndex
}

func (l *Log) SetStartTerm(t uint64) {
	l.startTerm = t
}

//--------------------------------------
// Log Indices
//--------------------------------------

// The current index in the log.
func (l *Log) CurrentIndex() uint64 {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if len(l.entries) == 0 {
		return l.startIndex
	}
	return l.entries[len(l.entries)-1].Index
}

// The current index in the log without locking
func (l *Log) internalCurrentIndex() uint64 {
	if len(l.entries) == 0 {
		return l.startIndex
	}
	return l.entries[len(l.entries)-1].Index
}

// The next index in the log.
func (l *Log) NextIndex() uint64 {
	return l.CurrentIndex() + 1
}

// The last committed index in the log.
func (l *Log) CommitIndex() uint64 {
	return l.commitIndex
}

// Determines if the log contains zero entries.
func (l *Log) IsEmpty() bool {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return (len(l.entries) == 0) && (l.startIndex == 0)
}

// The name of the last command in the log.
func (l *Log) LastCommandName() string {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if len(l.entries) > 0 {
		if command := l.entries[len(l.entries)-1].Command; command != nil {
			return command.CommandName()
		}
	}
	return ""
}

//--------------------------------------
// Log Terms
//--------------------------------------

// The current term in the log.
func (l *Log) CurrentTerm() uint64 {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if len(l.entries) == 0 {
		return l.startTerm
	}
	return l.entries[len(l.entries)-1].Term
}

//------------------------------------------------------------------------------
//
// Methods
//
//------------------------------------------------------------------------------

//--------------------------------------
// State
//--------------------------------------

// Opens the log file and reads existing entries. The log can remain open and
// continue to append entries to the end of the log.
func (l *Log) Open(path string) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	// Read all the entries from the log if one exists.
	var lastIndex int = 0
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		// Open the log file.
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		reader := bufio.NewReader(file)

		// Read the file and decode entries.
		for {
			if _, err := reader.Peek(1); err == io.EOF {
				break
			}

			// Instantiate log entry and decode into it.
			entry := NewLogEntry(l, 0, 0, nil)
			n, err := entry.Decode(reader)
			if err != nil {
				file.Close()
				if err = os.Truncate(path, int64(lastIndex)); err != nil {
					return fmt.Errorf("raft.Log: Unable to recover: %v", err)
				}
				break
			}

			// Append entry.
			l.entries = append(l.entries, entry)
			l.commitIndex = entry.Index

			// Apply the command.
			entry.result, err = l.ApplyFunc(entry.Command)

			l.errors = append(l.errors, err)

			lastIndex += n
		}

		file.Close()
	}

	// Open the file for appending.
	var err error
	l.file, err = os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	l.path = path
	return nil
}

// Closes the log file.
func (l *Log) Close() {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if l.file != nil {
		l.file.Close()
		l.file = nil
	}
	l.entries = make([]*LogEntry, 0)
	l.errors = make([]error, 0)
}

//--------------------------------------
// Entries
//--------------------------------------

// Creates a log entry associated with this log.
func (l *Log) CreateEntry(term uint64, command Command) *LogEntry {
	return NewLogEntry(l, l.NextIndex(), term, command)
}

// Checks if the log contains a given index/term combination.
func (l *Log) ContainsEntry(index uint64, term uint64) bool {
	if index <= l.startIndex || index > (l.startIndex+uint64(len(l.entries))) {
		return false
	}
	return (l.entries[index-1].Term == term)
}

// Retrieves a list of entries after a given index as well as the term of the
// index provided. A nil list of entries is returned if the index no longer
// exists because a snapshot was made.
func (l *Log) GetEntriesAfter(index uint64) ([]*LogEntry, uint64) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	// Return nil if index is before the start of the log.
	if index < l.startIndex {
		return nil, 0
	}

	// Return an error if the index doesn't exist.
	if index > (uint64(len(l.entries)) + l.startIndex) {
		panic(fmt.Sprintf("raft: Index is beyond end of log: %v %v", len(l.entries), index))
	}

	// If we're going from the beginning of the log then return the whole log.
	if index == l.startIndex {
		return l.entries, l.startTerm
	}

	debugln("[GetEntries] index ", index, "lastIndex", l.entries[len(l.entries)-1].Index)

	// Determine the term at the given entry and return a subslice.
	term := l.entries[index-1-l.startIndex].Term

	return l.entries[index-l.startIndex:], term
}

// Retrieves the error returned from an entry. The error can only exist after
// the entry has been committed.
func (l *Log) GetEntryError(entry *LogEntry) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if entry == nil {
		panic("raft: Log entry required for error retrieval")
	}

	if entry.Index > 0 && entry.Index <= uint64(len(l.errors)) {
		return l.errors[entry.Index-1]
	}
	return nil
}

//--------------------------------------
// Commit
//--------------------------------------

// Retrieves the last index and term that has been committed to the log.
func (l *Log) CommitInfo() (index uint64, term uint64) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	// If we don't have any entries then just return zeros.
	if l.commitIndex == 0 {
		return 0, 0
	}

	// no new commit log after snapshot
	if l.commitIndex == l.startIndex {
		return l.startIndex, l.startTerm
	}

	// Return the last index & term from the last committed entry.
	lastCommitEntry := l.entries[l.commitIndex-1-l.startIndex]
	return lastCommitEntry.Index, lastCommitEntry.Term
}

// Retrieves the last index and term that has been committed to the log.
func (l *Log) LastInfo() (index uint64, term uint64) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	// If we don't have any entries then just return zeros.
	if len(l.entries) == 0 {
		return l.startIndex, l.startTerm
	}

	// Return the last index & term
	lastEntry := l.entries[len(l.entries)-1]
	return lastEntry.Index, lastEntry.Term
}

// Updates the commit index
func (l *Log) UpdateCommitIndex(index uint64) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.commitIndex = index
}

// Updates the commit index and writes entries after that index to the stable storage.
func (l *Log) SetCommitIndex(index uint64) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if index > l.startIndex+uint64(len(l.entries)) {
		return fmt.Errorf("raft.Log: Commit index (%d) out of range (%d)", index, len(l.entries))
	}

	// Do not allow previous indices to be committed again.

	// This could happens, since the guarantee is that the new leader has up-to-dated
	// log entires rather than has most up-to-dated committed index

	// For example, Leader 1 send log 80 to follower 2 and follower 3
	// follower 2 and follow 3 all got the new entries and reply
	// leader 1 committed entry 80 and send reply to follower 2 and follower3
	// follower 2 receive the new committed index and update committed index to 80
	// leader 1 fail to send the committed index to follower 3
	// follower 3 promote to leader (server 1 and server 2 will vote, since leader 3
	// has up-to-dated the entries)
	// when new leader 3 send heartbeat with committed index = 0 to follower 2,
	// follower 2 should reply success and let leader 3 update the committed index to 80

	if index < l.commitIndex {
		return nil
	}

	// Find all entries whose index is between the previous index and the current index.
	for i := l.commitIndex + 1; i <= index; i++ {
		entryIndex := i - 1 - l.startIndex
		entry := l.entries[entryIndex]

		// Write to storage.
		if err := entry.Encode(l.file); err != nil {
			return err
		}

		// Update commit index.
		l.commitIndex = entry.Index

		// Apply the changes to the state machine and store the error code.
		entry.result, l.errors[entryIndex] = l.ApplyFunc(entry.Command)

	}
	return nil
}

//--------------------------------------
// Truncation
//--------------------------------------

// Truncates the log to the given index and term. This only works if the log
// at the index has not been committed.
func (l *Log) Truncate(index uint64, term uint64) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	debugln("[Truncate] truncate to ", index)
	// Do not allow committed entries to be truncated.
	if index < l.CommitIndex() {
		debugln("[Truncate] error 1")
		return fmt.Errorf("raft.Log: Index is already committed (%v): (IDX=%v, TERM=%v)", l.CommitIndex(), index, term)
	}

	// Do not truncate past end of entries.
	if index > l.startIndex+uint64(len(l.entries)) {
		debugln("[Truncate] error 2")
		return fmt.Errorf("raft.Log: Entry index does not exist (MAX=%v): (IDX=%v, TERM=%v)", len(l.entries), index, term)
	}

	// If we're truncating everything then just clear the entries.
	if index == l.startIndex {
		l.entries = []*LogEntry{}
	} else {
		// Do not truncate if the entry at index does not have the matching term.
		entry := l.entries[index-l.startIndex-1]
		if len(l.entries) > 0 && entry.Term != term {
			debugln("[Truncate] error 3")
			return fmt.Errorf("raft.Log: Entry at index does not have matching term (%v): (IDX=%v, TERM=%v)", entry.Term, index, term)
		}

		// Otherwise truncate up to the desired entry.
		if index < l.startIndex+uint64(len(l.entries)) {
			debugln("[Truncate] truncate to ", index)
			l.entries = l.entries[0 : index-l.startIndex]
		}
	}

	return nil
}

//--------------------------------------
// Append
//--------------------------------------

// Appends a series of entries to the log. These entries are not written to
// disk until SetCommitIndex() is called.
func (l *Log) AppendEntries(entries []*LogEntry) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	// Append each entry but exit if we hit an error.
	for _, entry := range entries {
		if err := l.appendEntry(entry); err != nil {
			return err
		}
	}

	return nil
}

// Appends a single entry to the log.
func (l *Log) AppendEntry(entry *LogEntry) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return l.appendEntry(entry)
}

// Writes a single log entry to the end of the log. This function does not
// obtain a lock and should only be used internally. Use AppendEntries() and
// AppendEntry() to use it externally.
func (l *Log) appendEntry(entry *LogEntry) error {
	if l.file == nil {
		return errors.New("raft.Log: Log is not open")
	}

	// Make sure the term and index are greater than the previous.
	if len(l.entries) > 0 {
		lastEntry := l.entries[len(l.entries)-1]
		if entry.Term < lastEntry.Term {
			return fmt.Errorf("raft.Log: Cannot append entry with earlier term (%x:%x <= %x:%x)", entry.Term, entry.Index, lastEntry.Term, lastEntry.Index)
		} else if entry.Term == lastEntry.Term && entry.Index <= lastEntry.Index {
			return fmt.Errorf("raft.Log: Cannot append entry with earlier index in the same term (%x:%x <= %x:%x)", entry.Term, entry.Index, lastEntry.Term, lastEntry.Index)
		}
	}

	// Append to entries list if stored on disk.
	l.entries = append(l.entries, entry)
	l.errors = append(l.errors, nil)

	return nil
}

//--------------------------------------
// Log compaction
//--------------------------------------

// compaction the log before index
func (l *Log) Compact(index uint64, term uint64) error {
	var entries []*LogEntry

	l.mutex.Lock()
	defer l.mutex.Unlock()

	// nothing to compaction
	// the index may be greater than the current index if
	// we just recovery from on snapshot
	if index >= l.internalCurrentIndex() {
		entries = make([]*LogEntry, 0)
	} else {

		// get all log entries after index
		entries = l.entries[index-l.startIndex:]
	}

	// create a new log file and add all the entries
	file, err := os.OpenFile(l.path+".new", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		err = entry.Encode(file)
		if err != nil {
			return err
		}
	}
	// close the current log file
	l.file.Close()

	// remove the current log file to .bak
	err = os.Remove(l.path)
	if err != nil {
		return err
	}

	// rename the new log file
	err = os.Rename(l.path+".new", l.path)
	if err != nil {
		return err
	}
	l.file = file

	// compaction the in memory log
	l.entries = entries
	l.startIndex = index
	l.startTerm = term
	return nil
}

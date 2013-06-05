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
	ApplyFunc   func(Command) error
	file        *os.File
	path		string
	entries     []*LogEntry
	commitIndex uint64
	mutex       sync.Mutex
	startIndex  uint64 // the index before the first entry in the Log entries
	startTerm	uint64
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
	return (len(l.entries) == 0)
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

			// Apply the command.
			if err = l.ApplyFunc(entry.Command); err != nil {
				file.Close()
				return err
			}

			// Append entry.
			l.entries = append(l.entries, entry)

			l.commitIndex = entry.Index
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
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if index <= l.startIndex || index > (l.startIndex + uint64(len(l.entries))) {
		return false
	}
	return (l.entries[index-1].Term == term)
}

// Retrieves a list of entries after a given index. This function also returns
// the term of the index provided.
func (l *Log) GetEntriesAfter(index uint64) ([]*LogEntry, uint64) {
	// l.mutex.Lock()
	// defer l.mutex.Unlock()

	// Return an error if the index doesn't exist.
	if index > (uint64(len(l.entries)) + l.startIndex) {
		panic(fmt.Sprintf("raft.Log: Index is beyond end of log: %v", index))
	}
	fmt.Println("getEA", index, l.startIndex)
	// If we're going from the beginning of the log then return the whole log.
	if index == l.startIndex {
		fmt.Println(len(l.entries))
		if len(l.entries) > 0 {
			fmt.Println(l.entries[0].Index)
		}
		return l.entries, l.startTerm
	}
	// Determine the term at the given entry and return a subslice.
	term := l.entries[index - 1 - l.startIndex].Term
	return l.entries[index - l.startIndex:], term
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
		fmt.Println("commitinfo: zero")
		return 0, 0
	}

	// just after snapshot
	if l.commitIndex == l.startIndex {
		fmt.Println("commitinfo: ", l.startIndex, " ", l.startTerm)
		return l.startIndex, l.startTerm
	}
	fmt.Println(l.entries)
	fmt.Println(l.commitIndex, " ", l.startIndex)
	// Return the last index & term from the last committed entry.
	lastCommitEntry := l.entries[l.commitIndex - 1 - l.startIndex]
	return lastCommitEntry.Index, lastCommitEntry.Term
}


func (l *Log) UpdateCommitIndex(index uint64) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.commitIndex = index

}

// Updates the commit index and writes entries after that index to the stable storage.
func (l *Log) SetCommitIndex(index uint64) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	// Panic if we don't have any way to apply commands.
	if l.ApplyFunc == nil {
		panic("raft.Log: Apply function not set")
	}

	// Do not allow previous indices to be committed again.
	if index < l.commitIndex {
		return fmt.Errorf("raft.Log: Commit index (%d) ahead of requested commit index (%d)", l.commitIndex, index)
	}
	if index > l.startIndex + uint64(len(l.entries)) {
		return fmt.Errorf("raft.Log: Commit index (%d) out of range (%d)", index, len(l.entries))
	}

	// Find all entries whose index is between the previous index and the current index.
	for i := l.commitIndex + 1; i <= index; i++ {
		entry := l.entries[i - 1 - l.startIndex]

		// Apply the changes to the state machine.
		if err := l.ApplyFunc(entry.Command); err != nil {
			return err
		}

		// Write to storage.
		if err := entry.Encode(l.file); err != nil {
			return err
		}

		// Update commit index.
		l.commitIndex = entry.Index
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

	// Do not allow committed entries to be truncated.
	if index < l.CommitIndex() {
		fmt.Printf("raft.Log: Index is already committed (%v): (IDX=%v, TERM=%v)\n", l.CommitIndex(), index, term)
		return fmt.Errorf("raft.Log: Index is already committed (%v): (IDX=%v, TERM=%v)", l.CommitIndex(), index, term)
	}

	// Do not truncate past end of entries.
	if index > l.startIndex + uint64(len(l.entries)) {
		fmt.Printf("raft.Log: Entry index does not exist (MAX=%v): (IDX=%v, TERM=%v)\n", len(l.entries), index, term)
		return fmt.Errorf("raft.Log: Entry index does not exist (MAX=%v): (IDX=%v, TERM=%v)", len(l.entries), index, term)
	}

	// If we're truncating everything then just clear the entries.
	if index == l.startIndex {
		l.entries = []*LogEntry{}
	} else {
		// Do not truncate if the entry at index does not have the matching term.
		entry := l.entries[index - l.startIndex - 1]
		if len(l.entries) > 0 && entry.Term != term {
			fmt.Printf("raft.Log: Entry at index does not have matching term (%v): (IDX=%v, TERM=%v)\n", entry.Term, index, term)
			return fmt.Errorf("raft.Log: Entry at index does not have matching term (%v): (IDX=%v, TERM=%v)", entry.Term, index, term)
		}

		// Otherwise truncate up to the desired entry.
		if index < l.startIndex + uint64(len(l.entries)) {
			l.entries = l.entries[0:index - l.startIndex]
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
			fmt.Printf("raft.Log: Cannot append entry with earlier index in the same term (%x:%x <= %x:%x)", entry.Term, entry.Index, lastEntry.Term, lastEntry.Index)
			return fmt.Errorf("raft.Log: Cannot append entry with earlier index in the same term (%x:%x <= %x:%x)", entry.Term, entry.Index, lastEntry.Term, lastEntry.Index)
		}
	}

	// Append to entries list if stored on disk.
	l.entries = append(l.entries, entry)

	return nil
}



//--------------------------------------
// Log compaction
//--------------------------------------

func (l *Log) Compaction(index uint64, term uint64) error {
	var entries []*LogEntry

	l.mutex.Lock()
	defer l.mutex.Unlock()
	fmt.Println("Compaction: ", index, " ", l.internalCurrentIndex(), " ", l.startIndex)
	// recovery from a newer snapshot
	if index >= l.internalCurrentIndex() {
		entries = make([]*LogEntry, 0)
	} else {

		// get all log entries after index
		entries = l.entries[index - l.startIndex:]
	}
	// create a new log file and add all the entries
	file, err := os.OpenFile(l.path + ".new", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
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
	os.Rename(l.path, l.path + "." + string(l.commitIndex))
	// rename the new log file
	os.Rename(l.path + ".new", l.path)
	l.file = file

	// compaction the in memory log
	l.entries = entries
	l.startIndex = index
	l.startTerm = term
	fmt.Println("Compaction: ", len(l.entries))
	return nil
}

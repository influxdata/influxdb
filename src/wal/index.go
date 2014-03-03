package wal

import (
	logger "code.google.com/p/log4go"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
)

type indexEntry struct {
	FirstRequestNumber uint32 // first request number in the block
	LastRequestNumber  uint32 // number of requests in the block
	FirstOffset        int64  // the offset of the first request
	LastOffset         int64  // the offset of the last request
}

func (self *indexEntry) NumberOfRequests() int {
	return int(self.LastRequestNumber-self.FirstRequestNumber) + 1
}

type index struct {
	f       *os.File
	Entries []*indexEntry
}

func newIndex(path string) (*index, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if stat.Size() == 0 {
		// append the version, which is 1 right now
		fmt.Fprintf(f, "%d\n", 1)
	}
	if _, err = f.Seek(0, os.SEEK_SET); err != nil {
		return nil, err
	}
	content, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}

	lines := strings.Split(string(content), "\n")
	entries := make([]*indexEntry, 0, len(lines))
	// skip the first line, that's the version
	for _, line := range lines[1:] {
		if len(line) == 0 {
			break
		}

		fields := strings.Split(line, ",")
		firstRequestNumber, _ := strconv.ParseInt(fields[0], 10, 64)
		firstOffset, _ := strconv.ParseInt(fields[1], 10, 64)
		lastRequestNumber, _ := strconv.ParseInt(fields[2], 10, 64)
		lastOffset, _ := strconv.ParseInt(fields[3], 10, 64)
		entries = append(entries, &indexEntry{
			uint32(firstRequestNumber),
			uint32(lastRequestNumber),
			firstOffset,
			lastOffset,
		})
	}
	return &index{f, entries}, nil
}

func (self *index) addEntry(firstRequestNumber, lastRequestNumber uint32, firstOffset, lastOffset int64) {
	if firstRequestNumber > lastRequestNumber {
		panic(fmt.Errorf("invalid index entry: [%d,%d,%d,%d]", firstRequestNumber, lastRequestNumber, firstOffset, lastOffset))
	}

	logger.Debug("Adding index entry [%d,%d,%d,%d]", firstRequestNumber, lastRequestNumber, firstOffset, lastOffset)
	fmt.Fprintf(self.f, "%d,%d,%d,%d\n", firstRequestNumber, firstOffset, lastRequestNumber, lastOffset)
	entry := &indexEntry{
		FirstRequestNumber: firstRequestNumber,
		FirstOffset:        firstOffset,
		LastRequestNumber:  lastRequestNumber,
		LastOffset:         lastOffset,
	}
	self.Entries = append(self.Entries, entry)
}

func (self *index) syncFile() error {
	return self.f.Sync()
}

func (self *index) close() error {
	return self.f.Close()
}

func (self *index) delete() error {
	return os.Remove(self.f.Name())
}

func (self *index) getLastRequestInfo() (uint32, int64) {
	if len(self.Entries) == 0 {
		return 0, 0
	}
	lastEntry := self.Entries[len(self.Entries)-1]
	return lastEntry.LastRequestNumber, lastEntry.LastOffset
}

func (self *index) getLastOffset() int64 {
	if len(self.Entries) == 0 {
		return 0
	}

	entry := self.Entries[len(self.Entries)-1]
	return entry.LastOffset
}

func (self *index) getLength() int {
	firstRequest, _ := self.getFirstRequestInfo()
	lastRequest, _ := self.getLastRequestInfo()
	return int(lastRequest - firstRequest)
}

func (self *index) getFirstRequestInfo() (uint32, int64) {
	if len(self.Entries) == 0 {
		return 0, 0
	}
	firstEntry := self.Entries[0]
	return firstEntry.FirstRequestNumber, firstEntry.FirstOffset
}

func (self *index) blockSearch(requestNumber uint32) func(int) bool {
	return func(i int) bool {
		// The returned function must satisfy `f(i) => f(i+1)`, meaning if
		// for index i f returns true, then it must return true for every
		// index greater than i. sort.Search will return the smallest
		// index satisfying f
		return requestNumber <= self.Entries[i].LastRequestNumber
	}
}

func (self *index) requestOffset(requestNumber uint32) int64 {
	numberOfEntries := len(self.Entries)

	if numberOfEntries == 0 {
		return -1
	}

	firstEntry := self.Entries[0]
	if requestNumber < firstEntry.FirstRequestNumber {
		return -1
	}

	lastEntry := self.Entries[numberOfEntries-1]
	if requestNumber > lastEntry.LastRequestNumber {
		return -1
	}

	index := sort.Search(numberOfEntries, self.blockSearch(requestNumber))
	return self.Entries[index].FirstOffset
}

func (self *index) requestOrLastOffset(requestNumber uint32) int64 {
	numberOfEntries := len(self.Entries)
	if numberOfEntries == 0 {
		return 0
	}

	if requestNumber > self.Entries[numberOfEntries-1].LastRequestNumber {
		return self.Entries[numberOfEntries-1].LastOffset
	}
	return self.requestOffset(requestNumber)
}

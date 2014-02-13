package wal

import (
	"sort"
)

type indexEntry struct {
	startRequestNumber uint32 // first request number in the block
	size               uint32 // number of requests in the block
	startOffset        uint64 // the offset of the first request
}

type index struct {
	entries []*indexEntry
}

func (self *index) addEntry(entry *indexEntry) {
	self.entries = append(self.entries, entry)
}

func (self *index) findOffsetBlock(requestNumber uint32) func(int) bool {
	return func(i int) bool {
		// The returned function must satisfy `f(i) => f(i+1)`, meaning if
		// for index i f returns true, then it must return true for every
		// index greater than i. sort.Search will return the smallest
		// index satisfying f
		if self.entries[i].startRequestNumber > requestNumber {
			return true
		}

		return false
	}
}

func (self *index) requestOffset(requestNumber uint32) uint64 {
	numberOfEntries := len(self.entries)
	index := sort.Search(numberOfEntries, self.findOffsetBlock(requestNumber))
	if index != numberOfEntries {
		return self.entries[index-1].startOffset
	}

	if requestNumber >= self.entries[0].startRequestNumber {
		return self.entries[0].startOffset
	}

	lastEntry := self.entries[len(self.entries)-1]
	lastRequestNumber := lastEntry.startRequestNumber + lastEntry.size
	if requestNumber > lastRequestNumber {
		return lastEntry.startOffset
	}

	return 0
}

package wal

import (
	logger "code.google.com/p/log4go"
	"sort"
)

type indexEntry struct {
	StartRequestNumber uint32 // first request number in the block
	Size               uint32 // number of requests in the block
	StartOffset        uint64 // the offset of the first request
}

type index struct {
	Entries       []*indexEntry
	CurrentOffset uint64
}

func (self *index) addEntry(startRequestNumber, size uint32, currentOffset uint64) {
	entry := &indexEntry{
		StartRequestNumber: startRequestNumber,
		Size:               size,
		StartOffset:        self.CurrentOffset,
	}
	self.Entries = append(self.Entries, entry)
	self.CurrentOffset = currentOffset
}

func (self *index) findOffsetBlock(order RequestNumberOrder, requestNumber uint32) func(int) bool {
	return func(i int) bool {
		// The returned function must satisfy `f(i) => f(i+1)`, meaning if
		// for index i f returns true, then it must return true for every
		// index greater than i. sort.Search will return the smallest
		// index satisfying f
		return order.isAfter(self.Entries[i].StartRequestNumber, requestNumber)
	}
}

func (self *index) requestOffset(order RequestNumberOrder, requestNumber uint32) uint64 {
	numberOfEntries := len(self.Entries)
	if numberOfEntries == 0 {
		logger.Info("no index entries, assuming beginning of the file")
		return 0
	}

	firstEntry := self.Entries[0]
	if order.isBeforeOrEqual(requestNumber, firstEntry.StartRequestNumber) {
		return firstEntry.StartOffset
	}

	lastEntry := self.Entries[numberOfEntries-1]
	if order.isAfterOrEqual(requestNumber, lastEntry.StartRequestNumber) {
		return lastEntry.StartOffset
	}

	index := sort.Search(numberOfEntries, self.findOffsetBlock(order, requestNumber))
	return self.Entries[index-1].StartOffset
}

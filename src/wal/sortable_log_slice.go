package wal

type sortableLogSlice struct {
	logFiles []*log
	order    RequestNumberOrder
}

func (self sortableLogSlice) Len() int {
	return len(self.logFiles)
}

func (self sortableLogSlice) Less(i, j int) bool {
	left := self.logFiles[i].firstRequestNumber()
	right := self.logFiles[j].firstRequestNumber()
	return self.order.isBefore(left, right)
}

func (self sortableLogSlice) Swap(i, j int) {
	self.logFiles[i], self.logFiles[j] = self.logFiles[j], self.logFiles[i]
}

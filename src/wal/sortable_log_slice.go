package wal

type sortableLogSlice struct {
	logFiles []*log
	logIndex []*index
}

func (self sortableLogSlice) Len() int {
	return len(self.logFiles)
}

func (self sortableLogSlice) Less(i, j int) bool {
	left := self.logFiles[i].suffix()
	right := self.logFiles[j].suffix()
	return left < right
}

func (self sortableLogSlice) Swap(i, j int) {
	self.logFiles[i], self.logFiles[j] = self.logFiles[j], self.logFiles[i]
	self.logIndex[i], self.logIndex[j] = self.logIndex[j], self.logIndex[i]
}

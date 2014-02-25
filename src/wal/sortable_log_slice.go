package wal

type sortableLogSlice []*log

func (self sortableLogSlice) Len() int {
	return len(self)
}

func (self sortableLogSlice) Less(i, j int) bool {
	return self[i].firstRequestNumber() < self[j].firstRequestNumber()
}

func (self sortableLogSlice) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}

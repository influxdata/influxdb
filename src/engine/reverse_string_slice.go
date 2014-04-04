package engine

type ReverseStringSlice []string

func (self ReverseStringSlice) Less(i, j int) bool {
	return j < i
}

func (self ReverseStringSlice) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}

func (self ReverseStringSlice) Len() int {
	return len(self)
}

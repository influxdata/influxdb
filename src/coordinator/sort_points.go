package coordinator

import (
	"protocol"
)

type SortPointsByTimeDescending []*protocol.Point

func (self SortPointsByTimeDescending) Len() int {
	return len(self)
}
func (self SortPointsByTimeDescending) Less(i, j int) bool {
	return self[i].GetTimestamp() > self[j].GetTimestamp()
}
func (self SortPointsByTimeDescending) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}

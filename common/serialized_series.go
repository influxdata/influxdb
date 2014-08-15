package common

import "sort"

type SerializedSeries struct {
	Name    string          `json:"name"`
	Columns []string        `json:"columns"`
	Points  [][]interface{} `json:"points"`
}

func SortSerializedSeries(s []*SerializedSeries) {
	sort.Sort(BySerializedSeriesNameAsc{s})
}

func (self *SerializedSeries) GetName() string {
	return self.Name
}

func (self *SerializedSeries) GetColumns() []string {
	return self.Columns
}

func (self *SerializedSeries) GetPoints() [][]interface{} {
	return self.Points
}

type SerializedSeriesCollection []*SerializedSeries

func (s SerializedSeriesCollection) Len() int      { return len(s) }
func (s SerializedSeriesCollection) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

type BySerializedSeriesNameAsc struct{ SerializedSeriesCollection }

func (s BySerializedSeriesNameAsc) Less(i, j int) bool {
	if s.SerializedSeriesCollection[i] != nil && s.SerializedSeriesCollection[j] != nil {
		return s.SerializedSeriesCollection[i].Name < s.SerializedSeriesCollection[j].Name
	}
	return false
}

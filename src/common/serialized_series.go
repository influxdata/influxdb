package common

type SerializedSeries struct {
	Name    string          `json:"name"`
	Columns []string        `json:"columns"`
	Points  [][]interface{} `json:"points"`
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

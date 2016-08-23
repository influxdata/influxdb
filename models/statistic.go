package models

type Statistic struct {
	Name   string                 `json:"name"`
	Tags   map[string]string      `json:"tags"`
	Values map[string]interface{} `json:"values"`
}

func NewStatistic(name string) Statistic {
	return Statistic{
		Name: name,
		Tags: make(map[string]string),
		Values: make(map[string]interface{}),
	}
}

package models

type Statistic struct {
	Name   string                 `json:"name"`
	Tags   Tags                   `json:"tags"`
	Values map[string]interface{} `json:"values"`
}

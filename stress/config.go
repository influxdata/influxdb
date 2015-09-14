package runner

import (
	"fmt"

	"github.com/BurntSushi/toml"
	"github.com/influxdb/influxdb/client"
)

type tag struct {
	Key    string   `toml:"key"`
	Values []string `toml:"values"`
}

type field struct {
	Key  string `toml:"key"`
	Type string `toml:"type"`
}

type series struct {
	PointCount               int     `toml:"point_count"`
	Measurement              string  `toml:"measurement"`
	GenericTagsetCardinality int     `toml:"generic_tagset_cardinality"`
	Tags                     []tag   `toml:"series.tag"`
	Fields                   []field `toml:"series.field"`
}
type seriesIter struct {
	s     *series
	count int
}

// iterates through the point
func (s *series) Iter() *seriesIter {
	return &seriesIter{s: s, count: 0}
}

// makes one point for each of its series
// iterates through all of the series
func (iter *seriesIter) Next() (client.Point, bool) {
	if iter.count > iter.s.GenericTagsetCardinality {
		return client.Point{}, false
	}
	p := client.Point{
		Measurement: iter.s.Measurement,
		Tags:        map[string]string{"region": "uswest", "host": fmt.Sprintf("host-%d", iter.count)},
		Fields:      map[string]interface{}{"value": 1.0},
	}
	iter.count++
	return p, true
}

type write struct {
	Concurrency   int    `toml:"concurrency"`
	BatchSize     int    `toml:"batch_size"`
	BatchInterval string `toml:"batch_interval"`
	Database      string `toml:"database"`
	ResetDatabase bool   `toml:"reset_database"`
	StartingPoint string `toml:"starting_time"`
	Address       string `toml:"address"`
	Precision     string `toml:"precision"`
}

type query struct {
	Concurrency int    `toml:"concurrency"`
	Measurement string `toml:"measurement"`
	TagKey      string `toml:"tag_key"`
	TimeFrame   string `toml:"time_frame"`
	Statement   string `toml:"statement"`
}

type StressTest struct {
	Write   write    `toml:"write"`
	Series  []series `toml:"series"`
	Queries []query  `toml:"query"`
}

func DecodeFile(s string) (*StressTest, error) {
	t := &StressTest{}

	if _, err := toml.DecodeFile(s, t); err != nil {
		return nil, err
	}

	return t, nil
}

package main

import (
	"fmt"
	"time"

	"github.com/BurntSushi/toml"
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

type write struct {
	Concurrency   int           `toml:"concurrency"`
	BatchSize     int           `toml:"batch_size"`
	BatchInterval time.Duration `toml:"batch_interval"`
	Database      string        `toml:"database"`
	ResetDatabase bool          `toml:"reset_database"`
	StartingPoint time.Duration `toml:"starting_time"`
}

type query struct {
	Concurrency int           `toml:"concurrency"`
	Measurement string        `toml:"measurement"`
	TagKey      string        `toml:"tag_key"`
	TimeFrame   time.Duration `toml:"time_frame"`
	Statement   string        `toml:"statement"`
}

type Test struct {
	Write   write    `toml:"write"`
	Series  []series `toml:"series"`
	Queries []query  `toml:"query"`
}

func main() {
	t := &Test{}
	if _, err := toml.DecodeFile("example.toml", t); err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(t)

}

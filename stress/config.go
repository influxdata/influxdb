package runner

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/influxdb/influxdb/client"
)

// tag is a struct that contains data
// about a tag for in a series
type tag struct {
	Key   string `toml:"key"`
	Value string `toml:"value"`
}

// tag is a struct that contains data
// about a field for in a series
type field struct {
	Key  string `toml:"key"`
	Type string `toml:"type"`
}

// series is a struct that contains data
// about the series that will be written
// during a stress test
type series struct {
	PointCount  int     `toml:"point_count"`
	Measurement string  `toml:"measurement"`
	SeriesCount int     `toml:"series_count"`
	TagCount    int     `toml:"tag_count"`
	Tags        []tag   `toml:"tag"`
	Fields      []field `toml:"field"`
}

// write is a struct that contains the business
// logic for the stress test. e.g. where the
// influxdb instance is running, what database
// should points be written into
type write struct {
	Concurrency   int    `toml:"concurrency"`
	BatchSize     int    `toml:"batch_size"`
	BatchInterval string `toml:"batch_interval"`
	Database      string `toml:"database"`
	ResetDatabase bool   `toml:"reset_database"`
	StartingPoint int    `toml:"starting_point"`
	Address       string `toml:"address"`
	Precision     string `toml:"precision"`
}

// query is a struct that contains the logic for
// a query that will be ran on during the stress
// test
type query struct {
	Concurrency int    `toml:"concurrency"`
	Measurement string `toml:"measurement"`
	TagKey      string `toml:"tag_key"`
	TimeFrame   string `toml:"time_frame"`
	Statement   string `toml:"statement"`
}

// Config is a struct that is passed into the `Run()` function.
type Config struct {
	Write   write    `toml:"write"`
	Series  []series `toml:"series"`
	Queries []query  `toml:"query"`
}

// NewSeries, takes a measurement, and point count,
// and a series count and returns a series
func NewSeries(m string, p int, sc int) series {
	s := series{
		PointCount:  p,
		SeriesCount: sc,
		Measurement: m,
		Tags: []tag{
			tag{
				Key:   "host",
				Value: "server",
			},
		},
		Fields: []field{
			field{
				Key: "value",
			},
		},
	}

	return s
}

// NewConfig returns a pointer to a config
// with some default parameters set
func NewConfig() *Config {

	w := write{
		Concurrency:   10,
		BatchSize:     5000,
		BatchInterval: "0s",
		Database:      "stress",
		ResetDatabase: true,
		Address:       "localhost:8086",
		Precision:     "n",
	}

	c := &Config{
		Write: w,
	}

	return c
}

// DecodeFile takes a file path for a toml config file
// and returns a pointer to a Config Struct.
func DecodeFile(s string) (*Config, error) {
	t := &Config{}

	// Decode the toml file
	if _, err := toml.DecodeFile(s, t); err != nil {
		return nil, err
	}

	// Initialize Config struct
	// NOTE: Not happy with the implementation
	// but it will do for now
	for j, srs := range t.Series {
		for i := 0; i < srs.TagCount; i++ {

			tag := tag{
				Key:   fmt.Sprintf("tag-key-%d", i),
				Value: "tag-value",
			}

			srs.Tags = append(srs.Tags, tag)
			fmt.Println(srs)
		}

		t.Series[j] = srs
	}

	return t, nil
}

// seriesIter is a struct that contains a
// series and a count, where count is the
// number of points that have been written
// for the series `s`
type seriesIter struct {
	s         *series
	count     int
	timestamp time.Time
}

func (s *series) writeInterval(weeks int, i int) time.Time {
	st := time.Duration(weeks) * 7 * 24 * time.Hour
	w := st - (st/time.Duration(s.PointCount))*time.Duration(i)
	return time.Now().Add(-1 * w)
}

// Iter returns a pointer to a seriesIter
func (s *series) Iter(weeks int, i int) *seriesIter {

	return &seriesIter{s: s, count: -1, timestamp: s.writeInterval(weeks, i)}
}

// newTagMap returns a tagset
func (s *series) newTagMap(i int) map[string]string {
	m := map[string]string{}

	for _, tag := range s.Tags {
		m[tag.Key] = fmt.Sprintf("%s-%d", tag.Value, i)
	}

	return m
}

// newFieldMap returns a new field set for
// a given series
func (s *series) newFieldMap() map[string]interface{} {
	m := map[string]interface{}{}

	for _, field := range s.Fields {
		switch field.Type {
		case "float64":
			m[field.Key] = float64(rand.Intn(1000))
		case "int":
			m[field.Key] = rand.Intn(1000)
		case "bool":
			b := rand.Intn(2) == 1
			m[field.Key] = b
		default:
			m[field.Key] = float64(rand.Intn(1000))
		}

	}

	return m
}

// Next returns a new point for a series.
// Currently, there is an off by one bug here.
func (iter *seriesIter) Next() (client.Point, bool) {
	iter.count++
	p := client.Point{
		Measurement: iter.s.Measurement,
		Tags:        iter.s.newTagMap(iter.count),
		Fields:      iter.s.newFieldMap(),
		Time:        iter.timestamp,
	}
	b := iter.count < iter.s.SeriesCount
	return p, b
}

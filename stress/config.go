package runner

import (
	"bytes"
	"fmt"
	"math/rand"
	"time"

	"github.com/BurntSushi/toml"
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
	Jitter        int    `toml:"jitter"`
}

// query is a struct that contains the logic for
// a query that will be ran on during the stress
// test
type query struct {
	Enabled     bool     `toml:"enabled"`
	Concurrency int      `toml:"concurrency"`
	Aggregates  []string `toml:"aggregates"`
	Fields      []string `toml:"fields"`
}

// measurementQuery is a struct that contains
// the logic that runs a query against a measurement
// over a time period that is specified by
// `Offset`
type measurementQuery struct {
	query
	Offset string `toml:"offset"`
}

// seriesQuery is a struct that contains
// the logic that runs a query against a single
// series
type seriesQuery struct {
	query
	Interval string `toml:"interval"`
}

// Config is a struct that is passed into the `Run()` function.
type Config struct {
	Write             write            `toml:"write"`
	Series            []series         `toml:"series"`
	MeasurementQuery  measurementQuery `toml:"measurement_query"`
	SeriesQuery       seriesQuery      `toml:"series_query"`
	ChannelBufferSize int              `toml:"channel_buffer_size"`
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
	precision string
}

// writeInterval returns a timestamp for the current time
// interval
func (s *series) writeInterval(weeks int, i int) time.Time {
	st := time.Duration(weeks) * 7 * 24 * time.Hour
	w := st - (st/time.Duration(s.PointCount))*time.Duration(i)
	return time.Now().Add(-1 * w)
}

// Iter returns a pointer to a seriesIter
func (s *series) Iter(weeks int, i int, p string) *seriesIter {

	return &seriesIter{s: s, count: -1, timestamp: s.writeInterval(weeks, i), precision: p}
}

// Next returns a new point for a series.
// Currently, there is an off by one bug here.
func (iter *seriesIter) Next() ([]byte, bool) {
	var buf bytes.Buffer
	iter.count++

	buf.Write([]byte(fmt.Sprintf("%v,", iter.s.Measurement)))
	buf.Write(iter.s.newTagSet(iter.count))
	buf.Write([]byte(" "))
	buf.Write(iter.s.newFieldSet())
	buf.Write([]byte(" "))

	switch iter.precision {
	case "s":
		buf.Write([]byte(fmt.Sprintf("%v", iter.timestamp.Unix())))
	default:
		buf.Write([]byte(fmt.Sprintf("%v", iter.timestamp.UnixNano())))
	}

	b := iter.count < iter.s.SeriesCount
	byt := buf.Bytes()

	return byt, b
}

// newTagSet returns a byte array representation
// of the tagset for a series
func (s *series) newTagSet(c int) []byte {
	var buf bytes.Buffer
	for _, tag := range s.Tags {
		buf.Write([]byte(fmt.Sprintf("%v=%v-%v,", tag.Key, tag.Value, c)))
	}

	b := buf.Bytes()
	b = b[0 : len(b)-1]

	return b
}

// newFieldSet returns a byte array representation
// of the field-set for a series
func (s *series) newFieldSet() []byte {
	var buf bytes.Buffer

	for _, field := range s.Fields {
		switch field.Type {
		case "float64":
			buf.Write([]byte(fmt.Sprintf("%v=%v,", field.Key, rand.Intn(1000))))
		case "int":
			buf.Write([]byte(fmt.Sprintf("%v=%vi,", field.Key, rand.Intn(1000))))
		case "bool":
			b := rand.Intn(2) == 1
			buf.Write([]byte(fmt.Sprintf("%v=%v,", field.Key, b)))
		default:
			buf.Write([]byte(fmt.Sprintf("%v=%v,", field.Key, rand.Intn(1000))))
		}
	}

	b := buf.Bytes()
	b = b[0 : len(b)-1]

	return b
}

package graphite

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/influxdb/influxdb"
)

var (
	// ErrBindAddressRequired is returned when starting the Server
	// without a TCP or UDP listening address.
	ErrBindAddressRequired = errors.New("bind address required")

	// ErrServerClosed return when closing an already closed graphite server.
	ErrServerClosed = errors.New("server already closed")

	// ErrDatabaseNotSpecified retuned when no database was specified in the config file
	ErrDatabaseNotSpecified = errors.New("database was not specified in config")

	// ErrServerNotSpecified returned when Server is not specified.
	ErrServerNotSpecified = errors.New("server not present")
)

type ProcessorSink interface {
	WriteSeries(database, retentionPolicy, name string, tags map[string]string, timestamp time.Time, values map[string]interface{}) error
	DefaultRetentionPolicy(database string) (*influxdb.RetentionPolicy, error)
}

// Processor performs processing of Graphite data, and writes it to a sink.
type Processor struct {
	// Sink is the destination for processed Graphite data.
	sink dataSink

	// Database is the name of the database to insert data into.
	Database string
	// NamePosition is the position of name to be parsed from metric_path
	NamePosition string
	// NameSeparator is separator to parse metric_path with to get name and series
	NameSeparator string
}

func NewProcessor(sink *ProcessorSink) *Processor {
	p = Processor{}
	return &p
}

// handleMessage decodes a graphite message from the reader and sends it to the
// committer goroutine.
func (p *processor) handleMessage(r *bufio.Reader) error {
	// Decode graphic metric.
	m, err := DecodeMetric(r, s.NamePosition, s.NameSeparator)
	if err != nil {
		return err
	}

	// Convert metric to a field value.
	var values = make(map[string]interface{})
	values[m.Name] = m.Value

	retentionPolicy, err := s.sink.DefaultRetentionPolicy(s.Database)

	if err != nil {
		return fmt.Errorf("error looking up default database retention policy: %s", err)
	}

	if err := s.sink.WriteSeries(
		s.Database,
		retentionPolicy.Name,
		m.Name,
		m.Tags,
		m.Timestamp,
		values,
	); err != nil {
		return fmt.Errorf("write series data: %s", err)
	}
	return nil
}

type Metric struct {
	Name      string
	Tags      map[string]string
	Value     interface{}
	Timestamp time.Time
}

// returns err == io.EOF when we hit EOF without any further data
func DecodeMetric(r *bufio.Reader, position, separator string) (*Metric, error) {
	// Read up to the next newline.
	buf, err := r.ReadBytes('\n')
	if err != nil && err != io.EOF {
		// it's possible to get EOF but also data
		return nil, fmt.Errorf("connection closed uncleanly/broken: %s\n", err.Error())
	}
	// Trim the buffer, even though there should be no padding
	str := strings.TrimSpace(string(buf))
	// Remove line return
	str = strings.TrimSuffix(str, `\n`)
	if str == "" {
		return nil, err
	}
	// Break into 3 fields (name, value, timestamp).
	fields := strings.Fields(str)
	if len(fields) != 3 {
		return nil, fmt.Errorf("received %q which doesn't have three fields", str)
	}

	m := new(Metric)
	// decode the name and tags
	name, tags, err := DecodeNameAndTags(fields[0], position, separator)
	if err != nil {
		return nil, err
	}
	m.Name = name
	m.Tags = tags

	// Parse value.
	v, err := strconv.ParseFloat(fields[1], 64)
	if err != nil {
		return nil, err
	}

	// Determine if value is a float or an int.
	if i := int64(v); float64(i) == v {
		m.Value = int64(v)
	} else {
		m.Value = v
	}

	// Parse timestamp.
	unixTime, err := strconv.ParseInt(fields[2], 10, 64)
	if err != nil {
		return nil, err
	}

	m.Timestamp = time.Unix(0, unixTime*int64(time.Millisecond))

	return m, nil
}

func DecodeNameAndTags(field, position, separator string) (string, map[string]string, error) {
	var (
		name string
		tags = make(map[string]string)
	)

	if separator == "" {
		separator = "."
	}

	// decode the name and tags
	values := strings.Split(field, separator)
	if len(values)%2 != 1 {
		// There should always be an odd number of fields to map a metric name and tags
		// ex: region.us-west.hostname.server01.cpu -> tags -> region: us-west, hostname: server01, metric name -> cpu
		return name, tags, fmt.Errorf("received %q which doesn't conform to format of key.value.key.value.metric or metric", field)
	}

	np := strings.ToLower(strings.TrimSpace(position))
	switch np {
	case "last":
		name = values[len(values)-1]
		values = values[0 : len(values)-1]
	case "first":
		name = values[0]
		values = values[1:len(values)]
	default:
		name = values[0]
		values = values[1:len(values)]
	}
	if name == "" {
		return name, tags, fmt.Errorf("no name specified for metric. %q", field)
	}

	// Grab the pairs and throw them in the map
	for i := 0; i < len(values); i += 2 {
		k := values[i]
		v := values[i+1]
		tags[k] = v
	}

	return name, tags, nil
}

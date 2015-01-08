package graphite

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
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

// SeriesWriter defines the interface for the destination of the data.
type SeriesWriter interface {
	WriteSeries(database, retentionPolicy, name string, tags map[string]string, timestamp time.Time, values map[string]interface{}) error
}

// Parser encapulates a Graphite Parser.
type Parser struct {
	Separator   string
	LastEnabled bool
}

// metric represents a Metric as processed by the Graphite parser.
type Metric struct {
	Name      string
	Tags      map[string]string
	Value     interface{}
	Timestamp time.Time
}

// NewParser returns a GraphiteParser instance.
func NewParser() *Parser {
	p := Parser{}
	return &p
}

// Parse performs Graphite parsing of a single line.
func (p *Parser) Parse(line string) (*Metric, error) {
	// Break into 3 fields (name, value, timestamp).
	fields := strings.Fields(line)
	if len(fields) != 3 {
		return nil, fmt.Errorf("received %q which doesn't have three fields", line)
	}

	m := new(Metric)
	// decode the name and tags
	name, tags, err := p.DecodeNameAndTags(fields[0])
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

// DecodeNameAndTags parses the name and tags of a single field of a Graphite datum.
func (p *Parser) DecodeNameAndTags(field string) (string, map[string]string, error) {
	var (
		name string
		tags = make(map[string]string)
	)

	// decode the name and tags
	values := strings.Split(field, p.Separator)
	if len(values)%2 != 1 {
		// There should always be an odd number of fields to map a metric name and tags
		// ex: region.us-west.hostname.server01.cpu -> tags -> region: us-west, hostname: server01, metric name -> cpu
		return name, tags, fmt.Errorf("received %q which doesn't conform to format of key.value.key.value.metric or metric", field)
	}

	if p.LastEnabled {
		name = values[len(values)-1]
		values = values[0 : len(values)-1]
	} else {
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

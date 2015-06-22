package graphite

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/influxdb/influxdb/tsdb"
)

// Parser encapulates a Graphite Parser.
type Parser struct {
	Separator     string
	FieldNames    []string
	IgnoreUnnamed bool
}

// NewParser returns a GraphiteParser instance.
func NewParser(schema string, separator string, ignore bool) *Parser {
	return &Parser{
		Separator:     separator,
		FieldNames:    strings.Split(schema, separator),
		IgnoreUnnamed: ignore,
	}
}

// Parse performs Graphite parsing of a single line.
func (p *Parser) Parse(line string) (tsdb.Point, error) {
	// Break into 3 fields (name, value, timestamp).
	fields := strings.Fields(line)
	if len(fields) != 3 {
		return nil, fmt.Errorf("received %q which doesn't have three fields", line)
	}

	// decode the name and tags
	name, tags, err := p.DecodeNameAndTags(fields[0])
	if err != nil {
		return nil, err
	}

	// Parse value.
	v, err := strconv.ParseFloat(fields[1], 64)
	if err != nil {
		return nil, fmt.Errorf("field \"%s\" value: %s", fields[0], err)
	}

	fieldValues := make(map[string]interface{})
	fieldValues["value"] = v

	// Parse timestamp.
	unixTime, err := strconv.ParseFloat(fields[2], 64)
	if err != nil {
		return nil, fmt.Errorf("field \"%s\" time: %s", fields[0], err)
	}

	// Check if we have fractional seconds
	timestamp := time.Unix(int64(unixTime), int64((unixTime-math.Floor(unixTime))*float64(time.Second)))

	point := tsdb.NewPoint(name, tags, fieldValues, timestamp)

	return point, nil
}

// DecodeNameAndTags parses the name and tags of a single field of a Graphite datum.
func (p *Parser) DecodeNameAndTags(nameField string) (string, map[string]string, error) {
	var (
		measurement string
		tags        = make(map[string]string)
		minLen      int
	)

	fields := strings.Split(nameField, p.Separator)
	if len(fields) > len(p.FieldNames) {
		if !p.IgnoreUnnamed {
			return measurement, tags, fmt.Errorf("received %q which contains unnamed field", nameField)
		}
		minLen = len(p.FieldNames)
	} else {
		minLen = len(fields)
	}

	// decode the name and tags
	for i := 0; i < minLen; i++ {
		if p.FieldNames[i] == "measurement" {
			measurement = fields[i]
		} else if p.FieldNames[i] == "measurement*" {
			measurement = strings.Join(fields[i:len(fields)], ".")
			break
		} else {
			tags[p.FieldNames[i]] = fields[i]
		}
	}

	if measurement == "" {
		return measurement, tags, fmt.Errorf("no measurement specified for metric. %q", nameField)
	}

	return measurement, tags, nil
}

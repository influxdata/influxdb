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
	matchers []*matcher
}

// NewParser returns a GraphiteParser instance.
func NewParser(template string) (*Parser, error) {
	p := &Parser{}

	matcher, err := newMatcher(template)
	if err != nil {
		return nil, err
	}
	p.matchers = append(p.matchers, matcher)
	return p, nil
}

// Parse performs Graphite parsing of a single line.
func (p *Parser) Parse(line string) (tsdb.Point, error) {
	// Break into 3 fields (name, value, timestamp).
	fields := strings.Fields(line)
	if len(fields) != 3 {
		return nil, fmt.Errorf("received %q which doesn't have three fields", line)
	}

	// decode the name and tags
	name, tags := p.DecodeNameAndTags(fields[0])
	if name == "" {
		return nil, fmt.Errorf("unable to parse measurement name from %s", line)
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
func (p *Parser) DecodeNameAndTags(nameField string) (string, map[string]string) {
	return p.matchers[0].Match(nameField)
}

type matcher struct {
	tags              []string
	measurementPos    int
	greedyMeasurement bool
}

func newMatcher(template string) (*matcher, error) {
	tags := strings.Split(template, ".")
	matcher := &matcher{tags: tags, measurementPos: -1}

	for i, tag := range tags {
		if strings.HasPrefix(tag, "measurement") {
			matcher.measurementPos = i
		}
		if tag == "measurement*" {
			matcher.greedyMeasurement = true
		}
	}

	if matcher.measurementPos == -1 {
		return nil, fmt.Errorf("no measurement specified for template. %q", template)
	}

	return matcher, nil
}

func (m *matcher) Match(line string) (string, map[string]string) {
	fields := strings.Split(line, ".")
	var (
		measurement string
		tags        = make(map[string]string)
	)

	for i, tag := range m.tags {
		if i >= len(fields) {
			continue
		}

		if i == m.measurementPos {
			measurement = fields[i]
			if m.greedyMeasurement {
				measurement = strings.Join(fields[i:len(fields)], ".")
			}
		}

		if tag == "measurement" {
			measurement = fields[i]
		} else if tag == "measurement*" {
			measurement = strings.Join(fields[i:len(fields)], ".")
			break
		} else if tag != "" {
			tags[tag] = fields[i]
		}
	}

	return measurement, tags
}

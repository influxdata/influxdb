package graphite

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/influxdb/influxdb/tsdb"
)

var defaultTemplate *template

func init() {
	var err error
	defaultTemplate, err = newTemplate("measurement*")
	if err != nil {
		panic(err)
	}
}

// Parser encapulates a Graphite Parser.
type Parser struct {
	matcher *matcher
}

// NewParser returns a GraphiteParser instance.
func NewParser(templates []string) (*Parser, error) {
	p := &Parser{
		matcher: &matcher{},
	}

	for _, pattern := range templates {
		template, err := newTemplate(pattern)
		if err != nil {
			return nil, err
		}
		p.matcher.templates = append(p.matcher.templates, template)
	}
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
	return p.matcher.Match(nameField).Apply(nameField)
}

type template struct {
	tags              []string
	measurementPos    int
	greedyMeasurement bool
}

func newTemplate(pattern string) (*template, error) {
	tags := strings.Split(pattern, ".")
	template := &template{tags: tags, measurementPos: -1}

	for i, tag := range tags {
		if strings.HasPrefix(tag, "measurement") {
			template.measurementPos = i
		}
		if tag == "measurement*" {
			template.greedyMeasurement = true
		}
	}

	if template.measurementPos == -1 {
		return nil, fmt.Errorf("no measurement specified for template. %q", pattern)
	}

	return template, nil
}

func (t *template) Apply(line string) (string, map[string]string) {
	fields := strings.Split(line, ".")
	var (
		measurement string
		tags        = make(map[string]string)
	)

	for i, tag := range t.tags {
		if i >= len(fields) {
			continue
		}

		if i == t.measurementPos {
			measurement = fields[i]
			if t.greedyMeasurement {
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

type matcher struct {
	templates []*template
}

func (m *matcher) Match(line string) *template {
	if len(m.templates) == 0 {
		return defaultTemplate
	}
	return m.templates[0]
}

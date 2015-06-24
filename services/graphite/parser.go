package graphite

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/influxdb/influxdb/tsdb"
)

var defaultTemplate *template

func init() {
	var err error
	defaultTemplate, err = NewTemplate("measurement*", nil)
	if err != nil {
		panic(err)
	}
}

// Parser encapulates a Graphite Parser.
type Parser struct {
	matcher *matcher
	tags    tsdb.Tags
}

// NewParser returns a GraphiteParser instance.
func NewParser(templates []string, defaultTags tsdb.Tags) (*Parser, error) {

	matcher := &matcher{}
	matcher.AddDefaultTemplate(defaultTemplate)

	for _, pattern := range templates {

		template := pattern
		filter := ""
		// Format is [filter] <template> [tag1=value1,tag2=value2]
		parts := strings.Fields(pattern)
		if len(parts) >= 2 {
			filter = parts[0]
			template = parts[1]
		}

		tags := tsdb.Tags{}
		if strings.Contains(parts[len(parts)-1], "=") {
			tagStrs := strings.Split(parts[len(parts)-1], ",")
			for _, kv := range tagStrs {
				parts := strings.Split(kv, "=")
				tags[parts[0]] = parts[1]
			}
		}

		tmpl, err := NewTemplate(template, tags)
		if err != nil {
			return nil, err
		}
		matcher.Add(filter, tmpl)
	}
	return &Parser{matcher: matcher, tags: defaultTags}, nil
}

// Parse performs Graphite parsing of a single line.
func (p *Parser) Parse(line string) (tsdb.Point, error) {
	// Break into 3 fields (name, value, timestamp).
	fields := strings.Fields(line)
	if len(fields) != 3 {
		return nil, fmt.Errorf("received %q which doesn't have three fields", line)
	}

	// decode the name and tags
	matcher := p.matcher.Match(fields[0])
	name, tags := matcher.Apply(fields[0])
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

	for k, v := range p.tags {
		if _, ok := tags[k]; !ok {
			tags[k] = v
		}
	}
	point := tsdb.NewPoint(name, tags, fieldValues, timestamp)

	return point, nil
}

type template struct {
	tags              []string
	defaultTags       tsdb.Tags
	measurementPos    int
	greedyMeasurement bool
}

func NewTemplate(pattern string, defaultTags tsdb.Tags) (*template, error) {
	tags := strings.Split(pattern, ".")
	template := &template{tags: tags, measurementPos: -1, defaultTags: defaultTags}

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

	// Set any default tags
	for k, v := range t.defaultTags {
		tags[k] = v
	}

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

type entry struct {
	filter   *filter
	template *template
}

type entries []*entry

// Less returns a boolean indicating whether the filter at position j
// is less than the filter at postion k.  Filters are order by string
// comparison of each component parts.  A wildcard value "*" is never
// less than a non-wildcard values.
//
// For example, the filters:
//		"*.*"
//		"servers.*""
//		"servers.localhost""
//		"*.localhost"
//
// Would be sorted as:
//		"servers.localhost""
//		"servers.*""
//		"*.localhost"
//		"*.*"
func (s *entries) Less(j, k int) bool {
	jParts := (*s)[j].filter.sections
	kParts := (*s)[k].filter.sections

	i := 0
	for {
		if i >= len(jParts) && i >= len(kParts) {
			return false
		}

		if i < len(jParts) && i < len(kParts) {

			if jParts[i] == "*" && kParts[i] != "*" {
				return false
			}

			if jParts[i] != "*" && kParts[i] == "*" {
				return true
			}

			if jParts[i] < kParts[i] {
				return true
			}

			if jParts[i] > kParts[i] {
				return false
			}
		}

		if i >= len(jParts) && i < len(kParts) {
			return true
		}

		if i < len(jParts) && i >= len(kParts) {
			return false
		}

		i += 1
	}
}

func (s *entries) Swap(i, j int) {
	(*s)[i], (*s)[j] = (*s)[j], (*s)[i]
}

func (s *entries) Len() int {
	return len(*s)
}

type matcher struct {
	entries         entries
	defaultTemplate *template
}

func (m *matcher) Add(match string, template *template) {
	if match == "" {
		m.AddDefaultTemplate(template)
		return
	}
	m.entries = append(m.entries, &entry{&filter{sections: strings.Split(match, ".")}, template})
	sort.Sort(&m.entries)
}

func (m *matcher) AddDefaultTemplate(template *template) {
	m.defaultTemplate = template
}

func (m *matcher) Match(line string) *template {

	if len(m.entries) == 0 {
		return m.defaultTemplate
	}

	lineParts := strings.Split(line, ".")
	for _, entry := range m.entries {
		if entry.filter.Matches(lineParts) {
			return entry.template
		}
	}

	return m.defaultTemplate
}

type filter struct {
	sections []string
}

func (f *filter) Matches(lineParts []string) bool {
	for i, filterPart := range f.sections {
		if filterPart == "*" {
			continue
		}

		if i >= len(lineParts) {
			return false
		}

		if filterPart != lineParts[i] {
			return false
		}
	}
	return true
}

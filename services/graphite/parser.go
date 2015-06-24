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

	matcher := newMatcher()
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

// matcher
type matcher struct {
	root            *node
	defaultTemplate *template
}

func newMatcher() *matcher {
	return &matcher{
		root: &node{},
	}
}
func (m *matcher) Add(match string, template *template) {
	if match == "" {
		m.AddDefaultTemplate(template)
		return
	}
	m.root.Insert(match, template)
}

func (m *matcher) AddDefaultTemplate(template *template) {
	m.defaultTemplate = template
}

func (m *matcher) Match(line string) *template {
	tmpl := m.root.Search(line)
	if tmpl != nil {
		return tmpl
	}

	return m.defaultTemplate
}

type node struct {
	value    string
	children nodes
	template *template
}

func (n *node) insert(values []string, template *template) {
	if len(values) == 0 {
		return
	}

	for _, v := range n.children {
		if v.value == values[0] {
			v.insert(values[1:], template)
			return
		}
	}
	newNode := &node{value: values[0], template: template}
	n.children = append(n.children, newNode)
	sort.Sort(&n.children)
	newNode.insert(values[1:], template)
}

func (n *node) Insert(match string, template *template) {
	n.insert(strings.Split(match, "."), template)
}

func (n *node) search(lineParts []string) *template {
	if len(lineParts) == 0 || len(n.children) == 0 {
		return n.template
	}

	// If last element is a wildcard, don't include in this search since it's sorted
	// to the end but lexigraphically it would not alwasy be and sort.Search assumes
	// the slice is sorted.
	length := len(n.children)
	if n.children[length-1].value == "*" {
		length -= 1
	}

	i := sort.Search(length, func(i int) bool {
		return n.children[i].value == lineParts[0]
	})

	if i < len(n.children) && n.children[i].value == lineParts[0] {
		return n.children[i].search(lineParts[1:])
	} else {
		if n.children[len(n.children)-1].value == "*" {
			return n.children[len(n.children)-1].search(lineParts[1:])
		}
	}
	return n.template
}

func (n *node) Search(line string) *template {
	return n.search(strings.Split(line, "."))
}

type nodes []*node

// Less returns a boolean indicating whether the filter at position j
// is less than the filter at postion k.  Filters are order by string
// comparison of each component parts.  A wildcard value "*" is never
// less than a non-wildcard value.
//
// For example, the filters:
//             "*.*"
//             "servers.*""
//             "servers.localhost""
//             "*.localhost"
//
// Would be sorted as:
//             "servers.localhost""
//             "servers.*""
//             "*.localhost"
//             "*.*"
func (n *nodes) Less(j, k int) bool {
	if (*n)[j].value == "*" && (*n)[k].value != "*" {
		return false
	}

	if (*n)[j].value != "*" && (*n)[k].value == "*" {
		return true
	}

	return (*n)[j].value < (*n)[k].value
}

func (n *nodes) Swap(i, j int) { (*n)[i], (*n)[j] = (*n)[j], (*n)[i] }
func (n *nodes) Len() int      { return len(*n) }

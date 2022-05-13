package cardinality

import (
	"fmt"
	"strings"
	"sync"
	"text/tabwriter"

	"github.com/influxdata/influxdb/cmd/influx_inspect/report"
	"github.com/influxdata/influxdb/models"
)

var detailedHeader = []string{"DB", "RP", "measurement", "series", "fields", "tag total", "tags"}
var simpleHeader = []string{"DB", "RP", "measurement", "series"}

type measurementFactory struct {
	header   []string
	estTitle string
	newNode  func(inner bool) node
	counter  func() report.Counter
}

func (f *measurementFactory) printHeader(tw *tabwriter.Writer) error {
	_, err := fmt.Fprintln(tw, strings.Join(f.header, "\t"))
	return err
}

func (f *measurementFactory) printDivider(tw *tabwriter.Writer) error {
	divLine := makeTabDiv(f.header)
	_, err := fmt.Fprintln(tw, divLine)
	return err
}

func makeTabDiv(headers []string) string {
	div := make([]string, 0, len(headers))
	for _, s := range headers {
		div = append(div, strings.Repeat("-", len(s)))
	}
	return strings.Join(div, "\t")
}

func newSimpleMeasurementFactory(newCounterFn func() report.Counter, est string) *measurementFactory {
	return &measurementFactory{
		header:   simpleHeader,
		estTitle: est,
		newNode:  func(inner bool) node { return newSimpleNode(inner, newCounterFn) },
		counter:  newCounterFn,
	}
}

func newDetailedMeasurementFactory(newCounterFn func() report.Counter, est string) *measurementFactory {
	return &measurementFactory{
		header:   detailedHeader,
		estTitle: est,
		newNode:  func(inner bool) node { return newDetailedNode(inner, newCounterFn) },
		counter:  newCounterFn,
	}
}

type simpleNode struct {
	sync.Mutex
	report.Counter
	nodeMap
}

func (s *simpleNode) nextLevel() nodeMap {
	return s.nodeMap
}

func newSimpleNode(inner bool, fn func() report.Counter) *simpleNode {
	s := &simpleNode{Counter: fn()}
	if inner {
		s.nodeMap = make(nodeMap)
	}
	return s
}

func (s *simpleNode) recordMeasurement(key, _ []byte, _ models.Tags, _ func() report.Counter) {
	s.Add(key)
}

func (s *simpleNode) print(tw *tabwriter.Writer, db, rp, ms string) error {
	_, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%d\n",
		db,
		rp,
		ms,
		s.Count())
	return err
}

type detailedNode struct {
	simpleNode
	fields report.Counter
	tags   map[string]report.Counter
}

func (d *detailedNode) nextLevel() nodeMap {
	return d.nodeMap
}

func newDetailedNode(inner bool, fn func() report.Counter) *detailedNode {
	d := &detailedNode{
		simpleNode: simpleNode{
			Counter: fn(),
		},
		fields: fn(),
		tags:   make(map[string]report.Counter),
	}
	if inner {
		d.simpleNode.nodeMap = make(nodeMap)
	}
	return d
}

func (d *detailedNode) recordMeasurement(key, field []byte, tags models.Tags, newCounterFn func() report.Counter) {
	d.Add(key)
	d.fields.Add(field)
	for _, t := range tags {
		tc, ok := d.tags[string(t.Key)]
		if !ok {
			tc = newCounterFn()
			d.tags[string(t.Key)] = tc
		}
		tc.Add(t.Value)
	}
}

func (d *detailedNode) print(tw *tabwriter.Writer, db, rp, ms string) error {
	var tagKeys []string
	seriesN := d.Count()
	fieldsN := d.fields.Count()
	if ms != "" {
		tagKeys = make([]string, 0, len(d.tags))
	}
	tagN := uint64(0)
	for k, v := range d.tags {
		c := v.Count()
		tagN += c
		if ms != "" {
			tagKeys = append(tagKeys, fmt.Sprintf("%q: %d", k, c))
		}
	}

	_, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%d\t%d\t%s\n",
		db,
		rp,
		ms,
		seriesN,
		fieldsN,
		tagN,
		strings.Join(tagKeys, ", "))
	return err
}

func initRecord(n node, key []byte, field []byte, tags models.Tags, nodeFn func(bool) node, counterFn func() report.Counter, levelKeys ...string) {
	n.Lock()
	n.recordMeasurement(key, field, tags, counterFn)

	if len(levelKeys) > 0 && nil != n.nextLevel() {
		lc, ok := n.nextLevel()[levelKeys[0]]
		if !ok {
			lc = nodeFn(len(levelKeys) > 1)
		}
		n.nextLevel()[levelKeys[0]] = lc
		n.Unlock()
		initRecord(lc, key, field, tags, nodeFn, counterFn, levelKeys[1:]...)
	} else {
		n.Unlock()
	}
}

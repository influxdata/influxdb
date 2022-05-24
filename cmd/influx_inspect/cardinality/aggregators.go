package cardinality

import (
	"fmt"
	"strings"
	"sync"
	"text/tabwriter"

	"github.com/influxdata/influxdb/cmd/influx_inspect/report"
	"github.com/influxdata/influxdb/models"
)

type rollupNodeMap map[string]rollupNode
type rollupNodeLock interface {
	Lock()
	Unlock()
}

type rollupNode interface {
	rollupNodeLock
	report.Counter
	children() rollupNodeMap
	recordSeries(dbRp string, key, field []byte, tags models.Tags)
	print(tw *tabwriter.Writer, db, rp, ms string) error
	isLeaf() bool
	child(key string, isLeaf bool) rollupNode
}

var detailedHeader = []string{"DB", "RP", "measurement", "series", "fields", "tag total", "tags"}
var simpleHeader = []string{"DB", "RP", "measurement", "series"}

type rollupNodeFactory struct {
	header   []string
	estTitle string
	newNode  func(inner bool) rollupNode
	counter  func() report.Counter
}

var nodeFactory *rollupNodeFactory

func CreateNodeFactory(detailed, exact bool) *rollupNodeFactory {
	estTitle := " (est.)"
	newCounterFn := report.NewHLLCounter
	if exact {
		newCounterFn = report.NewExactCounter
		estTitle = ""
	}

	if detailed {
		nodeFactory = newDetailedNodeFactory(newCounterFn, estTitle)
	} else {
		nodeFactory = newSimpleNodeFactory(newCounterFn, estTitle)
	}
	return nodeFactory
}

func (f *rollupNodeFactory) printHeader(tw *tabwriter.Writer) error {
	_, err := fmt.Fprintln(tw, strings.Join(f.header, "\t"))
	return err
}

func (f *rollupNodeFactory) printDivider(tw *tabwriter.Writer) error {
	divLine := f.makeTabDivider()
	_, err := fmt.Fprintln(tw, divLine)
	return err
}

func (f *rollupNodeFactory) makeTabDivider() string {
	div := make([]string, 0, len(f.header))
	for _, s := range f.header {
		div = append(div, strings.Repeat("-", len(s)))
	}
	return strings.Join(div, "\t")
}

func newSimpleNodeFactory(newCounterFn func() report.Counter, est string) *rollupNodeFactory {
	return &rollupNodeFactory{
		header:   simpleHeader,
		estTitle: est,
		newNode:  func(inner bool) rollupNode { return newSimpleNode(inner, newCounterFn) },
		counter:  newCounterFn,
	}
}

func newDetailedNodeFactory(newCounterFn func() report.Counter, est string) *rollupNodeFactory {
	return &rollupNodeFactory{
		header:   detailedHeader,
		estTitle: est,
		newNode:  func(inner bool) rollupNode { return newDetailedNode(inner, newCounterFn) },
		counter:  newCounterFn,
	}
}

type simpleNode struct {
	sync.Mutex
	report.Counter
	rollupNodeMap
}

func (s *simpleNode) children() rollupNodeMap {
	return s.rollupNodeMap
}

func (s *simpleNode) child(key string, isLeaf bool) rollupNode {
	if s.isLeaf() {
		return nil
	}
	c, ok := s.children()[key]
	if !ok {
		c = nodeFactory.newNode(!isLeaf)
		s.children()[key] = c
	}
	return c
}

func (s *simpleNode) isLeaf() bool {
	return s.children() == nil
}

func newSimpleNode(inner bool, fn func() report.Counter) *simpleNode {
	s := &simpleNode{Counter: fn()}
	if inner {
		s.rollupNodeMap = make(rollupNodeMap)
	}
	return s
}

func (s *simpleNode) recordSeries(dbRp string, key, field []byte, tags models.Tags) {
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

func newDetailedNode(inner bool, fn func() report.Counter) *detailedNode {
	d := &detailedNode{
		simpleNode: simpleNode{
			Counter: fn(),
		},
		fields: fn(),
		tags:   make(map[string]report.Counter),
	}
	if inner {
		d.simpleNode.rollupNodeMap = make(rollupNodeMap)
	}
	return d
}

func (d *detailedNode) recordSeries(dbRpMs string, key, field []byte, tags models.Tags) {
	d.simpleNode.recordSeries(dbRpMs, key, field, tags)
	d.fields.Add(field)
	for _, t := range tags {
		// Add database and retention policy to correctly aggregate
		// in inner (non-leaf) nodes
		canonTag := dbRpMs + string(t.Key)
		tc, ok := d.tags[canonTag]
		if !ok {
			tc = nodeFactory.counter()
			d.tags[canonTag] = tc
		}
		tc.Add(t.Value)
	}
}

func (d *detailedNode) print(tw *tabwriter.Writer, db, rp, ms string) error {
	seriesN := d.Count()
	fieldsN := d.fields.Count()
	var tagKeys []string
	tagN := uint64(0)

	if d.isLeaf() {
		tagKeys = make([]string, 0, len(d.tags))
	}
	for k, v := range d.tags {
		c := v.Count()
		tagN += c
		if d.isLeaf() {
			tagKeys = append(tagKeys, fmt.Sprintf("%q: %d", k[strings.LastIndex(k, ".")+1:], c))
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

func initRecord(r rollupNode, depth, totalDepth int, db, rp, measurement string, key []byte, field []byte, tags models.Tags) {
	r.Lock()
	dbRpMs := fmt.Sprintf("%s.%s.%s.", db, rp, measurement)
	r.recordSeries(dbRpMs, key, field, tags)

	switch depth {
	case 2:
		c := r.child(measurement, true)
		c.recordSeries(dbRpMs, key, field, tags)
	case 1:
		c := r.child(rp, depth >= totalDepth)
		if depth < totalDepth {
			initRecord(c, depth+1, totalDepth, db, rp, measurement, key, field, tags)
		}
	case 0:
		c := r.child(db, depth >= totalDepth)
		if depth < totalDepth {
			initRecord(c, depth+1, totalDepth, db, rp, measurement, key, field, tags)
		}
	default:
	}
	r.Unlock()
}

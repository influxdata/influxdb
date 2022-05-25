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
	recordSeries(db, rp, ms string, key, field []byte, tags models.Tags)
	print(tw *tabwriter.Writer, printTags bool, db, rp, ms string) error
	isLeaf() bool
	child(key string, isLeaf bool) rollupNode
}

type nodeWrapper struct {
	rollupNode
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
		newNode:  func(isLeaf bool) rollupNode { return newDetailedNode(isLeaf, newCounterFn) },
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
		panic("Trying to get the child to a leaf node")
	}
	s.Lock()
	defer s.Unlock()
	c, ok := s.children()[key]
	if !ok {
		c = nodeFactory.newNode(isLeaf)
		s.children()[key] = c
	}
	return c
}

func (s *simpleNode) isLeaf() bool {
	return s.children() == nil
}

func newSimpleNode(isLeaf bool, fn func() report.Counter) *simpleNode {
	s := &simpleNode{Counter: fn()}
	if !isLeaf {
		s.rollupNodeMap = make(rollupNodeMap)
	} else {
		s.rollupNodeMap = nil
	}
	return s
}

func (s *simpleNode) recordSeries(db, rp, _ string, key, _ []byte, _ models.Tags) {
	s.Lock()
	defer s.Unlock()
	s.recordSeriesNoLock(db, rp, key)
}

func (s *simpleNode) recordSeriesNoLock(db, rp string, key []byte) {
	s.Add([]byte(fmt.Sprintf("%s.%s.%s", db, rp, key)))
}

func (s *simpleNode) print(tw *tabwriter.Writer, _ bool, db, rp, ms string) error {
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

func newDetailedNode(isLeaf bool, fn func() report.Counter) *detailedNode {
	d := &detailedNode{
		simpleNode: simpleNode{
			Counter: fn(),
		},
		fields: fn(),
		tags:   make(map[string]report.Counter),
	}
	if !isLeaf {
		d.simpleNode.rollupNodeMap = make(rollupNodeMap)
	} else {
		d.simpleNode.rollupNodeMap = nil
	}
	return d
}

func (d *detailedNode) recordSeries(db, rp, ms string, key, field []byte, tags models.Tags) {
	d.Lock()
	defer d.Unlock()
	d.simpleNode.recordSeriesNoLock(db, rp, key)
	d.fields.Add([]byte(fmt.Sprintf("%s.%s.%s.%s", db, rp, ms, field)))
	for _, t := range tags {
		// Add database, retention policy, and measurement
		// to correctly aggregate in inner (non-leaf) nodes
		canonTag := fmt.Sprintf("%s.%s.%s.%s", db, rp, ms, t.Key)
		tc, ok := d.tags[canonTag]
		if !ok {
			tc = nodeFactory.counter()
			d.tags[canonTag] = tc
		}
		tc.Add(t.Value)
	}
}

func (d *detailedNode) print(tw *tabwriter.Writer, printTags bool, db, rp, ms string) error {
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
		if printTags {
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

func (r *nodeWrapper) record(depth, totalDepth int, db, rp, measurement string, key []byte, field []byte, tags models.Tags) {
	r.recordSeries(db, rp, measurement, key, field, tags)

	switch depth {
	case 2:
		if depth < totalDepth {
			// Create measurement level in tree
			c := r.child(measurement, true)
			c.recordSeries(db, rp, measurement, key, field, tags)
		}
	case 1:
		if depth < totalDepth {
			// Create retention policy level in tree
			c := nodeWrapper{r.child(rp, (depth+1) == totalDepth)}
			c.record(depth+1, totalDepth, db, rp, measurement, key, field, tags)
		}
	case 0:
		if depth < totalDepth {
			// Create database level in tree
			c := nodeWrapper{r.child(db, (depth+1) == totalDepth)}
			c.record(depth+1, totalDepth, db, rp, measurement, key, field, tags)
		}
	default:
	}
}

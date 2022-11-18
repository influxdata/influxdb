package aggregators

import (
	"fmt"
	"strings"
	"sync"
	"text/tabwriter"

	report "github.com/influxdata/influxdb/v2/cmd/influxd/inspect/report_tsm"
	"github.com/influxdata/influxdb/v2/models"
)

type rollupNodeMap map[string]RollupNode

type RollupNode interface {
	sync.Locker
	report.Counter
	Children() rollupNodeMap
	RecordSeries(bucket, rp, ms string, key, field []byte, tags models.Tags)
	Print(tw *tabwriter.Writer, printTags bool, bucket, rp, ms string) error
	isLeaf() bool
	child(key string, isLeaf bool) NodeWrapper
}

type NodeWrapper struct {
	RollupNode
}

var detailedHeader = []string{"bucket", "retention policy", "measurement", "series", "fields", "tag total", "tags"}
var simpleHeader = []string{"bucket", "retention policy", "measurement", "series"}

type RollupNodeFactory struct {
	header   []string
	EstTitle string
	NewNode  func(isLeaf bool) NodeWrapper
	counter  func() report.Counter
}

var nodeFactory *RollupNodeFactory

func CreateNodeFactory(detailed, exact bool) *RollupNodeFactory {
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

func (f *RollupNodeFactory) PrintHeader(tw *tabwriter.Writer) error {
	_, err := fmt.Fprintln(tw, strings.Join(f.header, "\t"))
	return err
}

func (f *RollupNodeFactory) PrintDivider(tw *tabwriter.Writer) error {
	divLine := f.makeTabDivider()
	_, err := fmt.Fprintln(tw, divLine)
	return err
}

func (f *RollupNodeFactory) makeTabDivider() string {
	div := make([]string, 0, len(f.header))
	for _, s := range f.header {
		div = append(div, strings.Repeat("-", len(s)))
	}
	return strings.Join(div, "\t")
}

func newSimpleNodeFactory(newCounterFn func() report.Counter, est string) *RollupNodeFactory {
	return &RollupNodeFactory{
		header:   simpleHeader,
		EstTitle: est,
		NewNode:  func(isLeaf bool) NodeWrapper { return NodeWrapper{newSimpleNode(isLeaf, newCounterFn)} },
		counter:  newCounterFn,
	}
}

func newDetailedNodeFactory(newCounterFn func() report.Counter, est string) *RollupNodeFactory {
	return &RollupNodeFactory{
		header:   detailedHeader,
		EstTitle: est,
		NewNode:  func(isLeaf bool) NodeWrapper { return NodeWrapper{newDetailedNode(isLeaf, newCounterFn)} },
		counter:  newCounterFn,
	}
}

type simpleNode struct {
	sync.Mutex
	report.Counter
	rollupNodeMap
}

func (s *simpleNode) Children() rollupNodeMap {
	return s.rollupNodeMap
}

func (s *simpleNode) child(key string, isLeaf bool) NodeWrapper {
	if s.isLeaf() {
		panic("Trying to get the child to a leaf node")
	}
	s.Lock()
	defer s.Unlock()
	c, ok := s.Children()[key]
	if !ok {
		c = nodeFactory.NewNode(isLeaf)
		s.Children()[key] = c
	}
	return NodeWrapper{c}
}

func (s *simpleNode) isLeaf() bool {
	return s.Children() == nil
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

func (s *simpleNode) RecordSeries(bucket, rp, _ string, key, _ []byte, _ models.Tags) {
	s.Lock()
	defer s.Unlock()
	s.recordSeriesNoLock(bucket, rp, key)
}

func (s *simpleNode) recordSeriesNoLock(bucket, rp string, key []byte) {
	s.Add([]byte(fmt.Sprintf("%s.%s.%s", bucket, rp, key)))
}

func (s *simpleNode) Print(tw *tabwriter.Writer, _ bool, bucket, rp, ms string) error {
	_, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%d\n",
		bucket,
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

func (d *detailedNode) RecordSeries(bucket, rp, ms string, key, field []byte, tags models.Tags) {
	d.Lock()
	defer d.Unlock()
	d.simpleNode.recordSeriesNoLock(bucket, rp, key)
	d.fields.Add([]byte(fmt.Sprintf("%s.%s.%s.%s", bucket, rp, ms, field)))
	for _, t := range tags {
		// Add database, retention policy, and measurement
		// to correctly aggregate in inner (non-leaf) nodes
		canonTag := fmt.Sprintf("%s.%s.%s.%s", bucket, rp, ms, t.Key)
		tc, ok := d.tags[canonTag]
		if !ok {
			tc = nodeFactory.counter()
			d.tags[canonTag] = tc
		}
		tc.Add(t.Value)
	}
}

func (d *detailedNode) Print(tw *tabwriter.Writer, printTags bool, bucket, rp, ms string) error {
	seriesN := d.Count()
	fieldsN := d.fields.Count()
	var tagKeys []string
	tagN := uint64(0)

	if printTags {
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
		bucket,
		rp,
		ms,
		seriesN,
		fieldsN,
		tagN,
		strings.Join(tagKeys, ", "))
	return err
}

func (r *NodeWrapper) Record(depth, totalDepth int, bucket, rp, measurement string, key []byte, field []byte, tags models.Tags) {
	r.RecordSeries(bucket, rp, measurement, key, field, tags)

	switch depth {
	case 2:
		if depth < totalDepth {
			// Create measurement level in tree
			c := r.child(measurement, true)
			c.RecordSeries(bucket, rp, measurement, key, field, tags)
		}
	case 1:
		if depth < totalDepth {
			// Create retention policy level in tree
			c := r.child(rp, (depth+1) == totalDepth)
			c.Record(depth+1, totalDepth, bucket, rp, measurement, key, field, tags)
		}
	case 0:
		if depth < totalDepth {
			// Create database level in tree
			c := r.child(bucket, (depth+1) == totalDepth)
			c.Record(depth+1, totalDepth, bucket, rp, measurement, key, field, tags)
		}
	default:
	}
}

package engine

import "github.com/influxdb/influxdb/protocol"

// Traverses the buckets in either ascending or descending order. Each
// bucket have the form (C1, C2, time) where C1 and C2 are column
// values and time is the timestamp. The traverser applies the given
// function on all nodes with the timestamp in either ascending or
// descending order
type bucketTraverser struct {
	trie        *Trie // the trie we're going to traverse
	last        int64 // the last bucket's timestamp that we will visit
	current     int64 // the current timestamp
	step        int64 // the step (could be negative if the query is descending)
	levels      int   // the number of levels in the trie
	aggregators int   // the number of aggregators that we have to run
	asc         bool  // ascending or descending query
}

func newBucketTraverser(trie *Trie, levels, aggregators int, startBucket, endBucket, durationMicro int64, asc bool) *bucketTraverser {
	bt := &bucketTraverser{
		trie:        trie,
		levels:      levels,
		aggregators: aggregators,
		last:        startBucket,
		current:     endBucket,
		step:        -durationMicro,
		asc:         asc,
	}
	if !asc {
		return bt
	}

	bt.last = endBucket
	bt.current = startBucket
	bt.step = durationMicro
	return bt
}

func (bt *bucketTraverser) reachedEnd() bool {
	if bt.asc {
		return bt.current > bt.last
	}

	return bt.current < bt.last
}

func (bt *bucketTraverser) apply(f func(fv []*protocol.FieldValue, node *Node) error) error {
	for !bt.reachedEnd() {
		timestamp := &protocol.FieldValue{Int64Value: protocol.Int64(bt.current)}
		defaultChildNode := &Node{states: make([]interface{}, bt.aggregators)}
		err := bt.trie.TraverseLevel(bt.levels, func(v []*protocol.FieldValue, node *Node) error {
			childNode := node.GetChildNode(timestamp)
			if childNode == nil {
				childNode = defaultChildNode
			}
			return f(append(v, timestamp), childNode)
		})
		if err != nil {
			return err
		}

		bt.current += bt.step
	}
	return nil
}

package engine

import (
	"fmt"
	"sort"

	"github.com/influxdb/influxdb/protocol"
)

// TODO: add some documentation

// A trie structure to map groups to aggregates, each level of the
// group corresponds to a field in the group by clause

// values at each level are sorted like this => nil, boolean, int64, float64, string

type Trie struct {
	numLevels int
	numStates int
	rootNode  *Node
}

const MaxInt = int(^uint(0) >> 1)

type Nodes []*Node

type Node struct {
	isLeaf     bool
	value      *protocol.FieldValue // the value of the group by column corresponding to this level
	states     []interface{}        // the aggregator state
	childNodes Nodes                // the slice of the next level
}

func NewTrie(numLevels, numStates int) *Trie {
	trie := &Trie{numLevels, numStates, nil}
	trie.Clear()
	return trie
}

func (self *Trie) Clear() {
	self.rootNode = &Node{true, nil, make([]interface{}, self.numStates), nil}
}

func (self *Trie) CountLeafNodes() int {
	return self.rootNode.CountLeafNodes()
}

func (self *Trie) Traverse(f func([]*protocol.FieldValue, *Node) error) error {
	return self.TraverseLevel(-1, f)
}

// Traverses all nodes at the given level, -1 to get nodes at the most bottom level
func (self *Trie) TraverseLevel(level int, f func([]*protocol.FieldValue, *Node) error) error {
	if self.numLevels == 0 {
		return f(nil, self.rootNode)
	}

	if level == -1 {
		level = MaxInt
	}
	return self.rootNode.traverse(level, nil, f)
}

func (self *Trie) GetNode(values []*protocol.FieldValue) *Node {
	if len(values) != self.numLevels {
		panic(fmt.Errorf("number of levels doesn't match values. Expected: %d, Actual: %d", self.numLevels, len(values)))
	}

	if self.numLevels == 0 {
		return self.rootNode
	}

	node := self.rootNode
	for idx, v := range values {
		if self.numLevels-idx-1 > 0 {
			node = node.findOrCreateNode(v, 0)
		} else {
			node = node.findOrCreateNode(v, self.numStates)
		}
	}

	return node
}

func (self *Node) CountLeafNodes() int {
	size := 0
	for _, child := range self.childNodes {
		size += child.CountLeafNodes()
		if child.isLeaf {
			size++
		}
	}
	return size
}

func (self *Node) GetChildNode(value *protocol.FieldValue) *Node {
	idx := self.childNodes.findNode(value)
	if idx == len(self.childNodes) || !self.childNodes[idx].value.Equals(value) {
		return nil
	}
	return self.childNodes[idx]
}

func (self *Node) traverse(level int, values []*protocol.FieldValue, f func([]*protocol.FieldValue, *Node) error) error {
	if level == 0 {
		return f(values, self)
	}

	for _, node := range self.childNodes {
		if node.isLeaf {
			err := f(append(values, node.value), node)
			if err != nil {
				return err
			}
			continue
		}
		err := node.traverse(level-1, append(values, node.value), f)
		if err != nil {
			return err
		}
	}
	return nil
}

func (self *Node) findOrCreateNode(value *protocol.FieldValue, numOfStates int) *Node {
	idx := self.childNodes.findNode(value)
	if idx == len(self.childNodes) || !self.childNodes[idx].value.Equals(value) {
		// add the new node
		node := self.createChildNode(value, numOfStates)
		self.childNodes = append(self.childNodes, node)
		// if idx is equal to the previous length, then leave it at the
		// end, otherwise, move it to that index.
		if idx != len(self.childNodes)-1 {
			// shift all values to the right by one
			copy(self.childNodes[idx+1:], self.childNodes[idx:])
			self.childNodes[idx] = node
		}
		return node
	}
	return self.childNodes[idx]
}

func (self *Node) createChildNode(value *protocol.FieldValue, numOfStates int) *Node {
	node := &Node{value: value}
	if numOfStates > 0 {
		node.states = make([]interface{}, numOfStates)
		node.isLeaf = true
		return node
	}
	return node
}

func (self Nodes) findNode(value *protocol.FieldValue) int {
	return sort.Search(len(self), func(i int) bool {
		return self[i].value.GreaterOrEqual(value)
	})
}

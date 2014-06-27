package engine

import (
	"github.com/influxdb/influxdb/protocol"
	. "launchpad.net/gocheck"
)

type TrieTestSuite struct {
}

var _ = Suite(&TrieTestSuite{})

func (self *TrieTestSuite) TestTrie(c *C) {
	trie := NewTrie(2, 1)

	firstValue := []*protocol.FieldValue{
		{StringValue: protocol.String("some_value")},
		{Int64Value: protocol.Int64(1)},
	}
	firstNode := trie.GetNode(firstValue)
	c.Assert(firstNode, NotNil)
	c.Assert(trie.GetNode(firstValue), DeepEquals, firstNode)
	c.Assert(trie.CountLeafNodes(), Equals, 1)

	secondValue := []*protocol.FieldValue{
		{StringValue: protocol.String("some_value")},
		{Int64Value: protocol.Int64(2)},
	}
	secondNode := trie.GetNode(secondValue)
	c.Assert(secondNode, NotNil)
	c.Assert(trie.GetNode(secondValue), DeepEquals, secondNode)
	c.Assert(trie.CountLeafNodes(), Equals, 2)

	thirdValue := []*protocol.FieldValue{
		{StringValue: protocol.String("another_value")},
		{Int64Value: protocol.Int64(1)},
	}
	thirdNode := trie.GetNode(thirdValue)
	c.Assert(thirdNode, NotNil)
	c.Assert(trie.GetNode(thirdValue), DeepEquals, thirdNode)
	c.Assert(trie.CountLeafNodes(), Equals, 3)

	nodes := 0
	orderValues := [][]*protocol.FieldValue{thirdValue, firstValue, secondValue}
	c.Assert(trie.Traverse(func(v []*protocol.FieldValue, _ *Node) error {
		c.Assert(v, DeepEquals, orderValues[nodes])
		nodes++
		return nil
	}), IsNil)
	c.Assert(nodes, Equals, trie.CountLeafNodes())

	// make sure TraverseLevel work as expected
	ns := []*Node{}
	c.Assert(trie.TraverseLevel(0, func(_ []*protocol.FieldValue, n *Node) error {
		ns = append(ns, n)
		return nil
	}), IsNil)
	c.Assert(ns, HasLen, 1) // should return the root node only
	c.Assert(ns[0].value, IsNil)

	ns = []*Node{}
	c.Assert(trie.TraverseLevel(1, func(_ []*protocol.FieldValue, n *Node) error {
		ns = append(ns, n)
		return nil
	}), IsNil)
	c.Assert(ns, HasLen, 2) // should return the root node only
	c.Assert(ns[0].value.GetStringValue(), Equals, "another_value")
	c.Assert(ns[1].value.GetStringValue(), Equals, "some_value")

	c.Assert(ns[0].GetChildNode(&protocol.FieldValue{Int64Value: protocol.Int64(1)}).isLeaf, Equals, true)
	c.Assert(ns[0].GetChildNode(&protocol.FieldValue{Int64Value: protocol.Int64(2)}), IsNil)

	c.Assert(ns[1].GetChildNode(&protocol.FieldValue{Int64Value: protocol.Int64(1)}).isLeaf, Equals, true)
	c.Assert(ns[1].GetChildNode(&protocol.FieldValue{Int64Value: protocol.Int64(2)}).isLeaf, Equals, true)
}

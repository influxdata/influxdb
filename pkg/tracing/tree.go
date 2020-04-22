package tracing

import (
	"github.com/xlab/treeprint"
)

// A Visitor's Visit method is invoked for each node encountered by Walk.
// If the result of Visit is not nil, Walk visits each of the children.
type Visitor interface {
	Visit(*TreeNode) Visitor
}

// A TreeNode represents a single node in the graph.
type TreeNode struct {
	Raw      RawSpan
	Children []*TreeNode
}

// String returns the tree as a string.
func (t *TreeNode) String() string {
	if t == nil {
		return ""
	}
	tv := newTreeVisitor()
	Walk(tv, t)
	return tv.root.String()
}

// Walk traverses the graph in a depth-first order, calling v.Visit
// for each node until completion or v.Visit returns nil.
func Walk(v Visitor, node *TreeNode) {
	if v = v.Visit(node); v == nil {
		return
	}

	for _, c := range node.Children {
		Walk(v, c)
	}
}

type treeVisitor struct {
	root  treeprint.Tree
	trees []treeprint.Tree
}

func newTreeVisitor() *treeVisitor {
	t := treeprint.New()
	return &treeVisitor{root: t, trees: []treeprint.Tree{t}}
}

func (v *treeVisitor) Visit(n *TreeNode) Visitor {
	t := v.trees[len(v.trees)-1].AddBranch(n.Raw.Name)
	v.trees = append(v.trees, t)

	if labels := n.Raw.Labels; len(labels) > 0 {
		l := t.AddBranch("labels")
		for _, ll := range n.Raw.Labels {
			l.AddNode(ll.Key + ": " + ll.Value)
		}
	}

	for _, k := range n.Raw.Fields {
		t.AddNode(k.String())
	}

	for _, cn := range n.Children {
		Walk(v, cn)
	}

	v.trees[len(v.trees)-1] = nil
	v.trees = v.trees[:len(v.trees)-1]

	return nil
}

package radix

// This is a fork of https://github.com/armon/go-radix that removes the
// ability to update nodes as well as uses fixed int value type.

import (
	"bytes"
	"sort"
	"sync"
)

// leafNode is used to represent a value
type leafNode struct {
	valid bool // true if key/val are valid
	key   []byte
	val   int
}

// edge is used to represent an edge node
type edge struct {
	label byte
	node  *node
}

type node struct {
	// leaf is used to store possible leaf
	leaf leafNode

	// prefix is the common prefix we ignore
	prefix []byte

	// Edges should be stored in-order for iteration.
	// We avoid a fully materialized slice to save memory,
	// since in most cases we expect to be sparse
	edges edges
}

func (n *node) isLeaf() bool {
	return n.leaf.valid
}

func (n *node) addEdge(e edge) {
	// find the insertion point with bisection
	num := len(n.edges)
	i, j := 0, num
	for i < j {
		h := int(uint(i+j) >> 1)
		if n.edges[h].label < e.label {
			i = h + 1
		} else {
			j = h
		}
	}

	// make room, copy the suffix, and insert.
	n.edges = append(n.edges, edge{})
	copy(n.edges[i+1:], n.edges[i:])
	n.edges[i] = e
}

func (n *node) replaceEdge(e edge) {
	num := len(n.edges)
	idx := sort.Search(num, func(i int) bool {
		return n.edges[i].label >= e.label
	})
	if idx < num && n.edges[idx].label == e.label {
		n.edges[idx].node = e.node
		return
	}
	panic("replacing missing edge")
}

func (n *node) getEdge(label byte) *node {
	// linear search for small slices
	if len(n.edges) < 16 {
		for _, e := range n.edges {
			if e.label == label {
				return e.node
			}
		}
		return nil
	}

	// binary search for larger
	num := len(n.edges)
	i, j := 0, num
	for i < j {
		h := int(uint(i+j) >> 1)
		if n.edges[h].label < label {
			i = h + 1
		} else {
			j = h
		}
	}
	if i < num && n.edges[i].label == label {
		return n.edges[i].node
	}
	return nil
}

type edges []edge

// Tree implements a radix tree. This can be treated as a
// Dictionary abstract data type. The main advantage over
// a standard hash map is prefix-based lookups and
// ordered iteration. The tree is safe for concurrent access.
type Tree struct {
	mu   sync.RWMutex
	root *node
	size int
	buf  buffer
}

// New returns an empty Tree
func New() *Tree {
	return &Tree{root: &node{}}
}

// NewFromMap returns a new tree containing the keys
// from an existing map
func NewFromMap(m map[string]int) *Tree {
	t := &Tree{root: &node{}}
	for k, v := range m {
		t.Insert([]byte(k), v)
	}
	return t
}

// Len is used to return the number of elements in the tree
func (t *Tree) Len() int {
	t.mu.RLock()
	size := t.size
	t.mu.RUnlock()

	return size
}

// longestPrefix finds the length of the shared prefix
// of two strings
func longestPrefix(k1, k2 []byte) int {
	// for loops can't be inlined, but goto's can. we also use uint to help
	// out the compiler to prove bounds checks aren't necessary on the index
	// operations.

	lk1, lk2 := uint(len(k1)), uint(len(k2))
	i := uint(0)

loop:
	if lk1 <= i || lk2 <= i {
		return int(i)
	}
	if k1[i] != k2[i] {
		return int(i)
	}
	i++
	goto loop
}

// Insert is used to add a newentry or update
// an existing entry. Returns if inserted.
func (t *Tree) Insert(s []byte, v int) (int, bool) {
	t.mu.RLock()

	var parent *node
	n := t.root
	search := s

	for {
		// Handle key exhaution
		if len(search) == 0 {
			if n.isLeaf() {
				old := n.leaf.val

				t.mu.RUnlock()
				return old, false
			}

			n.leaf = leafNode{
				key:   t.buf.Copy(s),
				val:   v,
				valid: true,
			}
			t.size++

			t.mu.RUnlock()
			return v, true
		}

		// Look for the edge
		parent = n
		n = n.getEdge(search[0])

		// No edge, create one
		if n == nil {
			newNode := &node{
				leaf: leafNode{
					key:   t.buf.Copy(s),
					val:   v,
					valid: true,
				},
				prefix: t.buf.Copy(search),
			}

			e := edge{
				label: search[0],
				node:  newNode,
			}

			parent.addEdge(e)
			t.size++

			t.mu.RUnlock()
			return v, true
		}

		// Determine longest prefix of the search key on match
		commonPrefix := longestPrefix(search, n.prefix)
		if commonPrefix == len(n.prefix) {
			search = search[commonPrefix:]
			continue
		}

		// Split the node
		t.size++
		child := &node{
			prefix: t.buf.Copy(search[:commonPrefix]),
		}
		parent.replaceEdge(edge{
			label: search[0],
			node:  child,
		})

		// Restore the existing node
		child.addEdge(edge{
			label: n.prefix[commonPrefix],
			node:  n,
		})
		n.prefix = n.prefix[commonPrefix:]

		// Create a new leaf node
		leaf := leafNode{
			key:   t.buf.Copy(s),
			val:   v,
			valid: true,
		}

		// If the new key is a subset, add to to this node
		search = search[commonPrefix:]
		if len(search) == 0 {
			child.leaf = leaf

			t.mu.RUnlock()
			return v, true
		}

		// Create a new edge for the node
		child.addEdge(edge{
			label: search[0],
			node: &node{
				leaf:   leaf,
				prefix: t.buf.Copy(search),
			},
		})

		t.mu.RUnlock()
		return v, true
	}
}

// DeletePrefix is used to delete the subtree under a prefix
// Returns how many nodes were deleted
// Use this to delete large subtrees efficiently
func (t *Tree) DeletePrefix(s []byte) int {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.deletePrefix(nil, t.root, s)
}

// delete does a recursive deletion
func (t *Tree) deletePrefix(parent, n *node, prefix []byte) int {
	// Check for key exhaustion
	if len(prefix) == 0 {
		// Remove the leaf node
		subTreeSize := 0
		//recursively walk from all edges of the node to be deleted
		recursiveWalk(n, func(s []byte, v int) bool {
			subTreeSize++
			return false
		})
		if n.isLeaf() {
			n.leaf = leafNode{}
		}
		n.edges = nil // deletes the entire subtree

		// Check if we should merge the parent's other child
		if parent != nil && parent != t.root && len(parent.edges) == 1 && !parent.isLeaf() {
			parent.mergeChild()
		}
		t.size -= subTreeSize
		return subTreeSize
	}

	// Look for an edge
	label := prefix[0]
	child := n.getEdge(label)
	if child == nil || (!bytes.HasPrefix(child.prefix, prefix) && !bytes.HasPrefix(prefix, child.prefix)) {
		return 0
	}

	// Consume the search prefix
	if len(child.prefix) > len(prefix) {
		prefix = prefix[len(prefix):]
	} else {
		prefix = prefix[len(child.prefix):]
	}
	return t.deletePrefix(n, child, prefix)
}

func (n *node) mergeChild() {
	e := n.edges[0]
	child := e.node
	prefix := make([]byte, 0, len(n.prefix)+len(child.prefix))
	prefix = append(prefix, n.prefix...)
	prefix = append(prefix, child.prefix...)
	n.prefix = prefix
	n.leaf = child.leaf
	n.edges = child.edges
}

// Get is used to lookup a specific key, returning
// the value and if it was found
func (t *Tree) Get(s []byte) (int, bool) {
	t.mu.RLock()

	n := t.root
	search := s
	for {
		// Check for key exhaution
		if len(search) == 0 {
			if n.isLeaf() {
				t.mu.RUnlock()
				return n.leaf.val, true
			}
			break
		}

		// Look for an edge
		n = n.getEdge(search[0])
		if n == nil {
			break
		}

		// Consume the search prefix
		if bytes.HasPrefix(search, n.prefix) {
			search = search[len(n.prefix):]
		} else {
			break
		}
	}

	t.mu.RUnlock()
	return 0, false
}

// walkFn is used when walking the tree. Takes a
// key and value, returning if iteration should
// be terminated.
type walkFn func(s []byte, v int) bool

// recursiveWalk is used to do a pre-order walk of a node
// recursively. Returns true if the walk should be aborted
func recursiveWalk(n *node, fn walkFn) bool {
	// Visit the leaf values if any
	if n.leaf.valid && fn(n.leaf.key, n.leaf.val) {
		return true
	}

	// Recurse on the children
	for _, e := range n.edges {
		if recursiveWalk(e.node, fn) {
			return true
		}
	}
	return false
}

// Minimum is used to return the minimum value in the tree
func (t *Tree) Minimum() ([]byte, int, bool) {
	t.mu.RLock()

	n := t.root
	for {
		if n.isLeaf() {
			t.mu.RUnlock()
			return n.leaf.key, n.leaf.val, true
		}
		if len(n.edges) > 0 {
			n = n.edges[0].node
		} else {
			break
		}
	}

	t.mu.RUnlock()
	return nil, 0, false
}

// Maximum is used to return the maximum value in the tree
func (t *Tree) Maximum() ([]byte, int, bool) {
	t.mu.RLock()

	n := t.root
	for {
		if num := len(n.edges); num > 0 {
			n = n.edges[num-1].node
			continue
		}
		if n.isLeaf() {
			t.mu.RUnlock()
			return n.leaf.key, n.leaf.val, true
		}
		break
	}

	t.mu.RUnlock()
	return nil, 0, false
}

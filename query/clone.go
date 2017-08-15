package query

import (
	"container/list"
	"reflect"

	"github.com/influxdata/influxdb/influxql"
)

func (c *compiledField) Clone() *compiledField {
	// Clone the graph.
	cloner := newCloner()
	output := cloner.Clone(c.Output)

	// Duplicate the symbol table to the new write edges.
	// A shallow copy of the symbols should work since they should all be stateless.
	table := &SymbolTable{
		Table: make(map[*WriteEdge]Symbol, len(c.Symbols.Table)),
	}
	for edge, symbol := range c.Symbols.Table {
		table.Table[cloner.WriteEdges[edge]] = symbol
	}

	// Clone the field so we can modify its attributes with a new name.
	field := *c.Field
	field.Expr = influxql.CloneExpr(field.Expr)
	clone := &compiledField{
		global:  c.global,
		Field:   &field,
		Output:  output,
		Symbols: table,
	}
	return clone
}

// cloner contains the state used when cloning a graph.
type cloner struct {
	// Nodes contains a mapping of old nodes to their clones.
	Nodes map[Node]Node

	// ReadEdges contains a mapping of old read edges to their clones.
	ReadEdges map[*ReadEdge]*ReadEdge

	// WriteEdges contains a mapping of old write edges to their clones.
	WriteEdges map[*WriteEdge]*WriteEdge
}

func newCloner() *cloner {
	return &cloner{
		Nodes:      make(map[Node]Node),
		ReadEdges:  make(map[*ReadEdge]*ReadEdge),
		WriteEdges: make(map[*WriteEdge]*WriteEdge),
	}
}

func (c *cloner) Clone(out *ReadEdge) *ReadEdge {
	// Iterate through the ReadEdge and clone every node.
	v := reflect.ValueOf(out)
	c.cloneGraph(v)
	return c.ReadEdges[out]
}

func (c *cloner) cloneGraph(v reflect.Value) {
	// Clone all of the nodes in the tree. Clone all of the edges, but leave
	// the edges disconnected from their nodes until we are done cloning nodes.
	unvisited := list.New()
	c.clone(v, unvisited)

	for unvisited.Len() > 0 {
		e := unvisited.Front()
		unvisited.Remove(e)

		node := e.Value.(Node)
		if _, ok := c.Nodes[node]; ok {
			continue
		}
		v := reflect.ValueOf(node)
		c.Nodes[node] = c.clone(v, unvisited).Interface().(Node)
	}

	// Connect the created edges to their appropriate node now that everything
	// has been created.
	for old, new := range c.ReadEdges {
		if old.Node != nil {
			new.Node = c.Nodes[old.Node]
		}
	}
	for old, new := range c.WriteEdges {
		if old.Node != nil {
			new.Node = c.Nodes[old.Node]
		}
	}
}

func (c *cloner) clone(v reflect.Value, unvisited *list.List) reflect.Value {
	switch v.Kind() {
	case reflect.Ptr:
		// Check if the thing it points to is a read edge or a write edge.
		if v.IsNil() {
			return v
		}
		clone := c.clone(v.Elem(), unvisited)
		return clone.Addr()
	case reflect.Struct:
		// If the struct is a read or write edge, instantiate a zero version
		// and then record their memory locations.
		if typ := v.Type(); typ.AssignableTo(reflect.TypeOf(ReadEdge{})) {
			other := v.Addr().Interface().(*ReadEdge)
			// Check if this read edge already exists.
			if clone, ok := c.ReadEdges[other]; ok {
				return reflect.ValueOf(clone).Elem()
			}

			// Create a new one.
			clone := &ReadEdge{}
			c.ReadEdges[other] = clone
			if other.Node != nil {
				unvisited.PushBack(other.Node)
			}

			// Clone the corresponding write edge.
			if other.Input != nil {
				input := c.clone(reflect.ValueOf(other.Input), unvisited)
				clone.Input = input.Interface().(*WriteEdge)
			}
			return reflect.ValueOf(clone).Elem()
		} else if typ.AssignableTo(reflect.TypeOf(WriteEdge{})) {
			other := v.Addr().Interface().(*WriteEdge)
			// Check if this write edge already exists.
			if clone, ok := c.WriteEdges[other]; ok {
				return reflect.ValueOf(clone).Elem()
			}

			// Create a new one.
			clone := &WriteEdge{}
			c.WriteEdges[other] = clone
			if other.Node != nil {
				unvisited.PushBack(other.Node)
			}

			// Clone the corresponding write edge.
			if other.Output != nil {
				output := c.clone(reflect.ValueOf(other.Output), unvisited)
				clone.Output = output.Interface().(*ReadEdge)
			}
			return reflect.ValueOf(clone).Elem()
		}

		// Create a new copy of the struct using the old values.
		clone := reflect.New(v.Type()).Elem()
		clone.Set(v)

		// Iterate through all of the fields in the original node
		// and clone it into the new one if it is a read or write edge.
		for i := 0; i < v.NumField(); i++ {
			f := v.Field(i)
			if !f.CanSet() {
				continue
			}
			clone.Field(i).Set(c.clone(f, unvisited))
		}
		return clone
	case reflect.Slice:
		// Construct a new slice for the specified type with the same length.
		clone := reflect.MakeSlice(v.Type(), v.Len(), v.Len())
		for i := 0; i < v.Len(); i++ {
			clone.Index(i).Set(c.clone(v.Index(i), unvisited))
		}
		return clone
	default:
		clone := reflect.New(v.Type()).Elem()
		clone.Set(v)
		return clone
	}
}

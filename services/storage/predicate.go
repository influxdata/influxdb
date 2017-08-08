package storage

// Visitor can be called by Walk to traverse an AST hierarchy.
// The Visit() function is called once per node.
type PredicateVisitor interface {
	Visit(Node) PredicateVisitor
}

type Node interface {
	isNode()
}

func (*Predicate) isNode()                                 {}
func (*Predicate_Expression) isNode()                      {}
func (*Predicate_BooleanExpression) isNode()               {}
func (*Predicate_GroupExpression) isNode()                 {}
func (*Predicate_ValueExpression) isNode()                 {}
func (*Predicate_RefExpression) isNode()                   {}
func (*Predicate_LiteralExpression) isNode()               {}
func (*Predicate_LiteralExpression_StringValue) isNode()   {}
func (*Predicate_LiteralExpression_BooleanValue) isNode()  {}
func (*Predicate_LiteralExpression_IntegerValue) isNode()  {}
func (*Predicate_LiteralExpression_UnsignedValue) isNode() {}
func (*Predicate_LiteralExpression_FloatValue) isNode()    {}
func (*Predicate_LiteralExpression_RegexValue) isNode()    {}

func walkPredicate_Expression(v PredicateVisitor, node *Predicate_Expression) {
	switch val := node.Value.(type) {
	case *Predicate_Expression_BooleanExpression:
		Walk(v, val.BooleanExpression)

	case *Predicate_Expression_GroupExpression:
		Walk(v, val.GroupExpression)
	}
}

func walkPredicate_ValueExpression(v PredicateVisitor, node *Predicate_ValueExpression) {
	switch val := node.Value.(type) {
	case *Predicate_ValueExpression_Ref:
		Walk(v, val.Ref)
	case *Predicate_ValueExpression_Literal:
		Walk(v, val.Literal)
	}
}

func walkPredicate_LiteralExpression(v PredicateVisitor, node *Predicate_LiteralExpression) {
	switch val := node.Value.(type) {
	case *Predicate_LiteralExpression_StringValue:
		Walk(v, val)
	case *Predicate_LiteralExpression_BooleanValue:
		Walk(v, val)
	case *Predicate_LiteralExpression_IntegerValue:
		Walk(v, val)
	case *Predicate_LiteralExpression_UnsignedValue:
		Walk(v, val)
	case *Predicate_LiteralExpression_FloatValue:
		Walk(v, val)
	case *Predicate_LiteralExpression_RegexValue:
		Walk(v, val)
	}
}

func Walk(v PredicateVisitor, node Node) {
	if v = v.Visit(node); v == nil {
		return
	}

	switch n := node.(type) {
	case *Predicate:
		walkPredicate_Expression(v, n.Root)

	case *Predicate_Expression:
		walkPredicate_Expression(v, n)

	case *Predicate_GroupExpression:
		for _, val := range n.Expressions {
			walkPredicate_Expression(v, val)
		}

	case *Predicate_ValueExpression:
		walkPredicate_ValueExpression(v, n)

	case *Predicate_BooleanExpression:
		walkPredicate_ValueExpression(v, n.LHS)
		Walk(v, n.RHS)

	case *Predicate_LiteralExpression:
		walkPredicate_LiteralExpression(v, n)

	case *Predicate_RefExpression:
		// nothing to do
	}
}

package parser

import "fmt"

type QueryType int

const (
	Select QueryType = iota
	Delete
	DropContinuousQuery
	ListSeries
	ListContinuousQueries
	DropSeries
	Continuous
)

func (qt QueryType) String() string {
	switch qt {
	case Select:
		return "select"
	case Delete:
		return "delete"
	case DropContinuousQuery:
		return "drop continuous query"
	case ListSeries:
		return "list series"
	case ListContinuousQueries:
		return "list continuous queries"
	case DropSeries:
		return "drop series"
	case Continuous:
		return "continuous"
	default:
		return fmt.Sprintf("Unknown(%d)", qt)
	}
}

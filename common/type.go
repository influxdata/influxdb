package common

import "fmt"

type Type int

const (
	TYPE_UNKNOWN = iota
	TYPE_INT
	TYPE_STRING
	TYPE_BOOL
	TYPE_DOUBLE
)

func (t Type) String() string {
	switch t {
	case TYPE_INT:
		return "INT"
	case TYPE_STRING:
		return "STRING"
	case TYPE_BOOL:
		return "BOOL"
	case TYPE_DOUBLE:
		return "DOUBLE"
	default:
		panic(fmt.Errorf("Unknown type: %d", t))
	}
}

package influxql

import (
	"encoding/json"
	"errors"

	"github.com/influxdata/influxdb/models"
)

const (
	// WarningLevel is the message level for a warning.
	WarningLevel = "warning"
)

// TagSet is a fundamental concept within the query system. It represents a composite series,
// composed of multiple individual series that share a set of tag attributes.
type TagSet struct {
	Tags       map[string]string
	Filters    []Expr
	SeriesKeys []string
	Key        []byte
}

// AddFilter adds a series-level filter to the Tagset.
func (t *TagSet) AddFilter(key string, filter Expr) {
	t.SeriesKeys = append(t.SeriesKeys, key)
	t.Filters = append(t.Filters, filter)
}

// Message represents a user-facing message to be included with the result.
type Message struct {
	Level string `json:"level"`
	Text  string `json:"text"`
}

// Result represents a resultset returned from a single statement.
// Rows represents a list of rows that can be sorted consistently by name/tag.
type Result struct {
	// StatementID is just the statement's position in the query. It's used
	// to combine statement results if they're being buffered in memory.
	StatementID int `json:"-"`
	Series      models.Rows
	Messages    []*Message
	Err         error
}

// MarshalJSON encodes the result into JSON.
func (r *Result) MarshalJSON() ([]byte, error) {
	// Define a struct that outputs "error" as a string.
	var o struct {
		Series   []*models.Row `json:"series,omitempty"`
		Messages []*Message    `json:"messages,omitempty"`
		Err      string        `json:"error,omitempty"`
	}

	// Copy fields to output struct.
	o.Series = r.Series
	o.Messages = r.Messages
	if r.Err != nil {
		o.Err = r.Err.Error()
	}

	return json.Marshal(&o)
}

// UnmarshalJSON decodes the data into the Result struct
func (r *Result) UnmarshalJSON(b []byte) error {
	var o struct {
		Series   []*models.Row `json:"series,omitempty"`
		Messages []*Message    `json:"messages,omitempty"`
		Err      string        `json:"error,omitempty"`
	}

	err := json.Unmarshal(b, &o)
	if err != nil {
		return err
	}
	r.Series = o.Series
	r.Messages = o.Messages
	if o.Err != "" {
		r.Err = errors.New(o.Err)
	}
	return nil
}

// GetProcessor is a Method that returns processor type and index
// based on the type of expression.
func GetProcessor(expr Expr, startIndex int) (Processor, int) {
	switch expr := expr.(type) {
	case *VarRef:
		return newEchoProcessor(startIndex), startIndex + 1
	case *Call:
		return newEchoProcessor(startIndex), startIndex + 1
	case *BinaryExpr:
		return getBinaryProcessor(expr, startIndex)
	case *ParenExpr:
		return GetProcessor(expr.Expr, startIndex)
	case *NumberLiteral:
		return newLiteralProcessor(expr.Val), startIndex
	case *StringLiteral:
		return newLiteralProcessor(expr.Val), startIndex
	case *BooleanLiteral:
		return newLiteralProcessor(expr.Val), startIndex
	case *TimeLiteral:
		return newLiteralProcessor(expr.Val), startIndex
	case *DurationLiteral:
		return newLiteralProcessor(expr.Val), startIndex
	}
	panic("unreachable")
}

// Processor is a prcessor type returned by GetProcessor
type Processor func(values []interface{}) interface{}

func newEchoProcessor(index int) Processor {
	return func(values []interface{}) interface{} {
		if index > len(values)-1 {
			return nil
		}
		return values[index]
	}
}

func newLiteralProcessor(val interface{}) Processor {
	return func(values []interface{}) interface{} {
		return val
	}
}

func getBinaryProcessor(expr *BinaryExpr, startIndex int) (Processor, int) {
	lhs, index := GetProcessor(expr.LHS, startIndex)
	rhs, index := GetProcessor(expr.RHS, index)

	return newBinaryExprEvaluator(expr.Op, lhs, rhs), index
}

func newBinaryExprEvaluator(op Token, lhs, rhs Processor) Processor {
	switch op {
	case ADD:
		return func(values []interface{}) interface{} {
			l := lhs(values)
			r := rhs(values)
			if lf, rf, ok := processorValuesAsFloat64(l, r); ok {
				return lf + rf
			}
			return nil
		}
	case SUB:
		return func(values []interface{}) interface{} {
			l := lhs(values)
			r := rhs(values)
			if lf, rf, ok := processorValuesAsFloat64(l, r); ok {
				return lf - rf
			}
			return nil
		}
	case MUL:
		return func(values []interface{}) interface{} {
			l := lhs(values)
			r := rhs(values)
			if lf, rf, ok := processorValuesAsFloat64(l, r); ok {
				return lf * rf
			}
			return nil
		}
	case DIV:
		return func(values []interface{}) interface{} {
			l := lhs(values)
			r := rhs(values)
			if lf, rf, ok := processorValuesAsFloat64(l, r); ok {
				return lf / rf
			}
			return nil
		}
	default:
		// we shouldn't get here, but give them back nils if it goes this way
		return func(values []interface{}) interface{} {
			return nil
		}
	}
}

func processorValuesAsFloat64(lhs interface{}, rhs interface{}) (float64, float64, bool) {
	var lf float64
	var rf float64
	var ok bool

	lf, ok = lhs.(float64)
	if !ok {
		var li int64
		if li, ok = lhs.(int64); !ok {
			return 0, 0, false
		}
		lf = float64(li)
	}
	rf, ok = rhs.(float64)
	if !ok {
		var ri int64
		if ri, ok = rhs.(int64); !ok {
			return 0, 0, false
		}
		rf = float64(ri)
	}
	return lf, rf, true
}

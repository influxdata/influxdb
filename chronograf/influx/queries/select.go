package queries

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/influxdata/influxql"
)

type literalJSON struct {
	Expr string `json:"expr"`
	Val  string `json:"val"`
	Type string `json:"type"`
}

func ParseSelect(q string) (*SelectStatement, error) {
	stmt, err := influxql.NewParser(strings.NewReader(q)).ParseStatement()
	if err != nil {
		return nil, err
	}
	s, ok := stmt.(*influxql.SelectStatement)
	if !ok {
		return nil, fmt.Errorf("Error parsing query: not a SELECT statement")
	}
	return &SelectStatement{s}, nil
}

type BinaryExpr struct {
	*influxql.BinaryExpr
}

func (b *BinaryExpr) MarshalJSON() ([]byte, error) {
	octets, err := MarshalJSON(b.BinaryExpr.LHS)
	if err != nil {
		log.Fatalln(err)
		return nil, err
	}
	lhs := json.RawMessage(octets)

	octets, err = MarshalJSON(b.BinaryExpr.RHS)
	if err != nil {
		log.Fatalln(err)
		return nil, err
	}
	rhs := json.RawMessage(octets)

	return json.Marshal(struct {
		Expr string           `json:"expr"`
		Op   string           `json:"op"`
		LHS  *json.RawMessage `json:"lhs"`
		RHS  *json.RawMessage `json:"rhs"`
	}{"binary", b.Op.String(), &lhs, &rhs})
}

type Call struct {
	*influxql.Call
}

func (c *Call) MarshalJSON() ([]byte, error) {
	args := make([]json.RawMessage, len(c.Args))
	for i, arg := range c.Args {
		b, err := MarshalJSON(arg)
		if err != nil {
			return nil, err
		}
		args[i] = b
	}
	return json.Marshal(struct {
		Expr string            `json:"expr"`
		Name string            `json:"name"`
		Args []json.RawMessage `json:"args,omitempty"`
	}{"call", c.Name, args})
}

type Distinct struct {
	*influxql.Distinct
}

func (d *Distinct) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`{"expr": "distinct", "val": "%s"}`, d.Val)), nil
}

type Fill struct {
	Option influxql.FillOption
	Value  interface{}
}

func (f *Fill) MarshalJSON() ([]byte, error) {
	var fill string
	switch f.Option {
	case influxql.NullFill:
		fill = "null"
	case influxql.NoFill:
		fill = "none"
	case influxql.PreviousFill:
		fill = "previous"
	case influxql.LinearFill:
		fill = "linear"
	case influxql.NumberFill:
		fill = fmt.Sprintf("%v", f.Value)
	}
	return json.Marshal(fill)
}

type ParenExpr struct {
	*influxql.ParenExpr
}

func (p *ParenExpr) MarshalJSON() ([]byte, error) {
	expr, err := MarshalJSON(p.Expr)
	if err != nil {
		log.Fatalln(err)
		return nil, err
	}
	return []byte(fmt.Sprintf(`{"expr": "paren", "val": %s}`, expr)), nil
}

func LiteralJSON(lit string, litType string) ([]byte, error) {
	result := literalJSON{
		Expr: "literal",
		Val:  lit,
		Type: litType,
	}
	return json.Marshal(result)
}

type BooleanLiteral struct {
	*influxql.BooleanLiteral
}

func (b *BooleanLiteral) MarshalJSON() ([]byte, error) {
	return LiteralJSON(b.String(), "boolean")
}

type DurationLiteral struct {
	*influxql.DurationLiteral
}

func (d *DurationLiteral) MarshalJSON() ([]byte, error) {
	return LiteralJSON(d.String(), "duration")
}

type IntegerLiteral struct {
	*influxql.IntegerLiteral
}

func (i *IntegerLiteral) MarshalJSON() ([]byte, error) {
	return LiteralJSON(i.String(), "integer")
}

type NumberLiteral struct {
	*influxql.NumberLiteral
}

func (n *NumberLiteral) MarshalJSON() ([]byte, error) {
	return LiteralJSON(n.String(), "number")
}

type RegexLiteral struct {
	*influxql.RegexLiteral
}

func (r *RegexLiteral) MarshalJSON() ([]byte, error) {
	return LiteralJSON(r.String(), "regex")
}

// TODO: I don't think list is right
type ListLiteral struct {
	*influxql.ListLiteral
}

func (l *ListLiteral) MarshalJSON() ([]byte, error) {
	vals := make([]string, len(l.Vals))
	for i, v := range l.Vals {
		vals[i] = fmt.Sprintf(`"%s"`, v)
	}
	list := "[" + strings.Join(vals, ",") + "]"
	return []byte(list), nil
}

type StringLiteral struct {
	*influxql.StringLiteral
}

func (s *StringLiteral) MarshalJSON() ([]byte, error) {
	return LiteralJSON(s.Val, "string")
}

type TimeLiteral struct {
	*influxql.TimeLiteral
}

func (t *TimeLiteral) MarshalJSON() ([]byte, error) {
	return LiteralJSON(t.Val.UTC().Format(time.RFC3339Nano), "time")
}

type VarRef struct {
	*influxql.VarRef
}

func (v *VarRef) MarshalJSON() ([]byte, error) {
	if v.Type != influxql.Unknown {
		return []byte(fmt.Sprintf(`{"expr": "reference", "val": "%s", "type": "%s"}`, v.Val, v.Type.String())), nil
	} else {
		return []byte(fmt.Sprintf(`{"expr": "reference",  "val": "%s"}`, v.Val)), nil
	}
}

type Wildcard struct {
	*influxql.Wildcard
}

func (w *Wildcard) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`{"expr": "wildcard", "val": "%s"}`, w.String())), nil
}

func MarshalJSON(v interface{}) ([]byte, error) {
	switch v.(type) {
	case *influxql.BinaryExpr:
		return json.Marshal(&BinaryExpr{v.(*influxql.BinaryExpr)})
	case *influxql.BooleanLiteral:
		return json.Marshal(&BooleanLiteral{v.(*influxql.BooleanLiteral)})
	case *influxql.Call:
		return json.Marshal(&Call{v.(*influxql.Call)})
	case *influxql.Distinct:
		return json.Marshal(&Distinct{v.(*influxql.Distinct)})
	case *influxql.DurationLiteral:
		return json.Marshal(&DurationLiteral{v.(*influxql.DurationLiteral)})
	case *influxql.IntegerLiteral:
		return json.Marshal(&IntegerLiteral{v.(*influxql.IntegerLiteral)})
	case *influxql.NumberLiteral:
		return json.Marshal(&NumberLiteral{v.(*influxql.NumberLiteral)})
	case *influxql.ParenExpr:
		return json.Marshal(&ParenExpr{v.(*influxql.ParenExpr)})
	case *influxql.RegexLiteral:
		return json.Marshal(&RegexLiteral{v.(*influxql.RegexLiteral)})
	case *influxql.ListLiteral:
		return json.Marshal(&ListLiteral{v.(*influxql.ListLiteral)})
	case *influxql.StringLiteral:
		return json.Marshal(&StringLiteral{v.(*influxql.StringLiteral)})
	case *influxql.TimeLiteral:
		return json.Marshal(&TimeLiteral{v.(*influxql.TimeLiteral)})
	case *influxql.VarRef:
		return json.Marshal(&VarRef{v.(*influxql.VarRef)})
	case *influxql.Wildcard:
		return json.Marshal(&Wildcard{v.(*influxql.Wildcard)})
	default:
		t := reflect.TypeOf(v)
		return nil, fmt.Errorf("Error marshaling query: unknown type %s", t)
	}
}

type Measurement struct {
	Database        string         `json:"database"`
	RetentionPolicy string         `json:"retentionPolicy"`
	Name            string         `json:"name,omitempty"`
	Regex           *regexp.Regexp `json:"regex,omitempty"`
	Type            string         `json:"type"`
}

type Source struct {
	influxql.Source
}

func (s *Source) MarshalJSON() ([]byte, error) {
	switch src := s.Source.(type) {
	case *influxql.Measurement:
		m := Measurement{
			Database:        src.Database,
			RetentionPolicy: src.RetentionPolicy,
			Name:            src.Name,
			Type:            "measurement",
		}
		if src.Regex != nil {
			m.Regex = src.Regex.Val
		}
		return json.Marshal(m)
	default:
		return nil, fmt.Errorf("Error marshaling source.  Subqueries not supported yet")
	}
}

type Sources struct {
	influxql.Sources
}

// TODO: Handle subqueries
func (s *Sources) MarshalJSON() ([]byte, error) {
	srcs := make([]Source, len(s.Sources))
	for i, src := range s.Sources {
		srcs[i] = Source{src}
	}
	return json.Marshal(srcs)
}

type Field struct {
	*influxql.Field
}

func (f *Field) MarshalJSON() ([]byte, error) {
	b, err := MarshalJSON(f.Expr)
	if err != nil {
		return nil, err
	}
	column := json.RawMessage(b)
	return json.Marshal(struct {
		Alias  string           `json:"alias,omitempty"`
		Column *json.RawMessage `json:"column"`
	}{f.Alias, &column})
}

type Fields struct {
	influxql.Fields
}

func (f *Fields) MarshalJSON() ([]byte, error) {
	fields := make([]Field, len(f.Fields))
	for i, field := range f.Fields {
		fields[i] = Field{field}
	}

	return json.Marshal(fields)
}

type Condition struct {
	influxql.Expr
}

func (c *Condition) MarshalJSON() ([]byte, error) {
	return MarshalJSON(c.Expr)
}

type SortField struct {
	*influxql.SortField
}

func (s *SortField) MarshalJSON() ([]byte, error) {
	var order string
	if s.Ascending {
		order = "ascending"
	} else {
		order = "descending"
	}

	return json.Marshal(struct {
		Name  string `json:"name,omitempty"`
		Order string `json:"order,omitempty"`
	}{s.Name, order})
}

type SortFields struct {
	influxql.SortFields
}

func (f *SortFields) MarshalJSON() ([]byte, error) {
	fields := make([]SortField, len(f.SortFields))
	for i, field := range f.SortFields {
		fields[i] = SortField{field}
	}

	return json.Marshal(fields)
}

type Limits struct {
	Limit   int `json:"limit,omitempty"`
	Offset  int `json:"offset,omitempty"`
	SLimit  int `json:"slimit,omitempty"`
	SOffset int `json:"soffset,omitempty"`
}

type SelectStatement struct {
	*influxql.SelectStatement
}

func (s *SelectStatement) MarshalJSON() ([]byte, error) {
	stmt := map[string]interface{}{
		"fields":  &Fields{s.Fields},
		"sources": &Sources{s.Sources},
	}
	if len(s.Dimensions) > 0 {
		stmt["groupBy"] = &Dimensions{s.Dimensions, s.Fill, s.FillValue}
	}
	if s.Condition != nil {
		stmt["condition"] = &Condition{s.Condition}
	}
	if s.Limit != 0 || s.Offset != 0 || s.SLimit != 0 || s.SOffset != 0 {
		stmt["limits"] = &Limits{s.Limit, s.Offset, s.SLimit, s.SOffset}
	}
	if len(s.SortFields) > 0 {
		stmt["orderbys"] = &SortFields{s.SortFields}
	}
	return json.Marshal(stmt)
}

type Dimension struct {
	*influxql.Dimension
}

func (d *Dimension) MarshalJSON() ([]byte, error) {
	switch v := d.Expr.(type) {
	case *influxql.Call:
		if v.Name != "time" {
			return nil, errors.New("time dimension offset function must be now()")
		}
		// Make sure there is exactly one argument.
		if got := len(v.Args); got < 1 || got > 2 {
			return nil, errors.New("time dimension expected 1 or 2 arguments")
		}
		// Ensure the argument is a duration.
		lit, ok := v.Args[0].(*influxql.DurationLiteral)
		if !ok {
			return nil, errors.New("time dimension must have duration argument")
		}
		var offset string
		if len(v.Args) == 2 {
			switch o := v.Args[1].(type) {
			case *influxql.DurationLiteral:
				offset = o.String()
			case *influxql.Call:
				if o.Name != "now" {
					return nil, errors.New("time dimension offset function must be now()")
				} else if len(o.Args) != 0 {
					return nil, errors.New("time dimension offset now() function requires no arguments")
				}
				offset = "now()"
			default:
				return nil, errors.New("time dimension offset must be duration or now()")
			}
		}
		return json.Marshal(struct {
			Interval string `json:"interval"`
			Offset   string `json:"offset,omitempty"`
		}{lit.String(), offset})
	case *influxql.VarRef:
		return json.Marshal(v.Val)
	case *influxql.Wildcard:
		return json.Marshal(v.String())
	case *influxql.RegexLiteral:
		return json.Marshal(v.String())
	}
	return MarshalJSON(d.Expr)
}

type Dimensions struct {
	influxql.Dimensions
	FillOption influxql.FillOption
	FillValue  interface{}
}

func (d *Dimensions) MarshalJSON() ([]byte, error) {
	groupBys := struct {
		Time *json.RawMessage   `json:"time,omitempty"`
		Tags []*json.RawMessage `json:"tags,omitempty"`
		Fill *json.RawMessage   `json:"fill,omitempty"`
	}{}

	for _, dim := range d.Dimensions {
		switch dim.Expr.(type) {
		case *influxql.Call:
			octets, err := json.Marshal(&Dimension{dim})

			if err != nil {
				return nil, err
			}
			time := json.RawMessage(octets)
			groupBys.Time = &time
		default:
			octets, err := json.Marshal(&Dimension{dim})
			if err != nil {
				return nil, err
			}
			tag := json.RawMessage(octets)
			groupBys.Tags = append(groupBys.Tags, &tag)
		}
	}
	if d.FillOption != influxql.NullFill {
		octets, err := json.Marshal(&Fill{d.FillOption, d.FillValue})
		if err != nil {
			return nil, err
		}
		fill := json.RawMessage(octets)
		groupBys.Fill = &fill
	}
	return json.Marshal(groupBys)
}

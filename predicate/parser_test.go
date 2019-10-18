package predicate

import (
	"fmt"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb"
	influxtesting "github.com/influxdata/influxdb/testing"
	"github.com/influxdata/influxql"
)

func TestParseNode(t *testing.T) {
	cases := []struct {
		str  string
		node Node
		err  error
	}{
		{
			str: `               abc="opq" AND gender="male" AND temp=1123`,
			node: LogicalNode{Operator: LogicalAnd, Children: []Node{
				TagRuleNode{Tag: influxdb.Tag{Key: "abc", Value: "opq"}},
				TagRuleNode{Tag: influxdb.Tag{Key: "gender", Value: "male"}},
				TagRuleNode{Tag: influxdb.Tag{Key: "temp", Value: "1123"}},
			}},
		},
		{
			str: ` abc="opq" Or gender="male" OR temp=1123`,
			err: &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  "the logical operator OR is not supported yet at position 11",
			},
		},
		{
			str: ` (t1="v1" and t2="v2") and (t3=v3 and (t4=v4 and t5=v5 and t6=v6))`,
			node: LogicalNode{Operator: LogicalAnd, Children: []Node{
				LogicalNode{Operator: LogicalAnd, Children: []Node{
					TagRuleNode{Tag: influxdb.Tag{Key: "t1", Value: "v1"}},
					TagRuleNode{Tag: influxdb.Tag{Key: "t2", Value: "v2"}},
				}},
				LogicalNode{Operator: LogicalAnd, Children: []Node{
					TagRuleNode{Tag: influxdb.Tag{Key: "t3", Value: "v3"}},
					LogicalNode{Operator: LogicalAnd, Children: []Node{
						TagRuleNode{Tag: influxdb.Tag{Key: "t4", Value: "v4"}},
						TagRuleNode{Tag: influxdb.Tag{Key: "t5", Value: "v5"}},
						TagRuleNode{Tag: influxdb.Tag{Key: "t6", Value: "v6"}},
					}},
				}},
			}},
		},
		{
			str: ` (t1="v1" and t2="v2") and (`,
			err: &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  fmt.Sprintf("extra ( seen"),
			},
		},
		{
			str: ` (t1="v1" and t2="v2"))`,
			err: &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  fmt.Sprintf("extra ) seen"),
			},
		},
	}
	for _, c := range cases {
		node, err := Parse(c.str)
		influxtesting.ErrorsEqual(t, err, c.err)
		if c.err == nil {
			if diff := cmp.Diff(node, c.node); diff != "" {
				t.Errorf("tag rule mismatch:\n  %s", diff)
			}
		}
	}
}

func TestParseTagRule(t *testing.T) {
	cases := []struct {
		str  string
		node TagRuleNode
		err  error
	}{
		{
			str:  `           abc   =         "opq"`,
			node: TagRuleNode{Tag: influxdb.Tag{Key: "abc", Value: "opq"}},
		},
		{
			str: ` abc != "opq"`,
			err: &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  `operator: "!=" at position: 5 is not supported yet`,
			},
		},
		{
			str:  `abc=123`,
			node: TagRuleNode{Tag: influxdb.Tag{Key: "abc", Value: "123"}, Operator: influxdb.Equal},
		},
		{
			str:  `abc=true`,
			node: TagRuleNode{Tag: influxdb.Tag{Key: "abc", Value: "true"}, Operator: influxdb.Equal},
		},
		{
			str:  `abc=false`,
			node: TagRuleNode{Tag: influxdb.Tag{Key: "abc", Value: "false"}, Operator: influxdb.Equal},
		},
		{
			str: `abc!~/^payments\./`,
			err: &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  `operator: "!~" at position: 3 is not supported yet`,
			},
		},
		{
			str: `abc=~/^payments\./`,
			err: &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  `operator: "=~" at position: 3 is not supported yet`,
			},
		},
		{
			str: `abc>1000`,
			err: &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  `invalid operator ">" at position: 3`,
			},
		},
	}
	for _, c := range cases {
		p := new(parser)
		p.sc = influxql.NewScanner(strings.NewReader(c.str))
		tr, err := p.parseTagRuleNode()
		influxtesting.ErrorsEqual(t, err, c.err)
		if c.err == nil {
			if diff := cmp.Diff(tr, c.node); diff != "" {
				t.Errorf("tag rule mismatch:\n  %s", diff)
			}
		}
	}
}

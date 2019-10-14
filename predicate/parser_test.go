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
			str: ` abc="opq" AND gender="male" AND temp=1123`,
			node: LogicalNode{Operator: LogicalAnd, Children: []Node{
				TagRuleNode{Tag: influxdb.Tag{Key: "abc", Value: "opq"}},
				TagRuleNode{Tag: influxdb.Tag{Key: "gender", Value: "male"}},
				TagRuleNode{Tag: influxdb.Tag{Key: "temp", Value: "1123"}},
			}},
		},
		{
			str: ` abc="opq" Or gender="male" OR temp=1123`,
			node: LogicalNode{Operator: LogicalOr, Children: []Node{
				TagRuleNode{Tag: influxdb.Tag{Key: "abc", Value: "opq"}},
				TagRuleNode{Tag: influxdb.Tag{Key: "gender", Value: "male"}},
				TagRuleNode{Tag: influxdb.Tag{Key: "temp", Value: "1123"}},
			}},
		},
		{
			str: ` abc="opq" AND gender="male" OR temp=1123`,
			node: LogicalNode{Operator: LogicalOr, Children: []Node{
				LogicalNode{Operator: LogicalAnd, Children: []Node{
					TagRuleNode{Tag: influxdb.Tag{Key: "abc", Value: "opq"}},
					TagRuleNode{Tag: influxdb.Tag{Key: "gender", Value: "male"}},
				}},
				TagRuleNode{Tag: influxdb.Tag{Key: "temp", Value: "1123"}},
			}},
		},
		{
			str: ` abc="opq" OR gender="male" AND (temp=1123 or name="tom")`,
			node: LogicalNode{Operator: LogicalOr, Children: []Node{
				TagRuleNode{Tag: influxdb.Tag{Key: "abc", Value: "opq"}},
				LogicalNode{Operator: LogicalAnd, Children: []Node{
					TagRuleNode{Tag: influxdb.Tag{Key: "gender", Value: "male"}},
					LogicalNode{Operator: LogicalOr, Children: []Node{
						TagRuleNode{Tag: influxdb.Tag{Key: "temp", Value: "1123"}},
						TagRuleNode{Tag: influxdb.Tag{Key: "name", Value: "tom"}},
					}},
				}},
			}},
		},
		{
			str: ` abc="opq" Or gender="male" AND temp=1123 AND name2="jerry" OR name="tom"`,
			node: LogicalNode{Operator: LogicalOr, Children: []Node{
				TagRuleNode{Tag: influxdb.Tag{Key: "abc", Value: "opq"}},
				LogicalNode{Operator: LogicalAnd, Children: []Node{
					LogicalNode{Operator: LogicalAnd, Children: []Node{
						TagRuleNode{Tag: influxdb.Tag{Key: "gender", Value: "male"}},
						TagRuleNode{Tag: influxdb.Tag{Key: "temp", Value: "1123"}},
					}},
					TagRuleNode{Tag: influxdb.Tag{Key: "name2", Value: "jerry"}},
				}},
				TagRuleNode{Tag: influxdb.Tag{Key: "name", Value: "tom"}},
			}},
		},
		{
			str: ` abc="opq" AND gender="male" OR temp=1123 AND name="tom" OR name2="jerry"`,
			node: LogicalNode{Operator: LogicalOr, Children: []Node{
				LogicalNode{Operator: LogicalAnd, Children: []Node{
					TagRuleNode{Tag: influxdb.Tag{Key: "abc", Value: "opq"}},
					TagRuleNode{Tag: influxdb.Tag{Key: "gender", Value: "male"}},
				}},
				LogicalNode{Operator: LogicalOr, Children: []Node{
					LogicalNode{Operator: LogicalAnd, Children: []Node{
						TagRuleNode{Tag: influxdb.Tag{Key: "temp", Value: "1123"}},
						TagRuleNode{Tag: influxdb.Tag{Key: "name", Value: "tom"}},
					}},
					TagRuleNode{Tag: influxdb.Tag{Key: "name2", Value: "jerry"}},
				}},
			}},
		},
		{
			str: ` (t1="v1" or t2!="v2") and (t3=v3 and (t4!=v4 or t5=v5 and t6=v6))`,
			node: LogicalNode{Operator: LogicalAnd, Children: []Node{
				LogicalNode{Operator: LogicalOr, Children: []Node{
					TagRuleNode{Tag: influxdb.Tag{Key: "t1", Value: "v1"}},
					TagRuleNode{Tag: influxdb.Tag{Key: "t2", Value: "v2"}, Operator: influxdb.NotEqual},
				}},
				LogicalNode{Operator: LogicalAnd, Children: []Node{
					TagRuleNode{Tag: influxdb.Tag{Key: "t3", Value: "v3"}},
					LogicalNode{Operator: LogicalOr, Children: []Node{
						TagRuleNode{Tag: influxdb.Tag{Key: "t4", Value: "v4"}, Operator: influxdb.NotEqual},
						LogicalNode{Operator: LogicalAnd, Children: []Node{
							TagRuleNode{Tag: influxdb.Tag{Key: "t5", Value: "v5"}},
							TagRuleNode{Tag: influxdb.Tag{Key: "t6", Value: "v6"}},
						}},
					}},
				}},
			}},
		},
		{
			str: ` (t1="v1" or t2!="v2") and (`,
			err: &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  fmt.Sprintf("extra ( seen"),
			},
		},
		{
			str: ` (t1="v1" or t2!="v2"))`,
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
			str:  ` abc="opq"`,
			node: TagRuleNode{Tag: influxdb.Tag{Key: "abc", Value: "opq"}},
		},
		{
			str:  ` abc != "opq"`,
			node: TagRuleNode{Tag: influxdb.Tag{Key: "abc", Value: "opq"}, Operator: influxdb.NotEqual},
		},
		{
			str:  `abc!=123`,
			node: TagRuleNode{Tag: influxdb.Tag{Key: "abc", Value: "123"}, Operator: influxdb.NotEqual},
		},
		{
			str:  `abc!=true`,
			node: TagRuleNode{Tag: influxdb.Tag{Key: "abc", Value: "true"}, Operator: influxdb.NotEqual},
		},
		{
			str:  `abc=false`,
			node: TagRuleNode{Tag: influxdb.Tag{Key: "abc", Value: "false"}, Operator: influxdb.Equal},
		},
		{
			str:  `abc!~/^payments\./`,
			node: TagRuleNode{Tag: influxdb.Tag{Key: "abc", Value: `/^payments\./`}, Operator: influxdb.NotRegexEqual},
		},
		{
			str: `abc=~/^bad\\\\/`,
			err: &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  "bad regex at position: 4",
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

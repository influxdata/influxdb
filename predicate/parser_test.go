package predicate

import (
	"strings"
	"testing"

	"github.com/influxdata/influxdb/v2/kit/platform/errors"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	influxtesting "github.com/influxdata/influxdb/v2/testing"
	"github.com/influxdata/influxql"
)

func TestParseNode(t *testing.T) {
	cases := []struct {
		str  string
		node Node
		err  error
	}{
		{
			str:  `abc=opq`,
			node: TagRuleNode{Tag: influxdb.Tag{Key: "abc", Value: "opq"}},
		},
		{
			str: `abc=opq and gender="male"`,
			node: LogicalNode{Operator: LogicalAnd, Children: [2]Node{
				TagRuleNode{Tag: influxdb.Tag{Key: "abc", Value: "opq"}},
				TagRuleNode{Tag: influxdb.Tag{Key: "gender", Value: "male"}},
			}},
		},
		{
			str: `               abc="opq" AND gender="male" AND temp=1123`,
			node: LogicalNode{Operator: LogicalAnd, Children: [2]Node{
				LogicalNode{Operator: LogicalAnd, Children: [2]Node{
					TagRuleNode{Tag: influxdb.Tag{Key: "abc", Value: "opq"}},
					TagRuleNode{Tag: influxdb.Tag{Key: "gender", Value: "male"}},
				}},
				TagRuleNode{Tag: influxdb.Tag{Key: "temp", Value: "1123"}},
			}},
		},
		{
			str: ` abc="opq" Or gender="male" OR temp=1123`,
			err: &errors.Error{
				Code: errors.EInvalid,
				Msg:  "the logical operator OR is not supported yet at position 11",
			},
		},
		{
			str: ` (t1="v1" and t2="v2") and (t3=v3 and (t4=v4 and t5=v5 and t6=v6))`,
			node: LogicalNode{Operator: LogicalAnd, Children: [2]Node{
				LogicalNode{Operator: LogicalAnd, Children: [2]Node{
					TagRuleNode{Tag: influxdb.Tag{Key: "t1", Value: "v1"}},
					TagRuleNode{Tag: influxdb.Tag{Key: "t2", Value: "v2"}},
				}},
				LogicalNode{Operator: LogicalAnd, Children: [2]Node{
					TagRuleNode{Tag: influxdb.Tag{Key: "t3", Value: "v3"}},
					LogicalNode{Operator: LogicalAnd, Children: [2]Node{
						LogicalNode{Operator: LogicalAnd, Children: [2]Node{
							TagRuleNode{Tag: influxdb.Tag{Key: "t4", Value: "v4"}},
							TagRuleNode{Tag: influxdb.Tag{Key: "t5", Value: "v5"}},
						}},
						TagRuleNode{Tag: influxdb.Tag{Key: "t6", Value: "v6"}},
					}},
				}},
			}},
		},
		{
			str: ` (t1="v1" and t2="v2") and (`,
			err: &errors.Error{
				Code: errors.EInvalid,
				Msg:  "extra ( seen",
			},
		},
		{
			str: ` (t1="v1" and t2="v2"))`,
			err: &errors.Error{
				Code: errors.EInvalid,
				Msg:  "extra ) seen",
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
			str:  `abc=0x1231`,
			node: TagRuleNode{Tag: influxdb.Tag{Key: "abc", Value: "0x1231"}},
		},
		{
			str:  `abc=2d`,
			node: TagRuleNode{Tag: influxdb.Tag{Key: "abc", Value: "2d"}},
		},
		{
			str:  `abc=-5i`,
			node: TagRuleNode{Tag: influxdb.Tag{Key: "abc", Value: "-5i"}},
		},
		{
			str:  `abc= -1221`,
			node: TagRuleNode{Tag: influxdb.Tag{Key: "abc", Value: "-1221"}},
		},
		{
			str:  ` abc != "opq"`,
			node: TagRuleNode{Tag: influxdb.Tag{Key: "abc", Value: "opq"}, Operator: influxdb.NotEqual},
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
			err: &errors.Error{
				Code: errors.EInvalid,
				Msg:  `operator: "!~" at position: 3 is not supported yet`,
			},
		},
		{
			str: `abc=~/^payments\./`,
			err: &errors.Error{
				Code: errors.EInvalid,
				Msg:  `operator: "=~" at position: 3 is not supported yet`,
			},
		},
		{
			str: `abc>1000`,
			err: &errors.Error{
				Code: errors.EInvalid,
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

package statement

import (
	//	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb/stress/v2/statement"
)

func newParserFromString(s string) *Parser {
	f := strings.NewReader(s)
	p := NewParser(f)

	return p
}

func TestParser_ParseStatement(t *testing.T) {
	var tests = []struct {
		skip bool
		s    string
		stmt statement.Statement
		err  string
	}{

		// QUERY

		{
			s:    "QUERY basicCount\nSELECT count(%f) FROM cpu\nDO 100",
			stmt: &statement.QueryStatement{Name: "basicCount", TemplateString: "SELECT count(%v) FROM cpu", Args: []string{"%f"}, Count: 100},
		},

		{
			s:    "QUERY basicCount\nSELECT count(%f) FROM %m\nDO 100",
			stmt: &statement.QueryStatement{Name: "basicCount", TemplateString: "SELECT count(%v) FROM %v", Args: []string{"%f", "%m"}, Count: 100},
		},

		{
			skip: true, // SHOULD CAUSE AN ERROR
			s:    "QUERY\nSELECT count(%f) FROM %m\nDO 100",
			err:  "Missing Name",
		},

		// INSERT

		{
			s: "INSERT mockCpu\ncpu,\nhost=[us-west|us-east|eu-north],server_id=[str rand(7) 1000]\nbusy=[int rand(1000) 100],free=[float rand(10) 0]\n100000 10s",
			stmt: &statement.InsertStatement{
				Name:           "mockCpu",
				TemplateString: "cpu,host=%v,server_id=%v busy=%v,free=%v %v",
				TagCount:       2,
				Templates: []*statement.Template{
					&statement.Template{
						Tags: []string{"us-west", "us-east", "eu-north"},
					},
					&statement.Template{
						Function: &statement.Function{Type: "str", Fn: "rand", Argument: 7, Count: 1000},
					},
					&statement.Template{
						Function: &statement.Function{Type: "int", Fn: "rand", Argument: 1000, Count: 100},
					},
					&statement.Template{
						Function: &statement.Function{Type: "float", Fn: "rand", Argument: 10, Count: 0},
					},
				},
				Timestamp: &statement.Timestamp{
					Count:    100000,
					Duration: time.Duration(10 * time.Second),
				},
			},
		},

		{
			s: "INSERT mockCpu\ncpu,host=[us-west|us-east|eu-north],server_id=[str rand(7) 1000]\nbusy=[int rand(1000) 100],free=[float rand(10) 0]\n100000 10s",
			stmt: &statement.InsertStatement{
				Name:           "mockCpu",
				TemplateString: "cpu,host=%v,server_id=%v busy=%v,free=%v %v",
				TagCount:       2,
				Templates: []*statement.Template{
					&statement.Template{
						Tags: []string{"us-west", "us-east", "eu-north"},
					},
					&statement.Template{
						Function: &statement.Function{Type: "str", Fn: "rand", Argument: 7, Count: 1000},
					},
					&statement.Template{
						Function: &statement.Function{Type: "int", Fn: "rand", Argument: 1000, Count: 100},
					},
					&statement.Template{
						Function: &statement.Function{Type: "float", Fn: "rand", Argument: 10, Count: 0},
					},
				},
				Timestamp: &statement.Timestamp{
					Count:    100000,
					Duration: time.Duration(10 * time.Second),
				},
			},
		},

		{
			s: "INSERT mockCpu\n[str rand(1000) 10],\nhost=[us-west|us-east|eu-north],server_id=[str rand(7) 1000],other=x\nbusy=[int rand(1000) 100],free=[float rand(10) 0]\n100000 10s",
			stmt: &statement.InsertStatement{
				Name:           "mockCpu",
				TemplateString: "%v,host=%v,server_id=%v,other=x busy=%v,free=%v %v",
				TagCount:       3,
				Templates: []*statement.Template{
					&statement.Template{
						Function: &statement.Function{Type: "str", Fn: "rand", Argument: 1000, Count: 10},
					},
					&statement.Template{
						Tags: []string{"us-west", "us-east", "eu-north"},
					},
					&statement.Template{
						Function: &statement.Function{Type: "str", Fn: "rand", Argument: 7, Count: 1000},
					},
					&statement.Template{
						Function: &statement.Function{Type: "int", Fn: "rand", Argument: 1000, Count: 100},
					},
					&statement.Template{
						Function: &statement.Function{Type: "float", Fn: "rand", Argument: 10, Count: 0},
					},
				},
				Timestamp: &statement.Timestamp{
					Count:    100000,
					Duration: time.Duration(10 * time.Second),
				},
			},
		},

		{
			skip: true, // Expected error not working
			s:    "INSERT\ncpu,\nhost=[us-west|us-east|eu-north],server_id=[str rand(7) 1000]\nbusy=[int rand(1000) 100],free=[float rand(10) 0]\n100000 10s",
			err:  `found ",", expected WS`,
		},

		// EXEC

		{
			s:    `EXEC other_script`,
			stmt: &statement.ExecStatement{Script: "other_script"},
		},

		{
			skip: true, // Implement
			s:    `EXEC other_script.sh`,
			stmt: &statement.ExecStatement{Script: "other_script.sh"},
		},

		{
			skip: true, // Implement
			s:    `EXEC ../other_script.sh`,
			stmt: &statement.ExecStatement{Script: "../other_script.sh"},
		},

		{
			skip: true, // Implement
			s:    `EXEC /path/to/some/other_script.sh`,
			stmt: &statement.ExecStatement{Script: "/path/to/some/other_script.sh"},
		},

		// GO

		{
			skip: true,
			s:    "GO INSERT mockCpu\ncpu,\nhost=[us-west|us-east|eu-north],server_id=[str rand(7) 1000]\nbusy=[int rand(1000) 100],free=[float rand(10) 0]\n100000 10s",
			stmt: &statement.GoStatement{
				Statement: &statement.InsertStatement{
					Name:           "mockCpu",
					TemplateString: "cpu,host=%v,server_id=%v busy=%v,free=%v %v",
					Templates: []*statement.Template{
						&statement.Template{
							Tags: []string{"us-west", "us-east", "eu-north"},
						},
						&statement.Template{
							Function: &statement.Function{Type: "str", Fn: "rand", Argument: 7, Count: 1000},
						},
						&statement.Template{
							Function: &statement.Function{Type: "int", Fn: "rand", Argument: 1000, Count: 100},
						},
						&statement.Template{
							Function: &statement.Function{Type: "float", Fn: "rand", Argument: 10, Count: 0},
						},
					},
					Timestamp: &statement.Timestamp{
						Count:    100000,
						Duration: time.Duration(10 * time.Second),
					},
				},
			},
		},

		{
			skip: true,
			s:    "GO QUERY basicCount\nSELECT count(free) FROM cpu\nDO 100",
			stmt: &statement.GoStatement{
				Statement: &statement.QueryStatement{Name: "basicCount", TemplateString: "SELECT count(free) FROM cpu", Count: 100},
			},
		},

		{
			skip: true,
			s:    `GO EXEC other_script`,
			stmt: &statement.GoStatement{
				Statement: &statement.ExecStatement{Script: "other_script"},
			},
		},

		// SET

		{
			s:    `SET database [stress]`,
			stmt: &statement.SetStatement{Var: "database", Value: "stress"},
		},

		// WAIT

		{
			s:    `Wait`,
			stmt: &statement.WaitStatement{},
		},
	}

	for _, tst := range tests {

		if tst.skip {
			continue
		}

		stmt, err := newParserFromString(tst.s).Parse()
		tst.stmt.SetID("x")

		if err != nil && err.Error() != tst.err {
			t.Errorf("REAL ERROR: %v\nExpected ERROR: %v\n", err, tst.err)
		} else if err != nil && tst.err == err.Error() {
			t.Errorf("REAL ERROR: %v\nExpected ERROR: %v\n", err, tst.err)
		} else if stmt.SetID("x"); !reflect.DeepEqual(stmt, tst.stmt) {
			t.Errorf("Expected\n%#v\n%#v", tst.stmt, stmt)
		}
	}

}

//func TestBasicQuery(t *testing.T) {
//	s := ""
//
//	qry := &statement.QueryStatement{
//		Name:           "basicCount",
//		TemplateString: "SELECT count(free) FROM cpu",
//		Count:          "100",
//		Args:           []string(nil),
//	}
//	p := newParserFromString(s)
//	statement, err := p.ParseQueryStatement()
//	if err != nil {
//		t.Error(err)
//	}
//	if qry.Name != statement.Name {
//		t.Errorf("Failed parsing Query name\nEXPECTED: %#v\nGOT: %#v", qry.Name, statement.Name)
//	}
//	if qry.TemplateString != statement.TemplateString {
//		t.Errorf("Failed parsing Query template string\nEXPECTED: %#v\nGOT: %#v", qry.TemplateString, statement.TemplateString)
//	}
//	if qry.Count != statement.Count {
//		t.Errorf("Failed parsing Query count\nEXPECTED: %#v\nGOT: %#v", qry.Count, statement.Count)
//	}
//	if qry.Args != []string(nil) {
//		t.Errorf("Failed parsing Query args\nEXPECTED: %#v\nGOT: %#v", qry.Count, statement.Count)
//	}
//}
//
//func TestTemplateQuery(t *testing.T) {
//	s := `QUERY mockCpu
//SELECT mean(%f) FROM %m WHERE %t
//DO 10000`
//
//	qry := &statement.QueryStatement{
//		Name:           "mockCpu",
//		TemplateString: "SELECT mean(%v) FROM %v WHERE %v",
//		Count:          "10000",
//		Args:           []string(nil),
//	}
//	p := newParserFromString(s)
//	statement, err := p.ParseQueryStatement()
//	if err != nil {
//		t.Error(err)
//	}
//	if qry.Name != statement.Name {
//		t.Errorf("Failed parsing Query name\nEXPECTED: %#v\nGOT: %#v", qry.Name, statement.Name)
//	}
//	if qry.TemplateString != statement.TemplateString {
//		t.Errorf("Failed parsing Query template string\nEXPECTED: %#v\nGOT: %#v", qry.TemplateString, statement.TemplateString)
//	}
//	if qry.Count != statement.Count {
//		t.Errorf("Failed parsing Query count\nEXPECTED: %#v\nGOT: %#v", qry.Count, statement.Count)
//	}
//	if qry.Args != []string(nil) {
//		t.Errorf("Failed parsing Query args\nEXPECTED: %#v\nGOT: %#v", qry.Count, statement.Count)
//	}
//}
//
//func TestBasicSet(t *testing.T) {
//	s := `SET database stress`
//	set := &statement.SetStatement{
//		Var:   "database",
//		Value: "stress",
//	}
//	p := newParserFromString(s)
//	statement, err := p.ParseSetStatement()
//	if err != nil {
//		t.Error(err)
//	}
//	if set.Var != statement.Var {
//		t.Errorf("Failed parsing Set var\nEXPECTED: %#v\nGOT: %#v", set.Var, statement.Var)
//	}
//	if set.Value != statement.Value {
//		t.Errorf("Failed parsing Set value\nEXPECTED: %#v\nGOT: %#v", set.Value, statement.Value)
//	}
//}
//
//func TestTemplateInsert(t *testing.T) {
//	s := `INSERT mockCpu
//cpu,
//host=[us-west|us-east|eu-north],server_id=[str rand(7) 1000]
//busy=[int rand(1000) 100],free=[float rand(10) 0]
//100000 10s`
//	tmpl1 := &statement.Template{Tags: []string{"us-west", "us-east", "eu-north"}}
//	tmpl2 := &statement.Template{Function: &statement.Function{Type: "str", Fn: "rand", Argument: "7", Count: "1000"}}
//	tmpl3 := &statement.Template{Function: &statement.Function{Type: "int", Fn: "rand", Argument: "1000", Count: "100"}}
//	tmpl4 := &statement.Template{Function: &statement.Function{Type: "float", Fn: "rand", Argument: "10", Count: "0"}}
//	insert := &statement.InsertStatement{Name: "mockCpu", TemplateString: `cpu,host=%v,server_id=%v busy=%v,free=%v %v`, Templates: []*statement.Template{tmpl1, tmpl2, tmpl3, tmpl4}}
//	p := newParserFromString(s)
//	statement, err := p.ParseInsertStatement()
//	if err != nil {
//		t.Error(err)
//	}
//	if insert.Name != statement.Name {
//		t.Errorf("Failed parsing Insert name\nEXPECTED: %#v\nGOT: %#v", insert.Name, statement.Name)
//	}
//	if insert.TemplateString != statement.TemplateString {
//		t.Errorf("Failed parsing Insert name\nEXPECTED: %#v\nGOT: %#v", insert.TemplateString, statement.TemplateString)
//	}
//	if len(statement.Templates) != 4 {
//		t.Errorf("Expected %v template strings got %v", len(insert.Templates), len(statement.Templates))
//	}
//	for i, stmpl := range statement.Templates {
//		tmpl := insert.Templates[i]
//		if stmpl.Tags != nil {
//			if tmpl.Tags[0] != stmpl.Tags[0] {
//				t.Errorf("Expected explicit tag %v got %v", tmpl.Tags[0], stmpl.Tags[0])
//			}
//		}
//		if tmpl.Function != nil {
//			fn := tmpl.Function
//			sfn := stmpl.Function
//			if sfn.Type != fn.Type {
//				t.Errorf("Expected function of type %v got %v", fn.Type, sfn.Type)
//			}
//			if sfn.Fn != fn.Fn {
//				t.Errorf("Expected function of type %v got %v", fn.Fn, sfn.Fn)
//			}
//			if sfn.Argument != fn.Argument {
//				t.Errorf("Expected function of type %v got %v", fn.Argument, sfn.Argument)
//			}
//			if sfn.Count != fn.Count {
//				t.Errorf("Expected function of type %v got %v", fn.Count, sfn.Count)
//			}
//		}
//	}
//}
//
////func TestGoInsertTemplates(t *testing.T) {
////	s := `GO INSERT template
////[mem|other|thing],
////host=[us-west|us-east|eu-north],server_id=[str rand(7) 1000]
////busy=[int rand(1000) 100],free=[float rand(10) 0]
////100000 10s`
////	tmpl1 := &statement.Template{Tags: []string{"mem", "other", "thing"}}
////	tmpl2 := &statement.Template{Tags: []string{"us-west", "us-east", "eu-north"}}
////	tmpl3 := &statement.Template{Function: &statement.Function{Type: "str", Fn: "rand", Argument: "7", Count: "1000"}}
////	tmpl4 := &statement.Template{Function: &statement.Function{Type: "int", Fn: "rand", Argument: "1000", Count: "100"}}
////	tmpl5 := &statement.Template{Function: &statement.Function{Type: "float", Fn: "rand", Argument: "10", Count: "0"}}
////	goInsert := &statement.GoStatement{Statement: &statement.InsertStatement{
////		Name:           "template",
////		TemplateString: "%v,host=%v,server_id=%v busy=%v,free=%v %v",
////		Templates:      []*statement.Template{tmpl1, tmpl2, tmpl3, tmpl4, tmpl5},
////	}}
////	p := newParserFromString(s)
////	statement, err := p.ParseGoStatement()
////	if err != nil {
////		t.Error(err)
////	}
////	insert := goInsert.Statement.(*statement.InsertStatement)
////	smt := statement.Statement.(*statement.InsertStatement)
////	fmt.Println(insert, smt)
////}

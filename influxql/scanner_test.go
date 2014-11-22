package influxql_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/influxdb/influxdb/influxql"
)

// Ensure the scanner can scan tokens correctly.
func TestScanner_Scan(t *testing.T) {
	var tests = []struct {
		s   string
		tok influxql.Token
		lit string
		pos influxql.Pos
	}{
		// Special tokens (EOF, ILLEGAL, WS)
		{s: ``, tok: influxql.EOF},
		{s: `#`, tok: influxql.ILLEGAL, lit: `#`},
		{s: ` `, tok: influxql.WS, lit: " "},
		{s: "\t", tok: influxql.WS, lit: "\t"},
		{s: "\n", tok: influxql.WS, lit: "\n"},
		{s: "\r", tok: influxql.WS, lit: "\n"},
		{s: "\r\n", tok: influxql.WS, lit: "\n"},
		{s: "\rX", tok: influxql.WS, lit: "\n"},
		{s: "\n\r", tok: influxql.WS, lit: "\n\n"},
		{s: " \n\t \r\n\t", tok: influxql.WS, lit: " \n\t \n\t"},
		{s: " foo", tok: influxql.WS, lit: " "},

		// Numeric operators
		{s: `+`, tok: influxql.ADD},
		{s: `-`, tok: influxql.SUB},
		{s: `*`, tok: influxql.MUL},
		{s: `/`, tok: influxql.DIV},

		// Logical operators
		{s: `AND`, tok: influxql.AND},
		{s: `and`, tok: influxql.AND},
		{s: `OR`, tok: influxql.OR},
		{s: `or`, tok: influxql.OR},

		{s: `=`, tok: influxql.EQ},
		{s: `!=`, tok: influxql.NEQ},
		{s: `! `, tok: influxql.ILLEGAL, lit: "!"},
		{s: `<`, tok: influxql.LT},
		{s: `<=`, tok: influxql.LTE},
		{s: `>`, tok: influxql.GT},
		{s: `>=`, tok: influxql.GTE},

		// Misc tokens
		{s: `(`, tok: influxql.LPAREN},
		{s: `)`, tok: influxql.RPAREN},
		{s: `,`, tok: influxql.COMMA},
		{s: `;`, tok: influxql.SEMICOLON},

		// Identifiers
		{s: `foo`, tok: influxql.IDENT, lit: `foo`},
		{s: `Zx12_3U_-`, tok: influxql.IDENT, lit: `Zx12_3U_`},

		{s: `true`, tok: influxql.TRUE},
		{s: `false`, tok: influxql.FALSE},

		// Strings
		{s: `"testing 123!"`, tok: influxql.STRING, lit: `testing 123!`},
		{s: `"foo\nbar"`, tok: influxql.STRING, lit: "foo\nbar"},
		{s: `"foo\\bar"`, tok: influxql.STRING, lit: "foo\\bar"},
		{s: `"test`, tok: influxql.BADSTRING, lit: `test`},
		{s: "\"test\nfoo", tok: influxql.BADSTRING, lit: `test`},
		{s: `"test\g"`, tok: influxql.BADESCAPE, lit: `\g`, pos: influxql.Pos{Line: 0, Char: 5}},

		// Numbers
		{s: `100`, tok: influxql.NUMBER, lit: `100`},
		{s: `100.23`, tok: influxql.NUMBER, lit: `100.23`},
		{s: `+100.23`, tok: influxql.NUMBER, lit: `+100.23`},
		{s: `-100.23`, tok: influxql.NUMBER, lit: `-100.23`},
		{s: `-100.`, tok: influxql.NUMBER, lit: `-100`},
		{s: `.23`, tok: influxql.NUMBER, lit: `.23`},
		{s: `+.23`, tok: influxql.NUMBER, lit: `+.23`},
		{s: `-.23`, tok: influxql.NUMBER, lit: `-.23`},
		{s: `.`, tok: influxql.ILLEGAL, lit: `.`},
		{s: `-.`, tok: influxql.SUB, lit: ``},
		{s: `+.`, tok: influxql.ADD, lit: ``},
		{s: `10.3s`, tok: influxql.NUMBER, lit: `10.3`},

		// Durations
		{s: `10u`, tok: influxql.DURATION, lit: `10u`},
		{s: `10µ`, tok: influxql.DURATION, lit: `10µ`},
		{s: `10ms`, tok: influxql.DURATION, lit: `10ms`},
		{s: `-1s`, tok: influxql.DURATION, lit: `-1s`},
		{s: `10m`, tok: influxql.DURATION, lit: `10m`},
		{s: `10h`, tok: influxql.DURATION, lit: `10h`},
		{s: `10d`, tok: influxql.DURATION, lit: `10d`},
		{s: `10w`, tok: influxql.DURATION, lit: `10w`},
		{s: `10x`, tok: influxql.NUMBER, lit: `10`}, // non-duration unit

		// Keywords
		{s: `AS`, tok: influxql.AS},
		{s: `ASC`, tok: influxql.ASC},
		{s: `BY`, tok: influxql.BY},
		{s: `CONTINUOUS`, tok: influxql.CONTINUOUS},
		{s: `DELETE`, tok: influxql.DELETE},
		{s: `DESC`, tok: influxql.DESC},
		{s: `DROP`, tok: influxql.DROP},
		{s: `EXPLAIN`, tok: influxql.EXPLAIN},
		{s: `FROM`, tok: influxql.FROM},
		{s: `INNER`, tok: influxql.INNER},
		{s: `INSERT`, tok: influxql.INSERT},
		{s: `INTO`, tok: influxql.INTO},
		{s: `JOIN`, tok: influxql.JOIN},
		{s: `LIMIT`, tok: influxql.LIMIT},
		{s: `LIST`, tok: influxql.LIST},
		{s: `MERGE`, tok: influxql.MERGE},
		{s: `ORDER`, tok: influxql.ORDER},
		{s: `QUERIES`, tok: influxql.QUERIES},
		{s: `SELECT`, tok: influxql.SELECT},
		{s: `SERIES`, tok: influxql.SERIES},
		{s: `WHERE`, tok: influxql.WHERE},
		{s: `explain`, tok: influxql.EXPLAIN}, // case insensitive
	}

	for i, tt := range tests {
		s := influxql.NewScanner(strings.NewReader(tt.s))
		tok, pos, lit := s.Scan()
		if tt.tok != tok {
			t.Errorf("%d. %q token mismatch: exp=%q got=%q <%q>", i, tt.s, tt.tok, tok, lit)
		} else if tt.pos.Line != pos.Line || tt.pos.Char != pos.Char {
			t.Errorf("%d. %q pos mismatch: exp=%#v got=%#v", i, tt.s, tt.pos, pos)
		} else if tt.lit != lit {
			t.Errorf("%d. %q literal mismatch: exp=%q got=%q", i, tt.s, tt.lit, lit)
		}
	}
}

// Ensure the scanner can scan a series of tokens correctly.
func TestScanner_Scan_Multi(t *testing.T) {
	type result struct {
		tok influxql.Token
		pos influxql.Pos
		lit string
	}
	exp := []result{
		{tok: influxql.SELECT, pos: influxql.Pos{Line: 0, Char: 0}, lit: ""},
		{tok: influxql.WS, pos: influxql.Pos{Line: 0, Char: 6}, lit: " "},
		{tok: influxql.IDENT, pos: influxql.Pos{Line: 0, Char: 7}, lit: "value"},
		{tok: influxql.WS, pos: influxql.Pos{Line: 0, Char: 12}, lit: " "},
		{tok: influxql.FROM, pos: influxql.Pos{Line: 0, Char: 13}, lit: ""},
		{tok: influxql.WS, pos: influxql.Pos{Line: 0, Char: 17}, lit: " "},
		{tok: influxql.IDENT, pos: influxql.Pos{Line: 0, Char: 18}, lit: "myseries"},
		{tok: influxql.WS, pos: influxql.Pos{Line: 0, Char: 26}, lit: " "},
		{tok: influxql.WHERE, pos: influxql.Pos{Line: 0, Char: 27}, lit: ""},
		{tok: influxql.WS, pos: influxql.Pos{Line: 0, Char: 32}, lit: " "},
		{tok: influxql.IDENT, pos: influxql.Pos{Line: 0, Char: 33}, lit: "a"},
		{tok: influxql.WS, pos: influxql.Pos{Line: 0, Char: 34}, lit: " "},
		{tok: influxql.EQ, pos: influxql.Pos{Line: 0, Char: 35}, lit: ""},
		{tok: influxql.WS, pos: influxql.Pos{Line: 0, Char: 36}, lit: " "},
		{tok: influxql.STRING, pos: influxql.Pos{Line: 0, Char: 37}, lit: "b"},
		{tok: influxql.EOF, pos: influxql.Pos{Line: 0, Char: 40}, lit: ""},
	}

	// Create a scanner.
	v := `SELECT value from myseries WHERE a = "b"`
	s := influxql.NewScanner(strings.NewReader(v))

	// Continually scan until we reach the end.
	var act []result
	for {
		tok, pos, lit := s.Scan()
		act = append(act, result{tok, pos, lit})
		if tok == influxql.EOF {
			break
		}
	}

	// Verify the token counts match.
	if len(exp) != len(act) {
		t.Fatalf("token count mismatch: exp=%d, got=%d", len(exp), len(act))
	}

	// Verify each token matches.
	for i := range exp {
		if !reflect.DeepEqual(exp[i], act[i]) {
			t.Fatalf("%d. token mismatch:\n\nexp=%#v\n\ngot=%#v", i, exp[i], act[i])
		}
	}
}

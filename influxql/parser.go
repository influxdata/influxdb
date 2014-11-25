package influxql

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

// Parser represents an InfluxQL parser.
type Parser struct {
	s *bufScanner
}

// NewParser returns a new instance of Parsr.
func NewParser(r io.Reader) *Parser {
	return &Parser{s: newBufScanner(r)}
}

// ParseQuery parses an InfluxQL string and returns a Query AST object.
func (p *Parser) ParseQuery() (*Query, error) {
	// If there's only whitespace then return no statements.
	if tok, _, _ := p.scanIgnoreWhitespace(); tok == EOF {
		return &Query{}, nil
	}
	p.unscan()

	// Otherwise parse statements until EOF.
	var statements Statements
	for {

		// Read the next statement.
		s, err := p.ParseStatement()
		if err != nil {
			return nil, err
		}
		statements = append(statements, s)

		// Expect a semicolon or EOF after the statement.
		if tok, pos, lit := p.scanIgnoreWhitespace(); tok != SEMICOLON && tok != EOF {
			return nil, newParseError(tokstr(tok, lit), []string{";", "EOF"}, pos)
		} else if tok == EOF {
			break
		}
	}

	return &Query{Statements: statements}, nil
}

// ParseStatement parses an InfluxQL string and returns a Statement AST object.
func (p *Parser) ParseStatement() (Statement, error) {
	// Inspect the first token.
	tok, pos, lit := p.scanIgnoreWhitespace()
	switch tok {
	case SELECT:
		return p.parseSelectStatement()
	case DELETE:
		return p.parseDeleteStatement()
	case LIST:
		if tok, pos, lit := p.scanIgnoreWhitespace(); tok == SERIES {
			return p.parseListSeriesStatement()
		} else if tok == CONTINUOUS {
			return p.parseListContinuousQueriesStatement()
		} else {
			return nil, newParseError(tokstr(tok, lit), []string{"SERIES", "CONTINUOUS"}, pos)
		}
	case CREATE:
		if tok, pos, lit := p.scanIgnoreWhitespace(); tok == CONTINUOUS {
			return p.parseCreateContinuousQueryStatement()
		} else {
			return nil, newParseError(tokstr(tok, lit), []string{"CONTINUOUS"}, pos)
		}
	case DROP:
		if tok, pos, lit := p.scanIgnoreWhitespace(); tok == SERIES {
			return p.parseDropSeriesStatement()
		} else if tok == CONTINUOUS {
			return p.parseDropContinuousQueryStatement()
		} else {
			return nil, newParseError(tokstr(tok, lit), []string{"SERIES", "CONTINUOUS"}, pos)
		}
	default:
		return nil, newParseError(tokstr(tok, lit), []string{"SELECT"}, pos)
	}
}

// parseSelectStatement parses a select string and returns a Statement AST object.
// This function assumes the SELECT token has already been consumed.
func (p *Parser) parseSelectStatement() (*SelectStatement, error) {
	stmt := &SelectStatement{}

	// Parse fields: "SELECT FIELD+".
	fields, err := p.parseFields()
	if err != nil {
		return nil, err
	}
	stmt.Fields = fields

	// Parse source: "FROM IDENT".
	source, err := p.parseSource()
	if err != nil {
		return nil, err
	}
	stmt.Source = source

	// Parse condition: "WHERE EXPR".
	condition, err := p.parseCondition()
	if err != nil {
		return nil, err
	}
	stmt.Condition = condition

	// Parse dimensions: "GROUP BY DIMENSION+".
	dimensions, err := p.parseDimensions()
	if err != nil {
		return nil, err
	}
	stmt.Dimensions = dimensions

	// Parse limit: "LIMIT INT".
	limit, err := p.parseLimit()
	if err != nil {
		return nil, err
	}
	stmt.Limit = limit

	// Parse ordering: "ORDER BY (ASC|DESC)".
	ascending, err := p.parseOrderBy()
	if err != nil {
		return nil, err
	}
	stmt.Ascending = ascending

	return stmt, nil
}

// parseDeleteStatement parses a delete string and returns a DeleteStatement.
// This function assumes the DELETE token has already been consumed.
func (p *Parser) parseDeleteStatement() (*DeleteStatement, error) {
	stmt := &DeleteStatement{}

	// Parse source: "FROM IDENT".
	source, err := p.parseSource()
	if err != nil {
		return nil, err
	}
	stmt.Source = source

	// Parse condition: "WHERE EXPR".
	condition, err := p.parseCondition()
	if err != nil {
		return nil, err
	}
	stmt.Condition = condition

	return stmt, nil
}

// parseListSeriesStatement parses a string and returns a ListSeriesStatement.
// This function assumes the "LIST SERIES" tokens have already been consumed.
func (p *Parser) parseListSeriesStatement() (*ListSeriesStatement, error) {
	stmt := &ListSeriesStatement{}
	return stmt, nil
}

// parseDropSeriesStatement parses a string and returns a DropSeriesStatement.
// This function assumes the "DROP SERIES" tokens have already been consumed.
func (p *Parser) parseDropSeriesStatement() (*DropSeriesStatement, error) {
	stmt := &DropSeriesStatement{}

	// Read the name of the series to drop.
	tok, pos, lit := p.scanIgnoreWhitespace()
	if tok != IDENT && tok != STRING {
		return nil, newParseError(tokstr(tok, lit), []string{"identifier", "string"}, pos)
	}
	stmt.Name = lit

	return stmt, nil
}

// parseListContinuousQueriesStatement parses a string and returns a ListContinuousQueriesStatement.
// This function assumes the "LIST CONTINUOUS" tokens have already been consumed.
func (p *Parser) parseListContinuousQueriesStatement() (*ListContinuousQueriesStatement, error) {
	stmt := &ListContinuousQueriesStatement{}

	// Expect a "QUERIES" token.
	if tok, pos, lit := p.scanIgnoreWhitespace(); tok != QUERIES {
		return nil, newParseError(tokstr(tok, lit), []string{"QUERIES"}, pos)
	}

	return stmt, nil
}

// parseCreateContinuousQueriesStatement parses a string and returns a CreateContinuousQueryStatement.
// This function assumes the "CREATE CONTINUOUS" tokens have already been consumed.
func (p *Parser) parseCreateContinuousQueryStatement() (*CreateContinuousQueryStatement, error) {
	stmt := &CreateContinuousQueryStatement{}

	// Expect a "QUERY" token.
	if tok, pos, lit := p.scanIgnoreWhitespace(); tok != QUERY {
		return nil, newParseError(tokstr(tok, lit), []string{"QUERY"}, pos)
	}

	// Read the id of the query to create.
	tok, pos, lit := p.scanIgnoreWhitespace()
	if tok != IDENT && tok != STRING {
		return nil, newParseError(tokstr(tok, lit), []string{"identifier", "string"}, pos)
	}
	stmt.Name = lit

	// Expect an "AS SELECT" keyword.
	if tok, pos, lit := p.scanIgnoreWhitespace(); tok != AS {
		return nil, newParseError(tokstr(tok, lit), []string{"AS"}, pos)
	}
	if tok, pos, lit := p.scanIgnoreWhitespace(); tok != SELECT {
		return nil, newParseError(tokstr(tok, lit), []string{"SELECT"}, pos)
	}

	// Read the select statement to be used as the source.
	source, err := p.parseSelectStatement()
	if err != nil {
		return nil, err
	}
	stmt.Source = source

	// Expect an INTO keyword.
	if tok, pos, lit := p.scanIgnoreWhitespace(); tok != INTO {
		return nil, newParseError(tokstr(tok, lit), []string{"INTO"}, pos)
	}

	// Read the target of the query.
	tok, pos, lit = p.scanIgnoreWhitespace()
	if tok != IDENT && tok != STRING {
		return nil, newParseError(tokstr(tok, lit), []string{"identifier", "string"}, pos)
	}
	stmt.Target = lit

	return stmt, nil
}

// parseDropContinuousQueriesStatement parses a string and returns a DropContinuousQueryStatement.
// This function assumes the "DROP CONTINUOUS" tokens have already been consumed.
func (p *Parser) parseDropContinuousQueryStatement() (*DropContinuousQueryStatement, error) {
	stmt := &DropContinuousQueryStatement{}

	// Expect a "QUERY" token.
	if tok, pos, lit := p.scanIgnoreWhitespace(); tok != QUERY {
		return nil, newParseError(tokstr(tok, lit), []string{"QUERY"}, pos)
	}

	// Read the id of the query to drop.
	tok, pos, lit := p.scanIgnoreWhitespace()
	if tok != IDENT && tok != STRING {
		return nil, newParseError(tokstr(tok, lit), []string{"identifier", "string"}, pos)
	}
	stmt.Name = lit

	return stmt, nil
}

// parseFields parses a list of one or more fields.
func (p *Parser) parseFields() (Fields, error) {
	var fields Fields

	// Check for "*" (i.e., "all fields")
	if tok, _, _ := p.scanIgnoreWhitespace(); tok == MUL {
		fields = append(fields, &Field{&Wildcard{}, ""})
		return fields, nil
	}
	p.unscan()

	for {
		// Parse the field.
		f, err := p.parseField()
		if err != nil {
			return nil, err
		}

		// Add new field.
		fields = append(fields, f)

		// If there's not a comma next then stop parsing fields.
		if tok, _, _ := p.scan(); tok != COMMA {
			p.unscan()
			break
		}
	}
	return fields, nil
}

// parseField parses a single field.
func (p *Parser) parseField() (*Field, error) {
	f := &Field{}

	// Parse the expression first.
	expr, err := p.ParseExpr()
	if err != nil {
		return nil, err
	}
	f.Expr = expr

	// Parse the alias if the current and next tokens are "WS AS".
	alias, err := p.parseAlias()
	if err != nil {
		return nil, err
	}
	f.Alias = alias

	// Consume all trailing whitespace.
	p.consumeWhitespace()

	return f, nil
}

// parseAlias parses the "AS (IDENT|STRING)" alias for fields and dimensions.
func (p *Parser) parseAlias() (string, error) {
	// Check if the next token is "AS". If not, then unscan and exit.
	if tok, _, _ := p.scanIgnoreWhitespace(); tok != AS {
		p.unscan()
		return "", nil
	}

	// Then we should have the alias identifier.
	tok, pos, lit := p.scanIgnoreWhitespace()
	if tok != IDENT && tok != STRING {
		return "", newParseError(tokstr(tok, lit), []string{"identifier", "string"}, pos)
	}
	return lit, nil
}

// parseSource parses the "FROM" clause of the query.
func (p *Parser) parseSource() (Source, error) {
	// Ensure the FROM token exists.
	if tok, pos, lit := p.scanIgnoreWhitespace(); tok != FROM {
		return nil, newParseError(tokstr(tok, lit), []string{"FROM"}, pos)
	}

	// Scan the identifier for the source.
	tok, pos, lit := p.scanIgnoreWhitespace()
	if tok != IDENT && tok != STRING {
		return nil, newParseError(tokstr(tok, lit), []string{"identifier", "string"}, pos)
	}

	return &Series{Name: lit}, nil
}

// parseCondition parses the "WHERE" clause of the query, if it exists.
func (p *Parser) parseCondition() (Expr, error) {
	// Check if the WHERE token exists.
	if tok, _, _ := p.scanIgnoreWhitespace(); tok != WHERE {
		p.unscan()
		return nil, nil
	}

	// Scan the identifier for the source.
	expr, err := p.ParseExpr()
	if err != nil {
		return nil, err
	}

	return expr, nil
}

// parseDimensions parses the "GROUP BY" clause of the query, if it exists.
func (p *Parser) parseDimensions() (Dimensions, error) {
	// If the next token is not GROUP then exit.
	if tok, _, _ := p.scanIgnoreWhitespace(); tok != GROUP {
		p.unscan()
		return nil, nil
	}

	// Now the next token should be "BY".
	if tok, pos, lit := p.scanIgnoreWhitespace(); tok != BY {
		return nil, newParseError(tokstr(tok, lit), []string{"BY"}, pos)
	}

	var dimensions Dimensions
	for {
		// Parse the dimension.
		d, err := p.parseDimension()
		if err != nil {
			return nil, err
		}

		// Add new dimension.
		dimensions = append(dimensions, d)

		// If there's not a comma next then stop parsing dimensions.
		if tok, _, _ := p.scan(); tok != COMMA {
			p.unscan()
			break
		}
	}
	return dimensions, nil
}

// parseDimension parses a single dimension.
func (p *Parser) parseDimension() (*Dimension, error) {
	// Parse the expression first.
	expr, err := p.ParseExpr()
	if err != nil {
		return nil, err
	}

	// Consume all trailing whitespace.
	p.consumeWhitespace()

	return &Dimension{Expr: expr}, nil
}

// parseLimit parses the "LIMIT" clause of the query, if it exists.
func (p *Parser) parseLimit() (int, error) {
	// Check if the LIMIT token exists.
	if tok, _, _ := p.scanIgnoreWhitespace(); tok != LIMIT {
		p.unscan()
		return 0, nil
	}

	// Scan the limit number.
	tok, pos, lit := p.scanIgnoreWhitespace()
	if tok != NUMBER {
		return 0, newParseError(tokstr(tok, lit), []string{"number"}, pos)
	}

	// Return an error if the number has a fractional part.
	if strings.Contains(lit, ".") {
		return 0, &ParseError{Message: "fractional parts not allowed in limit", Pos: pos}
	}

	// Parse number.
	n, _ := strconv.ParseInt(lit, 10, 64)

	return int(n), nil
}

// parseOrderBy parses the "ORDER BY" clause of the query, if it exists.
func (p *Parser) parseOrderBy() (bool, error) {
	// Check if the ORDER token exists.
	if tok, _, _ := p.scanIgnoreWhitespace(); tok != ORDER {
		p.unscan()
		return false, nil
	}

	// Ensure the next token is BY.
	if tok, pos, lit := p.scanIgnoreWhitespace(); tok != BY {
		return false, newParseError(tokstr(tok, lit), []string{"BY"}, pos)
	}

	// Ensure the next token is ASC OR DESC.
	tok, pos, lit := p.scanIgnoreWhitespace()
	if tok != ASC && tok != DESC {
		return false, newParseError(tokstr(tok, lit), []string{"ASC", "DESC"}, pos)
	}

	return tok == ASC, nil
}

// ParseExpr parses an expression.
func (p *Parser) ParseExpr() (Expr, error) {
	// Parse a non-binary expression type to start.
	// This variable will always be the root of the expression tree.
	expr, err := p.parseUnaryExpr()
	if err != nil {
		return nil, err
	}

	// Loop over operations and unary exprs and build a tree based on precendence.
	for {
		// If the next token is NOT an operator then return the expression.
		op, _, _ := p.scanIgnoreWhitespace()
		if !op.isOperator() {
			p.unscan()
			return expr, nil
		}

		// Otherwise parse the next unary expression.
		rhs, err := p.parseUnaryExpr()
		if err != nil {
			return nil, err
		}

		// Assign the new root based on the precendence of the LHS and RHS operators.
		if lhs, ok := expr.(*BinaryExpr); ok && lhs.Op.Precedence() <= op.Precedence() {
			expr = &BinaryExpr{
				LHS: lhs.LHS,
				RHS: &BinaryExpr{LHS: lhs.RHS, RHS: rhs, Op: op},
				Op:  lhs.Op,
			}
		} else {
			expr = &BinaryExpr{LHS: expr, RHS: rhs, Op: op}
		}
	}
}

// parseUnaryExpr parses an non-binary expression.
func (p *Parser) parseUnaryExpr() (Expr, error) {
	// If the first token is a LPAREN then parse it as its own grouped expression.
	if tok, _, _ := p.scanIgnoreWhitespace(); tok == LPAREN {
		expr, err := p.ParseExpr()
		if err != nil {
			return nil, err
		}

		// Expect an RPAREN at the end.
		if tok, pos, lit := p.scanIgnoreWhitespace(); tok != RPAREN {
			return nil, newParseError(tokstr(tok, lit), []string{")"}, pos)
		}

		return &ParenExpr{Expr: expr}, nil
	}
	p.unscan()

	// Read next token.
	tok, pos, lit := p.scanIgnoreWhitespace()
	switch tok {
	case IDENT:
		// If the next immediate token is a left parentheses, parse as function call.
		// Otherwise parse as a variable reference.
		if tok0, _, _ := p.scan(); tok0 == LPAREN {
			return p.parseCall(lit)
		} else {
			p.unscan()
			return &VarRef{Val: lit}, nil
		}
	case STRING:
		return &StringLiteral{Val: lit}, nil
	case NUMBER:
		v, err := strconv.ParseFloat(lit, 64)
		if err != nil {
			return nil, &ParseError{Message: "unable to parse number", Pos: pos}
		}
		return &NumberLiteral{Val: v}, nil
	case TRUE, FALSE:
		return &BooleanLiteral{Val: (tok == TRUE)}, nil
	case DURATION:
		v, _ := ParseDuration(lit)
		return &DurationLiteral{Val: v}, nil
	default:
		return nil, newParseError(tokstr(tok, lit), []string{"identifier", "string", "number", "bool"}, pos)
	}
}

// parseCall parses a function call.
// This function assumes the function name and LPAREN have been consumed.
func (p *Parser) parseCall(name string) (*Call, error) {
	// If there's a right paren then just return immediately.
	if tok, _, _ := p.scan(); tok == RPAREN {
		return &Call{Name: name}, nil
	}
	p.unscan()

	// Otherwise parse function call arguments.
	var args []Expr
	for {
		// Parse an expression argument.
		arg, err := p.ParseExpr()
		if err != nil {
			return nil, err
		}
		args = append(args, arg)

		// If there's not a comma next then stop parsing arguments.
		if tok, _, _ := p.scan(); tok != COMMA {
			p.unscan()
			break
		}
	}

	// There should be a right parentheses at the end.
	if tok, pos, lit := p.scan(); tok != RPAREN {
		return nil, newParseError(tokstr(tok, lit), []string{")"}, pos)
	}

	return &Call{Name: name, Args: args}, nil
}

// scan returns the next token from the underlying scanner.
func (p *Parser) scan() (tok Token, pos Pos, lit string) { return p.s.Scan() }

// scanIgnoreWhitespace scans the next non-whitespace token.
func (p *Parser) scanIgnoreWhitespace() (tok Token, pos Pos, lit string) {
	tok, pos, lit = p.scan()
	if tok == WS {
		tok, pos, lit = p.scan()
	}
	return
}

// consumeWhitespace scans the next token if it's whitespace.
func (p *Parser) consumeWhitespace() {
	if tok, _, _ := p.scan(); tok != WS {
		p.unscan()
	}
}

// unscan pushes the previously read token back onto the buffer.
func (p *Parser) unscan() { p.s.Unscan() }

// ParseDuration parses a time duration from a string.
func ParseDuration(s string) (time.Duration, error) {
	// Return an error if the string is blank.
	if len(s) == 0 {
		return 0, ErrInvalidDuration
	}

	// If there's only character then it must be a digit (in microseconds).
	if len(s) == 1 {
		if n, err := strconv.ParseInt(s, 10, 64); err != nil {
			return 0, ErrInvalidDuration
		} else {
			return time.Duration(n) * time.Microsecond, nil
		}
	}

	// Split string into individual runes.
	a := split(s)

	// Extract the unit of measure.
	// If the last character is a digit then parse the whole string as microseconds.
	// If the last two characters are "ms" the parse as milliseconds.
	// Otherwise just use the last character as the unit of measure.
	var num, uom string
	if isDigit(rune(a[len(a)-1])) {
		num, uom = s, "u"
	} else if len(s) > 2 && s[len(s)-2:] == "ms" {
		num, uom = string(a[:len(a)-2]), "ms"
	} else {
		num, uom = string(a[:len(a)-1]), string(a[len(a)-1:])
	}

	// Parse the numeric part.
	n, err := strconv.ParseInt(num, 10, 64)
	if err != nil {
		return 0, ErrInvalidDuration
	}

	// Multiply by the unit of measure.
	switch uom {
	case "u", "µ":
		return time.Duration(n) * time.Microsecond, nil
	case "ms":
		return time.Duration(n) * time.Millisecond, nil
	case "s":
		return time.Duration(n) * time.Second, nil
	case "m":
		return time.Duration(n) * time.Minute, nil
	case "h":
		return time.Duration(n) * time.Hour, nil
	case "d":
		return time.Duration(n) * 24 * time.Hour, nil
	case "w":
		return time.Duration(n) * 7 * 24 * time.Hour, nil
	default:
		return 0, ErrInvalidDuration
	}
}

// split splits a string into a slice of runes.
func split(s string) (a []rune) {
	for _, ch := range s {
		a = append(a, ch)
	}
	return
}

// ErrInvalidDuration is returned when parsing a malformatted duration.
var ErrInvalidDuration = errors.New("invalid duration")

// ParseError represents an error that occurred during parsing.
type ParseError struct {
	Message  string
	Found    string
	Expected []string
	Pos      Pos
}

// newParseError returns a new instance of ParseError.
func newParseError(found string, expected []string, pos Pos) *ParseError {
	return &ParseError{Found: found, Expected: expected, Pos: pos}
}

// Error returns the string representation of the error.
func (e *ParseError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("%s at line %d, char %d", e.Message, e.Pos.Line+1, e.Pos.Char+1)
	}
	return fmt.Sprintf("found %s, expected %s at line %d, char %d", e.Found, strings.Join(e.Expected, ", "), e.Pos.Line+1, e.Pos.Char+1)
}

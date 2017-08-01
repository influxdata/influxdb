package influxql_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/influxdata/influxdb/influxql"
)

func TestParseTree_Clone(t *testing.T) {
	// Clone the default language parse tree and add a new syntax node.
	language := influxql.Language.Clone()
	language.Group(influxql.CREATE).Handle(influxql.STATS, func(p *influxql.Parser) (influxql.Statement, error) {
		return &influxql.ShowStatsStatement{}, nil
	})

	// Create a parser with CREATE STATS and parse the statement.
	parser := influxql.NewParser(strings.NewReader(`CREATE STATS`))
	stmt, err := language.Parse(parser)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if !reflect.DeepEqual(stmt, &influxql.ShowStatsStatement{}) {
		t.Fatalf("unexpected statement returned from parser: %s", stmt)
	}

	// Recreate the parser and try parsing with the original parsing. This should fail.
	parser = influxql.NewParser(strings.NewReader(`CREATE STATS`))
	if _, err := parser.ParseStatement(); err == nil {
		t.Fatal("expected error")
	}
}

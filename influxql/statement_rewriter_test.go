package influxql_test

import (
	"testing"

	"github.com/influxdata/influxdb/influxql"
)

func TestRewriteStatement(t *testing.T) {
	tests := []struct {
		stmt string
		s    string
	}{
		{
			stmt: `SHOW FIELD KEYS`,
			s:    `SELECT fieldKey, fieldType FROM _fieldKeys`,
		},
		{
			stmt: `SHOW FIELD KEYS FROM cpu`,
			s:    `SELECT fieldKey, fieldType FROM _fieldKeys WHERE _name = 'cpu'`,
		},
		{
			stmt: `SHOW FIELD KEYS FROM /c.*/`,
			s:    `SELECT fieldKey, fieldType FROM _fieldKeys WHERE _name =~ /c.*/`,
		},
		{
			stmt: `SHOW FIELD KEYS FROM mydb.myrp2.cpu`,
			s:    `SELECT fieldKey, fieldType FROM mydb.myrp2._fieldKeys WHERE _name = 'cpu'`,
		},
		{
			stmt: `SHOW FIELD KEYS FROM mydb.myrp2./c.*/`,
			s:    `SELECT fieldKey, fieldType FROM mydb.myrp2._fieldKeys WHERE _name =~ /c.*/`,
		},
		{
			stmt: `SHOW MEASUREMENTS`,
			s:    `SELECT _name AS "name" FROM _measurements`,
		},
		{
			stmt: `SHOW MEASUREMENTS WITH MEASUREMENT = cpu`,
			s:    `SELECT _name AS "name" FROM _measurements WHERE _name = 'cpu'`,
		},
		{
			stmt: `SHOW MEASUREMENTS WITH MEASUREMENT =~ /c.*/`,
			s:    `SELECT _name AS "name" FROM _measurements WHERE _name =~ /c.*/`,
		},
		{
			stmt: `SHOW MEASUREMENTS WHERE region = 'uswest'`,
			s:    `SELECT _name AS "name" FROM _measurements WHERE region = 'uswest'`,
		},
		{
			stmt: `SHOW MEASUREMENTS WITH MEASUREMENT = cpu WHERE region = 'uswest'`,
			s:    `SELECT _name AS "name" FROM _measurements WHERE (_name = 'cpu') AND (region = 'uswest')`,
		},
		{
			stmt: `SHOW SERIES`,
			s:    `SELECT "key" FROM _series`,
		},
		{
			stmt: `SHOW SERIES FROM cpu`,
			s:    `SELECT "key" FROM _series WHERE _name = 'cpu'`,
		},
		{
			stmt: `SHOW SERIES FROM mydb.myrp1.cpu`,
			s:    `SELECT "key" FROM mydb.myrp1._series WHERE _name = 'cpu'`,
		},
		{
			stmt: `SHOW SERIES FROM mydb.myrp1./c.*/`,
			s:    `SELECT "key" FROM mydb.myrp1._series WHERE _name =~ /c.*/`,
		},
		{
			stmt: `SHOW TAG KEYS`,
			s:    `SELECT tagKey FROM _tagKeys`,
		},
		{
			stmt: `SHOW TAG KEYS FROM cpu`,
			s:    `SELECT tagKey FROM _tagKeys WHERE _name = 'cpu'`,
		},
		{
			stmt: `SHOW TAG KEYS FROM /c.*/`,
			s:    `SELECT tagKey FROM _tagKeys WHERE _name =~ /c.*/`,
		},
		{
			stmt: `SHOW TAG KEYS FROM cpu WHERE region = 'uswest'`,
			s:    `SELECT tagKey FROM _tagKeys WHERE (_name = 'cpu') AND (region = 'uswest')`,
		},
		{
			stmt: `SHOW TAG KEYS FROM mydb.myrp1.cpu`,
			s:    `SELECT tagKey FROM mydb.myrp1._tagKeys WHERE _name = 'cpu'`,
		},
		{
			stmt: `SHOW TAG KEYS FROM mydb.myrp1./c.*/`,
			s:    `SELECT tagKey FROM mydb.myrp1._tagKeys WHERE _name =~ /c.*/`,
		},
		{
			stmt: `SHOW TAG KEYS FROM mydb.myrp1.cpu WHERE region = 'uswest'`,
			s:    `SELECT tagKey FROM mydb.myrp1._tagKeys WHERE (_name = 'cpu') AND (region = 'uswest')`,
		},
		{
			stmt: `SHOW TAG VALUES WITH KEY = region`,
			s:    `SELECT _tagKey AS "key", value FROM _tags WHERE _tagKey = 'region'`,
		},
		{
			stmt: `SHOW TAG VALUES FROM cpu WITH KEY = region`,
			s:    `SELECT _tagKey AS "key", value FROM _tags WHERE (_name = 'cpu') AND (_tagKey = 'region')`,
		},
		{
			stmt: `SHOW TAG VALUES FROM cpu WITH KEY IN (region, host)`,
			s:    `SELECT _tagKey AS "key", value FROM _tags WHERE (_name = 'cpu') AND (_tagKey = 'region' OR _tagKey = 'host')`,
		},
		{
			stmt: `SHOW TAG VALUES FROM mydb.myrp1.cpu WITH KEY IN (region, host)`,
			s:    `SELECT _tagKey AS "key", value FROM mydb.myrp1._tags WHERE (_name = 'cpu') AND (_tagKey = 'region' OR _tagKey = 'host')`,
		},
		{
			stmt: `SHOW TAG VALUES FROM cpu WITH KEY =~ /(region|host)/`,
			s:    `SELECT _tagKey AS "key", value FROM _tags WHERE (_name = 'cpu') AND (_tagKey =~ /(region|host)/)`,
		},
		{
			stmt: `SHOW TAG VALUES FROM mydb.myrp1.cpu WITH KEY =~ /(region|host)/`,
			s:    `SELECT _tagKey AS "key", value FROM mydb.myrp1._tags WHERE (_name = 'cpu') AND (_tagKey =~ /(region|host)/)`,
		},
		{
			stmt: `SELECT value FROM cpu`,
			s:    `SELECT value FROM cpu`,
		},
	}

	for _, test := range tests {
		stmt, err := influxql.ParseStatement(test.stmt)
		if err != nil {
			t.Errorf("error parsing statement: %s", err)
		} else {
			stmt, err = influxql.RewriteStatement(stmt)
			if err != nil {
				t.Errorf("error rewriting statement: %s", err)
			} else if s := stmt.String(); s != test.s {
				t.Errorf("error rendering string. expected %s, actual: %s", test.s, s)
			}
		}
	}
}

package query_test

import (
	"testing"

	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxql"
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
			stmt: `SHOW FIELD KEYS ON db0`,
			s:    `SELECT fieldKey, fieldType FROM db0.._fieldKeys`,
		},
		{
			stmt: `SHOW FIELD KEYS FROM cpu`,
			s:    `SELECT fieldKey, fieldType FROM _fieldKeys WHERE _name = 'cpu'`,
		},
		{
			stmt: `SHOW FIELD KEYS ON db0 FROM cpu`,
			s:    `SELECT fieldKey, fieldType FROM db0.._fieldKeys WHERE _name = 'cpu'`,
		},
		{
			stmt: `SHOW FIELD KEYS FROM /c.*/`,
			s:    `SELECT fieldKey, fieldType FROM _fieldKeys WHERE _name =~ /c.*/`,
		},
		{
			stmt: `SHOW FIELD KEYS ON db0 FROM /c.*/`,
			s:    `SELECT fieldKey, fieldType FROM db0.._fieldKeys WHERE _name =~ /c.*/`,
		},
		{
			stmt: `SHOW FIELD KEYS FROM mydb.myrp2.cpu`,
			s:    `SELECT fieldKey, fieldType FROM mydb.myrp2._fieldKeys WHERE _name = 'cpu'`,
		},
		{
			stmt: `SHOW FIELD KEYS ON db0 FROM mydb.myrp2.cpu`,
			s:    `SELECT fieldKey, fieldType FROM mydb.myrp2._fieldKeys WHERE _name = 'cpu'`,
		},
		{
			stmt: `SHOW FIELD KEYS FROM mydb.myrp2./c.*/`,
			s:    `SELECT fieldKey, fieldType FROM mydb.myrp2._fieldKeys WHERE _name =~ /c.*/`,
		},
		{
			stmt: `SHOW FIELD KEYS ON db0 FROM mydb.myrp2./c.*/`,
			s:    `SELECT fieldKey, fieldType FROM mydb.myrp2._fieldKeys WHERE _name =~ /c.*/`,
		},
		{
			stmt: `SHOW SERIES`,
			s:    `SELECT _seriesKey AS "key" FROM /.+/`,
		},
		{
			stmt: `SHOW SERIES ON db0`,
			s:    `SELECT _seriesKey AS "key" FROM db0../.+/`,
		},
		{
			stmt: `SHOW SERIES FROM cpu`,
			s:    `SELECT _seriesKey AS "key" FROM cpu`,
		},
		{
			stmt: `SHOW SERIES ON db0 FROM cpu`,
			s:    `SELECT _seriesKey AS "key" FROM db0..cpu`,
		},
		{
			stmt: `SHOW SERIES FROM mydb.myrp1.cpu`,
			s:    `SELECT _seriesKey AS "key" FROM mydb.myrp1.cpu`,
		},
		{
			stmt: `SHOW SERIES ON db0 FROM mydb.myrp1.cpu`,
			s:    `SELECT _seriesKey AS "key" FROM mydb.myrp1.cpu`,
		},
		{
			stmt: `SHOW SERIES FROM mydb.myrp1./c.*/`,
			s:    `SELECT _seriesKey AS "key" FROM mydb.myrp1./c.*/`,
		},
		{
			stmt: `SHOW SERIES ON db0 FROM mydb.myrp1./c.*/`,
			s:    `SELECT _seriesKey AS "key" FROM mydb.myrp1./c.*/`,
		},
		{
			stmt: `SHOW TAG KEYS`,
			s:    `SELECT distinct(_tagKey) AS tagKey FROM /.+/`,
		},
		{
			stmt: `SHOW TAG KEYS ON db0`,
			s:    `SELECT distinct(_tagKey) AS tagKey FROM db0../.+/`,
		},
		{
			stmt: `SHOW TAG KEYS FROM cpu`,
			s:    `SELECT distinct(_tagKey) AS tagKey FROM cpu`,
		},
		{
			stmt: `SHOW TAG KEYS ON db0 FROM cpu`,
			s:    `SELECT distinct(_tagKey) AS tagKey FROM db0..cpu`,
		},
		{
			stmt: `SHOW TAG KEYS FROM /c.*/`,
			s:    `SELECT distinct(_tagKey) AS tagKey FROM /c.*/`,
		},
		{
			stmt: `SHOW TAG KEYS ON db0 FROM /c.*/`,
			s:    `SELECT distinct(_tagKey) AS tagKey FROM db0../c.*/`,
		},
		{
			stmt: `SHOW TAG KEYS FROM cpu WHERE region = 'uswest'`,
			s:    `SELECT distinct(_tagKey) AS tagKey FROM cpu WHERE region = 'uswest'`,
		},
		{
			stmt: `SHOW TAG KEYS ON db0 FROM cpu WHERE region = 'uswest'`,
			s:    `SELECT distinct(_tagKey) AS tagKey FROM db0..cpu WHERE region = 'uswest'`,
		},
		{
			stmt: `SHOW TAG KEYS FROM mydb.myrp1.cpu`,
			s:    `SELECT distinct(_tagKey) AS tagKey FROM mydb.myrp1.cpu`,
		},
		{
			stmt: `SHOW TAG KEYS ON db0 FROM mydb.myrp1.cpu`,
			s:    `SELECT distinct(_tagKey) AS tagKey FROM mydb.myrp1.cpu`,
		},
		{
			stmt: `SHOW TAG KEYS FROM mydb.myrp1./c.*/`,
			s:    `SELECT distinct(_tagKey) AS tagKey FROM mydb.myrp1./c.*/`,
		},
		{
			stmt: `SHOW TAG KEYS ON db0 FROM mydb.myrp1./c.*/`,
			s:    `SELECT distinct(_tagKey) AS tagKey FROM mydb.myrp1./c.*/`,
		},
		{
			stmt: `SHOW TAG KEYS FROM mydb.myrp1.cpu WHERE region = 'uswest'`,
			s:    `SELECT distinct(_tagKey) AS tagKey FROM mydb.myrp1.cpu WHERE region = 'uswest'`,
		},
		{
			stmt: `SHOW TAG KEYS ON db0 FROM mydb.myrp1.cpu WHERE region = 'uswest'`,
			s:    `SELECT distinct(_tagKey) AS tagKey FROM mydb.myrp1.cpu WHERE region = 'uswest'`,
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
			stmt, err = query.RewriteStatement(stmt)
			if err != nil {
				t.Errorf("error rewriting statement: %s", err)
			} else if s := stmt.String(); s != test.s {
				t.Errorf("error rendering string. expected %s, actual: %s", test.s, s)
			}
		}
	}
}

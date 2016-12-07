package cli

import "testing"

func TestParseCommand_InsertInto(t *testing.T) {
	t.Parallel()

	c := CommandLine{}

	tests := []struct {
		cmd, db, rp string
	}{
		{
			cmd: `INSERT INTO test cpu,host=serverA,region=us-west value=1.0`,
			db:  "",
			rp:  "test",
		},
		{
			cmd: ` INSERT INTO .test cpu,host=serverA,region=us-west value=1.0`,
			db:  "",
			rp:  "test",
		},
		{
			cmd: `INSERT INTO   "test test" cpu,host=serverA,region=us-west value=1.0`,
			db:  "",
			rp:  "test test",
		},
		{
			cmd: `Insert iNTO test.test cpu,host=serverA,region=us-west value=1.0`,
			db:  "test",
			rp:  "test",
		},
		{
			cmd: `insert into "test test" cpu,host=serverA,region=us-west value=1.0`,
			db:  "",
			rp:  "test test",
		},
		{
			cmd: `insert into "d b"."test test" cpu,host=serverA,region=us-west value=1.0`,
			db:  "d b",
			rp:  "test test",
		},
	}

	for _, test := range tests {
		t.Logf("command: %s", test.cmd)
		bp, err := c.parseInsert(test.cmd)
		if err != nil {
			t.Fatal(err)
		}
		if bp.Database != test.db {
			t.Fatalf(`Command "insert into" db parsing failed, expected: %q, actual: %q`, test.db, bp.Database)
		}
		if bp.RetentionPolicy != test.rp {
			t.Fatalf(`Command "insert into" rp parsing failed, expected: %q, actual: %q`, test.rp, bp.RetentionPolicy)
		}
	}
}

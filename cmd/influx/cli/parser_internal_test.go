package cli

import (
	"errors"
	"testing"
)

func Test_parseDatabaseAndretentionPolicy(t *testing.T) {
	tests := []struct {
		stmt string
		db   string
		rp   string
		err  error
	}{
		{
			stmt: `foo`,
			db:   "foo",
		},
		{
			stmt: `"foo.bar"`,
			db:   "foo.bar",
		},
		{
			stmt: `"foo.bar".`,
			db:   "foo.bar",
		},
		{
			stmt: `."foo.bar"`,
			rp:   "foo.bar",
		},
		{
			stmt: `foo.bar`,
			db:   "foo",
			rp:   "bar",
		},
		{
			stmt: `"foo".bar`,
			db:   "foo",
			rp:   "bar",
		},
		{
			stmt: `"foo"."bar"`,
			db:   "foo",
			rp:   "bar",
		},
		{
			stmt: `"foo.bin"."bar"`,
			db:   "foo.bin",
			rp:   "bar",
		},
		{
			stmt: `"foo.bin"."bar.baz...."`,
			db:   "foo.bin",
			rp:   "bar.baz....",
		},
		{
			stmt: `  "foo.bin"."bar.baz...."  `,
			db:   "foo.bin",
			rp:   "bar.baz....",
		},

		{
			stmt: `"foo.bin"."bar".boom`,
			err:  errors.New("foo"),
		},
		{
			stmt: "foo.bar.",
			err:  errors.New("foo"),
		},
	}

	for _, test := range tests {
		db, rp, err := parseDatabaseAndRetentionPolicy([]byte(test.stmt))
		if err != nil && test.err == nil {
			t.Errorf("unexpected error: got %s", err)
			continue
		}
		if test.err != nil && err == nil {
			t.Errorf("expected err: got: nil, exp: %s", test.err)
			continue
		}
		if db != test.db {
			t.Errorf("unexpected database: got: %s, exp: %s", db, test.db)
		}
		if rp != test.rp {
			t.Errorf("unexpected retention policy: got: %s, exp: %s", rp, test.rp)
		}
	}

}

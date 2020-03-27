package httpd

import "testing"

// test of how we extract the database and retention policy from the bucket in
// our v2 api enpoint.
//
func TestV2DatabaseRetentionPolicyMapper(t *testing.T) {
	tests := map[string]struct {
		input     string
		db        string
		rp        string
		shoulderr bool
	}{
		"Properly Encoded": {
			input:     "database/retention",
			db:        "database",
			rp:        "retention",
			shoulderr: false,
		},
		"Empty Database": {
			input:     "/retention",
			db:        "",
			rp:        "",
			shoulderr: true,
		},
		"Empty Retention Policy": {
			input:     "database/",
			db:        "database",
			rp:        "",
			shoulderr: false,
		},
		"No Slash, Empty Retention Policy": {
			input:     "database",
			db:        "database",
			rp:        "",
			shoulderr: false,
		},
		"Empty String": {
			input:     "",
			db:        "",
			rp:        "",
			shoulderr: true,
		},
		"Space Before DB": {
			input:     "     database/retention",
			db:        "     database",
			rp:        "retention",
			shoulderr: false,
		},
		"Space After DB": {
			input:     "database     /retention",
			db:        "database     ",
			rp:        "retention",
			shoulderr: false,
		},
		"Space Before RP": {
			input:     "database/     retention",
			db:        "database",
			rp:        "     retention",
			shoulderr: false,
		},
		"Space After RP": {
			input:     "database/retention     ",
			db:        "database",
			rp:        "retention     ",
			shoulderr: false,
		},
	}

	t.Parallel()
	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			db, rp, err := bucket2dbrp(test.input)
			switch goterr, shoulderr := err != nil, test.shoulderr; {
			case goterr != shoulderr:
				switch shoulderr {
				case true:
					t.Fatalf("bucket2dbrp(%q) did not return an error; expected to return an error", test.input)
				default:
					t.Fatalf("bucket2dbrp(%q) return an error %v; expected to return a nil error", test.input, err)
				}
			}

			if got, expected := db, test.db; got != expected {
				t.Fatalf("bucket2dbrp(%q) returned a database of %q; epected %q", test.input, got, expected)
			}

			if got, expected := rp, test.rp; got != expected {
				t.Fatalf("bucket2dbrp(%q) returned a retention policy of %q; epected %q", test.input, got, expected)
			}
		})
	}
}

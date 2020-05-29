package influxdb

import (
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestSecretFieldJSON(t *testing.T) {
	cases := []struct {
		name   string
		fld    *SecretField
		json   string
		target SecretField
	}{
		{
			name:   "regular",
			fld:    &SecretField{Key: "some key"},
			json:   `"secret: some key"`,
			target: SecretField{Key: "some key"},
		},
		{name: "blank", fld: &SecretField{}, json: `""`},
		{
			name: "with value",
			fld: &SecretField{
				Key:   "some key",
				Value: strPtr("some value"),
			},
			json: `"secret: some key"`,
			target: SecretField{
				Key: "some key",
			},
		},
		{
			name: "unmarshal a post",
			json: `"some value"`,
			target: SecretField{
				Value: strPtr("some value"),
			},
		},
	}
	for _, c := range cases {
		if c.fld != nil {
			serialized, err := json.Marshal(c.fld)
			if err != nil {
				t.Fatalf("%s failed, secret key marshal err: %q", c.name, err.Error())
			}
			if string(serialized) != c.json {
				t.Fatalf("%s failed, secret key marshal result is unexpected, got %q, want %q", c.name, string(serialized), c.json)
			}
		}
		var deserialized SecretField
		if err := json.Unmarshal([]byte(c.json), &deserialized); err != nil {
			t.Fatalf("%s failed, secret key unmarshal err: %q", c.name, err.Error())
		}
		if diff := cmp.Diff(deserialized, c.target); diff != "" {
			t.Fatalf("%s failed, secret key unmarshal result is unexpected, diff %s", c.name, diff)
		}
	}
}

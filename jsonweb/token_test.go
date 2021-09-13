package jsonweb

import (
	"reflect"
	"testing"

	"github.com/golang-jwt/jwt"
	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
)

var (
	one      = platform.ID(1)
	two      = platform.ID(2)
	keyStore = KeyStoreFunc(func(kid string) ([]byte, error) {
		if kid != "some-key" {
			return nil, ErrKeyNotFound
		}

		return []byte("correct-key"), nil
	})
)

func Test_TokenParser(t *testing.T) {
	for _, test := range []struct {
		name     string
		keyStore KeyStore
		input    string
		// expectations
		token *Token
		err   error
	}{
		{
			name:  "happy path",
			input: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJjbG91ZDIuaW5mbHV4ZGF0YS5jb20iLCJhdWQiOiJnYXRld2F5LmluZmx1eGRhdGEuY29tIiwiaWF0IjoxNTY4NjI4OTgwLCJraWQiOiJzb21lLWtleSIsInBlcm1pc3Npb25zIjpbeyJhY3Rpb24iOiJ3cml0ZSIsInJlc291cmNlIjp7InR5cGUiOiJidWNrZXRzIiwiaWQiOiIwMDAwMDAwMDAwMDAwMDAxIiwib3JnSUQiOiIwMDAwMDAwMDAwMDAwMDAyIn19XX0.74vjbExiOd702VSIMmQWaDT_GFvUI0-_P-SfQ_OOHB0",
			token: &Token{
				StandardClaims: jwt.StandardClaims{
					Issuer:   "cloud2.influxdata.com",
					Audience: "gateway.influxdata.com",
					IssuedAt: 1568628980,
				},
				KeyID: "some-key",
				Permissions: []influxdb.Permission{
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type:  influxdb.BucketsResourceType,
							ID:    &one,
							OrgID: &two,
						},
					},
				},
			},
		},
		{
			name:  "key not found",
			input: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJjbG91ZDIuaW5mbHV4ZGF0YS5jb20iLCJhdWQiOiJnYXRld2F5LmluZmx1eGRhdGEuY29tIiwiaWF0IjoxNTY4NjMxMTQ0LCJraWQiOiJzb21lLW90aGVyLWtleSIsInBlcm1pc3Npb25zIjpbeyJhY3Rpb24iOiJyZWFkIiwicmVzb3VyY2UiOnsidHlwZSI6InRhc2tzIiwiaWQiOiIwMDAwMDAwMDAwMDAwMDAzIiwib3JnSUQiOiIwMDAwMDAwMDAwMDAwMDA0In19XX0.QVXJ3kGP1gsxisNZe7QmphXox-vjZr6MAMbd00CQlfA",
			err: &jwt.ValidationError{
				Inner:  ErrKeyNotFound,
				Errors: jwt.ValidationErrorUnverifiable,
			},
		},
		{
			name:  "invalid signature",
			input: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJjbG91ZDIuaW5mbHV4ZGF0YS5jb20iLCJhdWQiOiJnYXRld2F5LmluZmx1eGRhdGEuY29tIiwiaWF0IjoxNTY4NjMxMTQ0LCJraWQiOiJzb21lLWtleSIsInBlcm1pc3Npb25zIjpbeyJhY3Rpb24iOiJyZWFkIiwicmVzb3VyY2UiOnsidHlwZSI6InRhc2tzIiwiaWQiOiIwMDAwMDAwMDAwMDAwMDAzIiwib3JnSUQiOiIwMDAwMDAwMDAwMDAwMDA0In19XX0.RwmNs5u6NnjNq9xTdAIERFrI5ow-6lJpND3jRrTwkaE",
			err: &jwt.ValidationError{
				Inner:  jwt.ErrSignatureInvalid,
				Errors: jwt.ValidationErrorSignatureInvalid,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			parser := NewTokenParser(keyStore)

			token, err := parser.Parse(test.input)
			if !reflect.DeepEqual(test.err, err) {
				t.Errorf("expected %[1]s (%#[1]v), got %[2]s (%#[2]v)", test.err, err)
			}

			if diff := cmp.Diff(test.token, token); diff != "" {
				t.Errorf("unexpected token:\n%s", diff)
			}

			// if err is nil then token should be present
			if err == nil {
				// ensure this does not panic
				token.Identifier()
			}
		})
	}
}

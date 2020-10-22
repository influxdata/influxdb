package influxdb

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAuthorizationType_String(t *testing.T) {
	for _, tc := range []struct {
		name string
		val  AuthorizationType
		exp  string
	}{
		{
			name: "default value is plain",
			exp:  "plain",
		},
		{
			name: "plain",
			val:  AuthorizationTypePlain,
			exp:  "plain",
		},
		{
			name: "v1_user",
			val:  AuthorizationTypeV1User,
			exp:  "v1_user",
		},
		{
			name: "invalid",
			val:  AuthorizationType("foo"),
			exp:  "invalid",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.exp, tc.val.String())
		})
	}
}

func TestAuthorizationType_MarshalJSON(t *testing.T) {
	for _, tc := range []struct {
		name   string
		val    AuthorizationType
		exp    string
		expErr bool
	}{
		{
			name: "plain",
			val:  AuthorizationTypePlain,
			exp:  `"plain"`,
		},
		{
			name: "v1_user",
			val:  AuthorizationTypeV1User,
			exp:  `"v1_user"`,
		},
		{
			name:   "unknown is error",
			val:    AuthorizationType("foo"),
			expErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := json.Marshal(&tc.val)
			if tc.expErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, string(got), tc.exp)
			}
		})
	}
}

func TestAuthorizationType_UnmarshalJSON(t *testing.T) {
	for _, tc := range []struct {
		name   string
		exp    AuthorizationType
		json   string
		expErr bool
	}{
		{
			name: "plain",
			json: `"plain"`,
			exp:  AuthorizationTypePlain,
		},
		{
			name: "v1_user",
			json: `"v1_user"`,
			exp:  AuthorizationTypeV1User,
		},
		{
			name:   "empty is error",
			json:   `""`,
			expErr: true,
		},
		{
			name:   "unexpected code is error",
			json:   `"foo"`,
			expErr: true,
		},
		{
			name:   "unexpected type is error",
			json:   `0`,
			expErr: true,
		},
		{
			name:   "invalid json is error",
			json:   `"one_way`,
			expErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var val AuthorizationType
			err := json.Unmarshal([]byte(tc.json), &val)
			if tc.expErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, val, tc.exp)
			}
		})
	}
}

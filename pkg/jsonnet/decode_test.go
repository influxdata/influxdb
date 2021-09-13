package jsonnet_test

import (
	"strings"
	"testing"

	"github.com/influxdata/influxdb/v2/pkg/jsonnet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecoder(t *testing.T) {
	type (
		person struct {
			Name    string `json:"name"`
			Welcome string `json:"welcome"`
		}

		persons struct {
			Person1 person `json:"person1"`
			Person2 person `json:"person2"`
		}
	)

	const entry = `{
  person1: {
    name: "Alice",
    welcome: "Hello " + self.name + "!",
  },
  person2: self.person1 { name: "Bob" },
}`

	var out persons
	require.NoError(t, jsonnet.NewDecoder(strings.NewReader(entry)).Decode(&out))

	expected := persons{
		Person1: person{
			Name:    "Alice",
			Welcome: "Hello Alice!",
		},
		Person2: person{
			Name:    "Bob",
			Welcome: "Hello Bob!",
		},
	}
	assert.Equal(t, expected, out)
}

package launcher

import (
	"bytes"
	"testing"

	"github.com/influxdata/influxdb/v2/kit/platform"

	"github.com/influxdata/influxdb/v2/kit/cli"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

// Pretend we've already used cobra/viper to write
// values into these vars.
var stringVar = "string-value"
var intVar = 12344
var boolVar = false
var floatVar = 987.654
var sliceVar = []string{"hello", "world"}
var mapVar = map[string]string{"foo": "bar", "baz": "qux"}
var levelVar = zapcore.InfoLevel
var idVar, _ = platform.IDFromString("020f755c3c082000")

var opts = []cli.Opt{
	{
		DestP: &stringVar,
		Flag:  "string-var",
	},
	{
		DestP: &intVar,
		Flag:  "int-var",
	},
	{
		DestP: &boolVar,
		Flag:  "bool-var",
	},
	{
		DestP: &floatVar,
		Flag:  "float-var",
	},
	{
		DestP: &sliceVar,
		Flag:  "slice-var",
	},
	{
		DestP: &mapVar,
		Flag:  "map-var",
	},
	{
		DestP: &levelVar,
		Flag:  "level-var",
	},
	{
		DestP: &idVar,
		Flag:  "id-var",
	},
}

func Test_printAllConfig(t *testing.T) {
	var out bytes.Buffer
	require.NoError(t, printAllConfigRunE(opts, &out))

	expected := `bool-var: false
float-var: 987.654
id-var: 020f755c3c082000
int-var: 12344
level-var: info
map-var:
    baz: qux
    foo: bar
slice-var:
  - hello
  - world
string-var: string-value
`

	require.Equal(t, expected, out.String())
}

func Test_printOneConfig(t *testing.T) {
	testCases := []struct {
		key      string
		expected string
	}{
		{
			key:      "bool-var",
			expected: "false",
		},
		{
			key:      "float-var",
			expected: "987.654",
		},
		{
			key:      "id-var",
			expected: "020f755c3c082000",
		},
		{
			key:      "level-var",
			expected: "info",
		},
		{
			key: "map-var",
			expected: `baz: qux
foo: bar`,
		},
		{
			key: "slice-var",
			expected: `- hello
- world`,
		},
		{
			key:      "string-var",
			expected: "string-value",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.key, func(t *testing.T) {
			var out bytes.Buffer
			require.NoError(t, printOneConfigRunE(opts, tc.key, &out))
			require.Equal(t, tc.expected+"\n", out.String())
		})
	}

	t.Run("bad-key", func(t *testing.T) {
		var out bytes.Buffer
		require.Error(t, printOneConfigRunE(opts, "bad-key", &out))
		require.Empty(t, out.String())
	})
}

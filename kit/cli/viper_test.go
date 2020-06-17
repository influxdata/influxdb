package cli

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type customFlag bool

func (c customFlag) String() string {
	if c == true {
		return "on"
	}
	return "off"
}

func (c *customFlag) Set(s string) error {
	if s == "on" {
		*c = true
	} else {
		*c = false
	}

	return nil
}

func (c *customFlag) Type() string {
	return "fancy-bool"
}

func ExampleNewCommand() {
	var monitorHost string
	var number int
	var sleep bool
	var duration time.Duration
	var stringSlice []string
	var fancyBool customFlag
	cmd := NewCommand(&Program{
		Run: func() error {
			fmt.Println(monitorHost)
			for i := 0; i < number; i++ {
				fmt.Printf("%d\n", i)
			}
			fmt.Println(sleep)
			fmt.Println(duration)
			fmt.Println(stringSlice)
			fmt.Println(fancyBool)
			return nil
		},
		Name: "myprogram",
		Opts: []Opt{
			{
				DestP:   &monitorHost,
				Flag:    "monitor-host",
				Default: "http://localhost:8086",
				Desc:    "host to send influxdb metrics",
			},
			{
				DestP:   &number,
				Flag:    "number",
				Default: 2,
				Desc:    "number of times to loop",
			},
			{
				DestP:   &sleep,
				Flag:    "sleep",
				Default: true,
				Desc:    "whether to sleep",
			},
			{
				DestP:   &duration,
				Flag:    "duration",
				Default: time.Minute,
				Desc:    "how long to sleep",
			},
			{
				DestP:   &stringSlice,
				Flag:    "string-slice",
				Default: []string{"foo", "bar"},
				Desc:    "things come in lists",
			},
			{
				DestP:   &fancyBool,
				Flag:    "fancy-bool",
				Default: "on",
				Desc:    "things that implement pflag.Value",
			},
		},
	})

	if err := cmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
	// Output:
	// http://localhost:8086
	// 0
	// 1
	// true
	// 1m0s
	// [foo bar]
	// on
}

func Test_NewProgram(t *testing.T) {
	testFilePath, cleanup := newConfigFile(t, map[string]string{
		"FOO": "bar",
	})
	defer cleanup()
	defer setEnvVar("TEST_CONFIG_FILE", testFilePath)()

	tests := []struct {
		name      string
		envVarVal string
		args      []string
		expected  string
	}{
		{
			name:     "no vals reads from config",
			expected: "bar",
		},
		{
			name:      "reads from env var",
			envVarVal: "foobar",
			expected:  "foobar",
		},
		{
			name:     "reads from flag",
			args:     []string{"--foo=baz"},
			expected: "baz",
		},
		{
			name:      "flag has highest precedence",
			envVarVal: "foobar",
			args:      []string{"--foo=baz"},
			expected:  "baz",
		},
	}

	for _, tt := range tests {
		fn := func(t *testing.T) {
			if tt.envVarVal != "" {
				defer setEnvVar("TEST_FOO", tt.envVarVal)()
			}

			var testVar string
			program := &Program{
				Name: "test",
				Opts: []Opt{
					{
						DestP:    &testVar,
						Flag:     "foo",
						Required: true,
					},
				},
				Run: func() error { return nil },
			}

			cmd := NewCommand(program)
			cmd.SetArgs(append([]string{}, tt.args...))
			require.NoError(t, cmd.Execute())

			require.Equal(t, tt.expected, testVar)
		}

		t.Run(tt.name, fn)
	}
}

func setEnvVar(key, val string) func() {
	old := os.Getenv(key)
	os.Setenv(key, val)
	return func() {
		os.Setenv(key, old)
	}
}

func newConfigFile(t *testing.T, config interface{}) (string, func()) {
	t.Helper()

	testDir, err := ioutil.TempDir("", "")
	require.NoError(t, err)

	b, err := json.Marshal(config)
	require.NoError(t, err)

	testFile := path.Join(testDir, "config.json")
	require.NoError(t, ioutil.WriteFile(testFile, b, os.ModePerm))

	return testFile, func() {
		os.RemoveAll(testDir)
	}
}

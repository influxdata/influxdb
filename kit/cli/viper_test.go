package cli

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
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
	var smallerNumber int32
	var longerNumber int64
	var sleep bool
	var duration time.Duration
	var stringSlice []string
	var fancyBool customFlag
	var logLevel zapcore.Level
	cmd, err := NewCommand(viper.New(), &Program{
		Run: func() error {
			fmt.Println(monitorHost)
			for i := 0; i < number; i++ {
				fmt.Printf("%d\n", i)
			}
			fmt.Println(longerNumber - int64(smallerNumber))
			fmt.Println(sleep)
			fmt.Println(duration)
			fmt.Println(stringSlice)
			fmt.Println(fancyBool)
			fmt.Println(logLevel.String())
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
				DestP:   &smallerNumber,
				Flag:    "smaller-number",
				Default: math.MaxInt32,
				Desc:    "limited size number",
			},
			{
				DestP:   &longerNumber,
				Flag:    "longer-number",
				Default: math.MaxInt64,
				Desc:    "explicitly expanded-size number",
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
			{
				DestP:   &logLevel,
				Flag:    "log-level",
				Default: zapcore.WarnLevel,
			},
		},
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
	}

	cmd.SetArgs([]string{})
	if err := cmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
	// Output:
	// http://localhost:8086
	// 0
	// 1
	// 9223372034707292160
	// true
	// 1m0s
	// [foo bar]
	// on
	// warn
}

func Test_NewProgram(t *testing.T) {
	testFilePath, cleanup := newConfigFile(t, map[string]string{
		// config values should be same as flags
		"foo":         "bar",
		"shoe-fly":    "yadon",
		"number":      "2147483647",
		"long-number": "9223372036854775807",
		"log-level":   "debug",
	})
	defer cleanup()
	defer setEnvVar("TEST_CONFIG_PATH", testFilePath)()

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
			var testFly string
			var testNumber int32
			var testLongNumber int64
			var logLevel zapcore.Level
			program := &Program{
				Name: "test",
				Opts: []Opt{
					{
						DestP:    &testVar,
						Flag:     "foo",
						Required: true,
					},
					{
						DestP: &testFly,
						Flag:  "shoe-fly",
					},
					{
						DestP: &testNumber,
						Flag:  "number",
					},
					{
						DestP: &testLongNumber,
						Flag:  "long-number",
					},
					{
						DestP: &logLevel,
						Flag:  "log-level",
					},
				},
				Run: func() error { return nil },
			}

			cmd, err := NewCommand(viper.New(), program)
			require.NoError(t, err)
			cmd.SetArgs(append([]string{}, tt.args...))
			require.NoError(t, cmd.Execute())

			require.Equal(t, tt.expected, testVar)
			assert.Equal(t, "yadon", testFly)
			assert.Equal(t, int32(math.MaxInt32), testNumber)
			assert.Equal(t, int64(math.MaxInt64), testLongNumber)
			assert.Equal(t, zapcore.DebugLevel, logLevel)
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

func Test_RequiredFlag(t *testing.T) {
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
	}

	cmd, err := NewCommand(viper.New(), program)
	require.NoError(t, err)
	cmd.SetArgs([]string{})
	err = cmd.Execute()
	require.Error(t, err)
	require.Equal(t, `required flag(s) "foo" not set`, err.Error())
}

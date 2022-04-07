package cli

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
	"gopkg.in/yaml.v3"
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
		_, _ = fmt.Fprintln(os.Stderr, err)
		return
	}

	cmd.SetArgs([]string{})
	if err := cmd.Execute(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
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
	config := map[string]string{
		// config values should be same as flags
		"foo":         "bar",
		"shoe-fly":    "yadon",
		"number":      "2147483647",
		"long-number": "9223372036854775807",
		"log-level":   "debug",
	}

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
		for _, writer := range configWriters {
			fn := func(t *testing.T) {
				testDir := t.TempDir()

				confFile, err := writer.writeFn(testDir, config)
				require.NoError(t, err)

				defer setEnvVar("TEST_CONFIG_PATH", confFile)()

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

			t.Run(fmt.Sprintf("%s_%s", tt.name, writer.ext), fn)
		}
	}
}

func setEnvVar(key, val string) func() {
	old := os.Getenv(key)
	os.Setenv(key, val)
	return func() {
		os.Setenv(key, old)
	}
}

type configWriter func(dir string, config interface{}) (string, error)
type labeledWriter struct {
	ext     string
	writeFn configWriter
}

var configWriters = []labeledWriter{
	{ext: "json", writeFn: writeJsonConfig},
	{ext: "toml", writeFn: writeTomlConfig},
	{ext: "yml", writeFn: yamlConfigWriter(true)},
	{ext: "yaml", writeFn: yamlConfigWriter(false)},
}

func writeJsonConfig(dir string, config interface{}) (string, error) {
	b, err := json.Marshal(config)
	if err != nil {
		return "", err
	}
	confFile := path.Join(dir, "config.json")
	if err := os.WriteFile(confFile, b, os.ModePerm); err != nil {
		return "", err
	}
	return confFile, nil
}

func writeTomlConfig(dir string, config interface{}) (string, error) {
	confFile := path.Join(dir, "config.toml")
	w, err := os.OpenFile(confFile, os.O_CREATE|os.O_EXCL|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return "", err
	}
	defer w.Close()

	if err := toml.NewEncoder(w).Encode(config); err != nil {
		return "", err
	}

	return confFile, nil
}

func yamlConfigWriter(shortExt bool) configWriter {
	fileName := "config.yaml"
	if shortExt {
		fileName = "config.yml"
	}

	return func(dir string, config interface{}) (string, error) {
		confFile := path.Join(dir, fileName)
		w, err := os.OpenFile(confFile, os.O_CREATE|os.O_EXCL|os.O_WRONLY, os.ModePerm)
		if err != nil {
			return "", err
		}
		defer w.Close()

		if err := yaml.NewEncoder(w).Encode(config); err != nil {
			return "", err
		}

		return confFile, nil
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

func Test_ConfigPrecedence(t *testing.T) {
	jsonConfig := map[string]interface{}{"log-level": zapcore.DebugLevel}
	tomlConfig := map[string]interface{}{"log-level": zapcore.InfoLevel}
	yamlConfig := map[string]interface{}{"log-level": zapcore.WarnLevel}
	ymlConfig := map[string]interface{}{"log-level": zapcore.ErrorLevel}

	tests := []struct {
		name          string
		writeJson     bool
		writeToml     bool
		writeYaml     bool
		writeYml      bool
		expectedLevel zapcore.Level
	}{
		{
			name:          "JSON is used if present",
			writeJson:     true,
			writeToml:     true,
			writeYaml:     true,
			writeYml:      true,
			expectedLevel: zapcore.DebugLevel,
		},
		{
			name:          "TOML is used if no JSON present",
			writeJson:     false,
			writeToml:     true,
			writeYaml:     true,
			writeYml:      true,
			expectedLevel: zapcore.InfoLevel,
		},
		{
			name:          "YAML is used if no JSON or TOML present",
			writeJson:     false,
			writeToml:     false,
			writeYaml:     true,
			writeYml:      true,
			expectedLevel: zapcore.WarnLevel,
		},
		{
			name:          "YML is used if no other option present",
			writeJson:     false,
			writeToml:     false,
			writeYaml:     false,
			writeYml:      true,
			expectedLevel: zapcore.ErrorLevel,
		},
	}

	for _, tt := range tests {
		fn := func(t *testing.T) {
			testDir := t.TempDir()
			defer setEnvVar("TEST_CONFIG_PATH", testDir)()

			if tt.writeJson {
				_, err := writeJsonConfig(testDir, jsonConfig)
				require.NoError(t, err)
			}
			if tt.writeToml {
				_, err := writeTomlConfig(testDir, tomlConfig)
				require.NoError(t, err)
			}
			if tt.writeYaml {
				_, err := yamlConfigWriter(false)(testDir, yamlConfig)
				require.NoError(t, err)
			}
			if tt.writeYml {
				_, err := yamlConfigWriter(true)(testDir, ymlConfig)
				require.NoError(t, err)
			}

			var logLevel zapcore.Level
			program := &Program{
				Name: "test",
				Opts: []Opt{
					{
						DestP: &logLevel,
						Flag:  "log-level",
					},
				},
				Run: func() error { return nil },
			}

			cmd, err := NewCommand(viper.New(), program)
			require.NoError(t, err)
			cmd.SetArgs([]string{})
			require.NoError(t, cmd.Execute())

			require.Equal(t, tt.expectedLevel, logLevel)
		}

		t.Run(tt.name, fn)
	}
}

func Test_ConfigPathDotDirectory(t *testing.T) {
	testDir := t.TempDir()

	tests := []struct {
		name string
		dir  string
	}{
		{
			name: "dot at start",
			dir:  ".directory",
		},
		{
			name: "dot in middle",
			dir:  "config.d",
		},
		{
			name: "dot at end",
			dir:  "forgotmyextension.",
		},
	}

	config := map[string]string{
		"foo": "bar",
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			configDir := filepath.Join(testDir, tc.dir)
			require.NoError(t, os.Mkdir(configDir, 0700))

			_, err := writeTomlConfig(configDir, config)
			require.NoError(t, err)
			defer setEnvVar("TEST_CONFIG_PATH", configDir)()

			var foo string
			program := &Program{
				Name: "test",
				Opts: []Opt{
					{
						DestP: &foo,
						Flag:  "foo",
					},
				},
				Run: func() error { return nil },
			}

			cmd, err := NewCommand(viper.New(), program)
			require.NoError(t, err)
			cmd.SetArgs([]string{})
			require.NoError(t, cmd.Execute())

			require.Equal(t, "bar", foo)
		})
	}
}

func Test_LoadConfigCwd(t *testing.T) {
	testDir := t.TempDir()

	pwd, err := os.Getwd()
	require.NoError(t, err)
	defer os.Chdir(pwd)

	require.NoError(t, os.Chdir(testDir))

	config := map[string]string{
		"foo": "bar",
	}
	_, err = writeJsonConfig(testDir, config)
	require.NoError(t, err)

	var foo string
	program := &Program{
		Name: "test",
		Opts: []Opt{
			{
				DestP: &foo,
				Flag:  "foo",
			},
		},
		Run: func() error { return nil },
	}

	cmd, err := NewCommand(viper.New(), program)
	require.NoError(t, err)
	cmd.SetArgs([]string{})
	require.NoError(t, cmd.Execute())

	require.Equal(t, "bar", foo)
}

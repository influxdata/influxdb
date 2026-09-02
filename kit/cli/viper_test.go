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
	"github.com/influxdata/influxdb/v2/kit/exit"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/spf13/cobra"
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

// strictFlag is a pflag.Value that rejects what it does not recognize, which
// customFlag above cannot: its Set silently treats every unknown value as
// "off". The real option types behave like this one -- toml.Duration and
// toml.Size both refuse to parse garbage -- so a test for what BindOptions does
// with a rejected value needs a Value that can reject.
type strictFlag string

func (s strictFlag) String() string { return string(s) }

func (s *strictFlag) Set(v string) error {
	if v != "yes" && v != "no" {
		return fmt.Errorf("unrecognized value %q; expected yes or no", v)
	}
	*s = strictFlag(v)
	return nil
}

func (s *strictFlag) Type() string { return "strict-bool" }

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

// Test_EnvValueRejected covers a value supplied through the environment that
// the option it sets will not accept.
//
// Every branch of BindOptions used to discard the error: the server started on
// the default and exited 0, so INFLUXD_LOG_LEVEL=trace logged at info and said
// nothing, while --log-level=trace exits EX_USAGE. An environment variable is
// one of the three documented ways to set every one of these options, and a
// wrong one has to be as reportable as a wrong flag.
//
// The cases below run one option of each shape BindOptions switches on: the
// three that parse the value themselves, and the primitives it hands to cast.
// A primitive is the easier one to leave discarding, because cast accepts so
// much -- but not, for a bool, the "yes" and "on" an operator will reach for.
func Test_EnvValueRejected(t *testing.T) {
	tests := []struct {
		name  string
		env   string
		value string
		opt   func() Opt
	}{
		{
			name:  "log level",
			env:   "TEST_LOG_LEVEL",
			value: "trace",
			opt: func() Opt {
				var level zapcore.Level
				return Opt{DestP: &level, Flag: "log-level"}
			},
		},
		{
			name:  "id",
			env:   "TEST_ORG_ID",
			value: "not-an-id",
			opt: func() Opt {
				var id platform.ID
				return Opt{DestP: &id, Flag: "org-id"}
			},
		},
		{
			// pflag.Value covers the option types that parse themselves --
			// toml.Size and friends -- through a single branch.
			name:  "pflag value",
			env:   "TEST_CUSTOM",
			value: "maybe",
			opt: func() Opt {
				var s strictFlag
				return Opt{DestP: &s, Flag: "custom"}
			},
		},
		{
			// strconv.ParseBool, which is what both cast and pflag use, takes
			// neither "yes" nor "on". Discarding this one left the server
			// reporting while the operator believed it had been turned off.
			name:  "bool",
			env:   "TEST_REPORTING_DISABLED",
			value: "yes",
			opt: func() Opt {
				var b bool
				return Opt{DestP: &b, Flag: "reporting-disabled"}
			},
		},
		{
			name:  "int",
			env:   "TEST_QUERY_CONCURRENCY",
			value: "as many as it takes",
			opt: func() Opt {
				var i int
				return Opt{DestP: &i, Flag: "query-concurrency"}
			},
		},
		{
			name:  "duration",
			env:   "TEST_STORAGE_RETENTION_CHECK_INTERVAL",
			value: "30 minutes",
			opt: func() Opt {
				var d time.Duration
				return Opt{DestP: &d, Flag: "storage-retention-check-interval"}
			},
		},
		{
			// cast read a unitless value as nanoseconds, so this one used to
			// start a retention enforcer that ran every 300ns. pflag refuses
			// it on the command line and now refuses it here.
			name:  "unitless duration",
			env:   "TEST_STORAGE_RETENTION_CHECK_INTERVAL",
			value: "300",
			opt: func() Opt {
				var d time.Duration
				return Opt{DestP: &d, Flag: "storage-retention-check-interval"}
			},
		},
		{
			// cast parses at 64 bits and converts, so this one used to arrive
			// as a concurrency quota of -1294967296 and start the server.
			name:  "int32 out of range",
			env:   "TEST_QUERY_CONCURRENCY",
			value: "3000000000",
			opt: func() Opt {
				var i int32
				return Opt{DestP: &i, Flag: "query-concurrency"}
			},
		},
		{
			// The map branch takes the k=v form and JSON; a bare word is
			// neither, and the message pflag returns for it is the one an
			// operator gets from the flag.
			name:  "string map",
			env:   "TEST_FEATURE_FLAGS",
			value: "someFlag",
			opt: func() Opt {
				var m map[string]string
				return Opt{DestP: &m, Flag: "feature-flags"}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer setEnvVar(tt.env, tt.value)()

			program := &Program{
				Name: "test",
				Opts: []Opt{tt.opt()},
				Run:  func() error { return nil },
			}

			_, err := NewCommand(viper.New(), program)
			require.Error(t, err, "a value the option rejects must not be discarded")
			require.Equal(t, exit.CodeConfig, exit.Code(err),
				"an unusable environment value must exit %s: the fix is to edit "+
					"whatever exports it, so a supervisor set to stop retrying on a "+
					"config error must not restart into it", exit.Name(exit.CodeConfig))
			require.ErrorContains(t, err, tt.value, "the message must name the value")
		})
	}
}

// Test_EnvValueStringMap covers the forms a map option accepts from the
// environment. cast reads a string as JSON alone, which is not the form
// --feature-flags documents, so k=v used to be discarded in silence: the
// operator got no flags and no complaint.
func Test_EnvValueStringMap(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		expected map[string]string
	}{
		{
			name:     "k=v",
			value:    "someFlag=true",
			expected: map[string]string{"someFlag": "true"},
		},
		{
			name:     "k=v list",
			value:    "someFlag=true,otherFlag=false",
			expected: map[string]string{"someFlag": "true", "otherFlag": "false"},
		},
		{
			// What viper hands back for a flag it has already bound, which is
			// how every option arrives on the second and third BindOptions.
			name:     "as pflag prints it",
			value:    "[someFlag=true]",
			expected: map[string]string{"someFlag": "true"},
		},
		{
			name:     "empty as pflag prints it",
			value:    "[]",
			expected: map[string]string{},
		},
		{
			name:     "json",
			value:    `{"someFlag":"true"}`,
			expected: map[string]string{"someFlag": "true"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer setEnvVar("TEST_FEATURE_FLAGS", tt.value)()

			var flags map[string]string
			program := &Program{
				Name: "test",
				Opts: []Opt{{DestP: &flags, Flag: "feature-flags"}},
				Run:  func() error { return nil },
			}

			_, err := NewCommand(viper.New(), program)
			require.NoError(t, err)
			require.Equal(t, tt.expected, flags)
		})
	}
}

// Test_EnvValueStringSlice covers the forms a list option accepts from the
// environment. cast splits a string on whitespace while the flag splits on
// commas, so INFLUXD_MEASUREMENT=cpu,mem used to arrive as one element that
// matches no measurement: nothing exported, and nothing said about it.
func Test_EnvValueStringSlice(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		expected []string
	}{
		{
			name:     "comma separated",
			value:    "cpu,mem",
			expected: []string{"cpu", "mem"},
		},
		{
			name:     "single element",
			value:    "cpu",
			expected: []string{"cpu"},
		},
		{
			// Whitespace is part of the element, as it is for the flag. cast
			// split here instead, turning one name into two.
			name:     "space in an element",
			value:    "cpu load,mem",
			expected: []string{"cpu load", "mem"},
		},
		{
			// An element holding a comma quotes the way it does on the
			// command line: both read the value as one CSV record.
			name:     "quoted element",
			value:    `"cpu,mem",disk`,
			expected: []string{"cpu,mem", "disk"},
		},
		{
			// What viper hands back for a flag it has already bound, which is
			// how every option arrives on the second and third BindOptions.
			name:     "as pflag prints it",
			value:    "[cpu,mem]",
			expected: []string{"cpu", "mem"},
		},
		{
			name:     "empty as pflag prints it",
			value:    "[]",
			expected: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer setEnvVar("TEST_MEASUREMENT", tt.value)()

			var measurements []string
			program := &Program{
				Name: "test",
				Opts: []Opt{{DestP: &measurements, Flag: "measurement"}},
				Run:  func() error { return nil },
			}

			_, err := NewCommand(viper.New(), program)
			require.NoError(t, err)
			require.Equal(t, tt.expected, measurements)
		})
	}
}

// Test_BindOptions_Rebind covers binding one set of options onto a second
// command through the same viper, which is what influxd does for run and
// print-config.
//
// viper answers a key it has no value for with the default of a flag it has
// already bound, rendered as a string, so the second pass reads every option
// back through the same conversion an operator's value takes. Rejecting a value
// there rather than discarding it means a type whose default does not survive
// the round trip stops the server -- and stops it for `influxd version`, since
// the tree is built before the command line is read.
func Test_BindOptions_Rebind(t *testing.T) {
	var (
		name     string
		count    int
		enabled  bool
		interval time.Duration
		hosts    []string
		flags    map[string]string
		fancy    customFlag
	)
	opts := []Opt{
		{DestP: &name, Flag: "name", Default: "influxd"},
		{DestP: &count, Flag: "count", Default: 3},
		{DestP: &enabled, Flag: "enabled", Default: true},
		{DestP: &interval, Flag: "interval", Default: time.Minute},
		{DestP: &hosts, Flag: "hosts", Default: []string{"a", "b"}},
		{DestP: &flags, Flag: "feature-flags"},
		{DestP: &fancy, Flag: "fancy-bool", Default: "on"},
	}

	v := viper.New()
	first, err := NewCommand(v, &Program{Name: "test", Opts: opts, Run: func() error { return nil }})
	require.NoError(t, err)

	second := &cobra.Command{Use: "run"}
	require.NoError(t, BindOptions(v, second, opts), "rebinding must not reject a default of its own")
	first.AddCommand(second)

	assert.Equal(t, "influxd", name)
	assert.Equal(t, 3, count)
	assert.True(t, enabled)
	assert.Equal(t, time.Minute, interval)
	assert.Equal(t, []string{"a", "b"}, hosts)
	assert.Empty(t, flags)
	assert.Equal(t, customFlag(true), fancy)
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

// Test_PFlagValueTypedDefault confirms the pflag.Value binding path
// resolves Defaults of various concrete types into the destP value: a
// string Default goes through the cast.ToStringE → Set path, a typed
// Default whose type matches destP takes the direct-copy fast path, and
// a Default whose type neither matches destP nor casts to a string is
// rejected at bind time with an error naming the flag and offending type.
func Test_PFlagValueTypedDefault(t *testing.T) {
	tests := []struct {
		name             string
		dflt             interface{}
		want             customFlag
		wantBindErrParts []string // non-empty means NewCommand is expected to fail
	}{
		{name: "string default still works", dflt: "on", want: customFlag(true)},
		{name: "typed Stringer default", dflt: customFlag(true), want: customFlag(true)},
		{
			name:             "non-stringable default is rejected",
			dflt:             struct{}{},
			wantBindErrParts: []string{`flag "fancy-bool"`, "cannot resolve Default", "struct {}"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got customFlag
			cmd, err := NewCommand(viper.New(), &Program{
				Name: "test",
				Run:  func() error { return nil },
				Opts: []Opt{
					{DestP: &got, Flag: "fancy-bool", Default: tt.dflt},
				},
			})
			if len(tt.wantBindErrParts) > 0 {
				require.Error(t, err)
				for _, p := range tt.wantBindErrParts {
					require.Contains(t, err.Error(), p)
				}
				return
			}
			require.NoError(t, err)
			cmd.SetArgs([]string{})
			require.NoError(t, cmd.Execute())
			require.Equal(t, tt.want, got)
		})
	}
}

// lossyValue is a pflag.Value whose String() form is deliberately not
// reversible by Set(): String returns a constant token, and Set ignores its
// input and assigns a sentinel value. This makes any Stringer→Set round-trip
// observable: if the kit ever regresses to round-tripping typed Defaults
// through cast.ToStringE, dest will land on the sentinel instead of the
// caller's value.
type lossyValue uint64

func (v lossyValue) String() string    { return "lossy" }
func (v *lossyValue) Set(string) error { *v = 999; return nil }
func (v *lossyValue) Type() string     { return "lossy" }

// Test_PFlagValueTypedDefault_BypassesLossyStringer pins down that when
// Default's concrete type matches destP's pointee type, the kit copies the
// value directly rather than going through Stringer→Set. Without that
// bypass, the Default would be silently corrupted for any pflag.Value type
// whose String() is lossy (the motivating real-world case is toml.Size and
// toml.SSize, whose humanize.IBytes formatting drifts non-power-of-2 byte
// counts across a marshal/unmarshal cycle).
func Test_PFlagValueTypedDefault_BypassesLossyStringer(t *testing.T) {
	var got lossyValue
	cmd, err := NewCommand(viper.New(), &Program{
		Name: "test",
		Run:  func() error { return nil },
		Opts: []Opt{
			{DestP: &got, Flag: "lossy", Default: lossyValue(42)},
		},
	})
	require.NoError(t, err)
	cmd.SetArgs([]string{})
	require.NoError(t, cmd.Execute())
	require.Equal(t, lossyValue(42), got, "typed Default should be copied directly, not round-tripped through Stringer/Set")
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

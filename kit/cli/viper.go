package cli

import (
	"fmt"
	"math"
	"os"
	"path"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/influxdb/v2/kit/exit"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/spf13/cast"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.uber.org/zap/zapcore"
)

// Opt is a single command-line option
type Opt struct {
	DestP interface{} // pointer to the destination

	EnvVar     string
	Flag       string
	Hidden     bool
	Persistent bool
	Required   bool
	Short      rune // using rune b/c it guarantees correctness. a short must always be a string of length 1

	Default interface{}
	Desc    string
}

// Program parses CLI options
type Program struct {
	// Run is invoked by cobra on execute.
	Run func() error
	// Name is the name of the program in help usage and the env var prefix.
	Name string
	// Opts are the command line/env var options to the program
	Opts []Opt
}

// UsageArgs adapts a cobra positional-argument validator so that a violation
// carries exit.CodeUsage.
//
// Cobra's SetFlagErrorFunc covers a bad flag but not a bad positional argument,
// and both are the same mistake from an operator's point of view: the command
// line is wrong and re-running it unchanged cannot help.
func UsageArgs(validate cobra.PositionalArgs) cobra.PositionalArgs {
	return func(cmd *cobra.Command, args []string) error {
		// exit.WithCode passes a nil error through, so the accepting case
		// needs no guard here.
		return exit.WithCode(exit.CodeUsage, validate(cmd, args))
	}
}

// errEnvValue reports a configured value that the option it was destined for
// would not accept.
//
// The status is exit.CodeConfig rather than exit.CodeUsage: the command line is
// not what is wrong, and the fix is to edit whatever supplies the value.
// Reporting anything at all is the point -- every branch of BindOptions used to
// discard this error, so INFLUXD_LOG_LEVEL=trace started the server at info and
// exited 0, and INFLUXD_REPORTING_DISABLED=yes left reporting on just as
// quietly, while --log-level=trace exits 64. An environment variable is one of
// the three documented ways to set every one of these options, and a wrong one
// has to be as reportable as a wrong flag.
//
// The value reaches BindOptions through lookupEnv, which reads viper rather
// than the environment alone, so a config file key resolves here too and the
// message names both sources.
func errEnvValue(flag string, value interface{}, err error) error {
	return exit.WithCode(exit.CodeConfig,
		fmt.Errorf("invalid value %q for %q from the environment or config file: %w",
			fmt.Sprint(value), flag, err))
}

// NewCommand creates a new cobra command to be executed that respects env vars.
//
// Uses the upper-case version of the program's name as a prefix
// to all environment variables.
//
// This is to simplify the viper/cobra boilerplate.
func NewCommand(v *viper.Viper, p *Program) (*cobra.Command, error) {
	cmd := &cobra.Command{
		Use:  p.Name,
		Args: UsageArgs(cobra.NoArgs),
		RunE: func(_ *cobra.Command, _ []string) error {
			return p.Run()
		},
	}

	v.SetEnvPrefix(strings.ToUpper(p.Name))
	v.AutomaticEnv()
	// This normalizes "-" to an underscore in env names.
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))

	// done before we bind flags to viper keys.
	// order of precedence (1 highest -> 3 lowest):
	//	1. flags
	//  2. env vars
	//	3. config file
	if err := initializeConfig(v); err != nil {
		// An unreadable or unparseable config file will read the same way on
		// the next start, so the status marks it as configuration rather than
		// as a generic failure a supervisor should retry.
		return nil, exit.WithCode(exit.CodeConfig, fmt.Errorf("failed to load config file: %w", err))
	}
	if err := BindOptions(v, cmd, p.Opts); err != nil {
		return nil, fmt.Errorf("failed to bind config options: %w", err)
	}

	return cmd, nil
}

func initializeConfig(v *viper.Viper) error {
	configPath := v.GetString("CONFIG_PATH")
	if configPath == "" {
		// Default to looking in the working directory of the running process.
		configPath = "."
	}

	switch strings.ToLower(path.Ext(configPath)) {
	case ".json", ".toml", ".yaml", ".yml":
		v.SetConfigFile(configPath)
	default:
		v.AddConfigPath(configPath)
	}

	if err := v.ReadInConfig(); err != nil && !os.IsNotExist(err) {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return err
		}
	}
	return nil
}

// BindOptions adds opts to the specified command and automatically
// registers those options with viper.
func BindOptions(v *viper.Viper, cmd *cobra.Command, opts []Opt) error {
	for _, o := range opts {
		flagset := cmd.Flags()
		if o.Persistent {
			flagset = cmd.PersistentFlags()
		}
		envVal := lookupEnv(v, &o)
		hasShort := o.Short != 0

		switch destP := o.DestP.(type) {
		case *string:
			var d string
			if o.Default != nil {
				d = o.Default.(string)
			}
			if hasShort {
				flagset.StringVarP(destP, o.Flag, string(o.Short), d, o.Desc)
			} else {
				flagset.StringVar(destP, o.Flag, d, o.Desc)
			}
			if err := v.BindPFlag(o.Flag, flagset.Lookup(o.Flag)); err != nil {
				return fmt.Errorf("failed to bind flag %q: %w", o.Flag, err)
			}
			if envVal != nil {
				s, err := cast.ToStringE(envVal)
				if err != nil {
					return errEnvValue(o.Flag, envVal, err)
				}
				*destP = s
			}

		case *int:
			var d int
			if o.Default != nil {
				d = o.Default.(int)
			}
			if hasShort {
				flagset.IntVarP(destP, o.Flag, string(o.Short), d, o.Desc)
			} else {
				flagset.IntVar(destP, o.Flag, d, o.Desc)
			}
			if err := v.BindPFlag(o.Flag, flagset.Lookup(o.Flag)); err != nil {
				return fmt.Errorf("failed to bind flag %q: %w", o.Flag, err)
			}
			if envVal != nil {
				i, err := cast.ToIntE(envVal)
				if err != nil {
					return errEnvValue(o.Flag, envVal, err)
				}
				*destP = i
			}

		case *int32:
			var d int32
			if o.Default != nil {
				// N.B. since our CLI kit types default values as interface{} and
				// literal numbers get typed as int by default, it's very easy to
				// create an int32 CLI flag with an int default value.
				//
				// The compiler doesn't know to complain in that case, so you end up
				// with a runtime panic when trying to bind the CLI options.
				//
				// To avoid that headache, we support both int32 and int defaults
				// for int32 fields. This introduces a new runtime bomb if somebody
				// specifies an int default > math.MaxInt32, but that's hopefully
				// less likely.
				var ok bool
				d, ok = o.Default.(int32)
				if !ok {
					d = int32(o.Default.(int))
				}
			}
			if hasShort {
				flagset.Int32VarP(destP, o.Flag, string(o.Short), d, o.Desc)
			} else {
				flagset.Int32Var(destP, o.Flag, d, o.Desc)
			}
			if err := v.BindPFlag(o.Flag, flagset.Lookup(o.Flag)); err != nil {
				return fmt.Errorf("failed to bind flag %q: %w", o.Flag, err)
			}
			if envVal != nil {
				i, err := toInt32(envVal)
				if err != nil {
					return errEnvValue(o.Flag, envVal, err)
				}
				*destP = i
			}

		case *int64:
			var d int64
			if o.Default != nil {
				// N.B. since our CLI kit types default values as interface{} and
				// literal numbers get typed as int by default, it's very easy to
				// create an int64 CLI flag with an int default value.
				//
				// The compiler doesn't know to complain in that case, so you end up
				// with a runtime panic when trying to bind the CLI options.
				//
				// To avoid that headache, we support both int64 and int defaults
				// for int64 fields.
				var ok bool
				d, ok = o.Default.(int64)
				if !ok {
					d = int64(o.Default.(int))
				}
			}
			if hasShort {
				flagset.Int64VarP(destP, o.Flag, string(o.Short), d, o.Desc)
			} else {
				flagset.Int64Var(destP, o.Flag, d, o.Desc)
			}
			if err := v.BindPFlag(o.Flag, flagset.Lookup(o.Flag)); err != nil {
				return fmt.Errorf("failed to bind flag %q: %w", o.Flag, err)
			}
			if envVal != nil {
				i, err := cast.ToInt64E(envVal)
				if err != nil {
					return errEnvValue(o.Flag, envVal, err)
				}
				*destP = i
			}

		case *bool:
			var d bool
			if o.Default != nil {
				d = o.Default.(bool)
			}
			if hasShort {
				flagset.BoolVarP(destP, o.Flag, string(o.Short), d, o.Desc)
			} else {
				flagset.BoolVar(destP, o.Flag, d, o.Desc)
			}
			if err := v.BindPFlag(o.Flag, flagset.Lookup(o.Flag)); err != nil {
				return fmt.Errorf("failed to bind flag %q: %w", o.Flag, err)
			}
			if envVal != nil {
				b, err := cast.ToBoolE(envVal)
				if err != nil {
					return errEnvValue(o.Flag, envVal, err)
				}
				*destP = b
			}

		case *time.Duration:
			var d time.Duration
			if o.Default != nil {
				d = o.Default.(time.Duration)
			}
			if hasShort {
				flagset.DurationVarP(destP, o.Flag, string(o.Short), d, o.Desc)
			} else {
				flagset.DurationVar(destP, o.Flag, d, o.Desc)
			}
			if err := v.BindPFlag(o.Flag, flagset.Lookup(o.Flag)); err != nil {
				return fmt.Errorf("failed to bind flag %q: %w", o.Flag, err)
			}
			if envVal != nil {
				dur, err := toDuration(envVal)
				if err != nil {
					return errEnvValue(o.Flag, envVal, err)
				}
				*destP = dur
			}

		case *[]string:
			var d []string
			if o.Default != nil {
				d = o.Default.([]string)
			}
			if hasShort {
				flagset.StringSliceVarP(destP, o.Flag, string(o.Short), d, o.Desc)
			} else {
				flagset.StringSliceVar(destP, o.Flag, d, o.Desc)
			}
			if err := v.BindPFlag(o.Flag, flagset.Lookup(o.Flag)); err != nil {
				return fmt.Errorf("failed to bind flag %q: %w", o.Flag, err)
			}
			if envVal != nil {
				ss, err := toStringSlice(envVal)
				if err != nil {
					return errEnvValue(o.Flag, envVal, err)
				}
				*destP = ss
			}

		case *map[string]string:
			var d map[string]string
			if o.Default != nil {
				d = o.Default.(map[string]string)
			}
			if hasShort {
				flagset.StringToStringVarP(destP, o.Flag, string(o.Short), d, o.Desc)
			} else {
				flagset.StringToStringVar(destP, o.Flag, d, o.Desc)
			}
			if err := v.BindPFlag(o.Flag, flagset.Lookup(o.Flag)); err != nil {
				return fmt.Errorf("failed to bind flag %q: %w", o.Flag, err)
			}
			if envVal != nil {
				sms, err := toStringMapString(envVal)
				if err != nil {
					return errEnvValue(o.Flag, envVal, err)
				}
				*destP = sms
			}

		case pflag.Value:
			if hasShort {
				flagset.VarP(destP, o.Flag, string(o.Short), o.Desc)
			} else {
				flagset.Var(destP, o.Flag, o.Desc)
			}
			if o.Default != nil {
				// Prefer a direct value copy when Default's concrete type
				// matches destP's pointee type. This bypasses the Stringer →
				// Set round-trip, which is lossy for types whose String() goes
				// through humanize.IBytes (toml.Size / toml.SSize emit a
				// rounded form like "24 MiB" for non-power-of-2 byte counts,
				// which then parses back to a different value). Falling
				// through to cast.ToStringE keeps Defaults like a plain string
				// or untyped int working for callers whose Default differs in
				// type from DestP — e.g. Default: "on" for a customFlag.
				dv := reflect.ValueOf(o.Default)
				pv := reflect.ValueOf(destP)
				if dv.IsValid() && pv.Kind() == reflect.Ptr && dv.Type() == pv.Elem().Type() {
					pv.Elem().Set(dv)
				} else {
					s, err := cast.ToStringE(o.Default)
					if err != nil {
						return fmt.Errorf("flag %q: cannot resolve Default of type %T: %w", o.Flag, o.Default, err)
					}
					// A Default the option itself rejects is a mistake in the
					// option's definition, not in anything an operator did, so
					// it stays an unclassified error like the cast failure
					// above. Silently keeping the zero value instead would ship
					// a flag whose documented default is not its actual one.
					if err := destP.Set(s); err != nil {
						return fmt.Errorf("flag %q: cannot apply Default %q: %w", o.Flag, s, err)
					}
				}
			}
			if err := v.BindPFlag(o.Flag, flagset.Lookup(o.Flag)); err != nil {
				return fmt.Errorf("failed to bind flag %q: %w", o.Flag, err)
			}
			if envVal != nil {
				s, err := cast.ToStringE(envVal)
				if err != nil {
					return errEnvValue(o.Flag, envVal, err)
				}
				if err := destP.Set(s); err != nil {
					return errEnvValue(o.Flag, s, err)
				}
			}

		case *platform.ID:
			var d platform.ID
			if o.Default != nil {
				d = o.Default.(platform.ID)
			}
			if hasShort {
				IDVarP(flagset, destP, o.Flag, string(o.Short), d, o.Desc)
			} else {
				IDVar(flagset, destP, o.Flag, d, o.Desc)
			}
			if envVal != nil {
				s, err := cast.ToStringE(envVal)
				if err != nil {
					return errEnvValue(o.Flag, envVal, err)
				}
				if err := (*destP).DecodeFromString(s); err != nil {
					return errEnvValue(o.Flag, s, err)
				}
			}

		case *zapcore.Level:
			var l zapcore.Level
			if o.Default != nil {
				l = o.Default.(zapcore.Level)
			}
			if hasShort {
				LevelVarP(flagset, destP, o.Flag, string(o.Short), l, o.Desc)
			} else {
				LevelVar(flagset, destP, o.Flag, l, o.Desc)
			}
			if envVal != nil {
				s, err := cast.ToStringE(envVal)
				if err != nil {
					return errEnvValue(o.Flag, envVal, err)
				}
				if err := (*destP).Set(s); err != nil {
					return errEnvValue(o.Flag, s, err)
				}
			}

		default:
			// if you get this error, sorry about that!
			// anyway, go ahead and make a PR and add another type.
			return fmt.Errorf("unknown destination type %t", o.DestP)
		}

		// N.B. these "Mark" calls must run after the block above,
		// otherwise cobra will return a "no such flag" error.

		// Cobra will complain if a flag marked as required isn't present on the CLI.
		// To support setting required args via config and env variables, we only enforce
		// the required check if we didn't find a value in the viper instance.
		if o.Required && envVal == nil {
			if err := cmd.MarkFlagRequired(o.Flag); err != nil {
				return fmt.Errorf("failed to mark flag %q as required: %w", o.Flag, err)
			}
		}
		if o.Hidden {
			if err := flagset.MarkHidden(o.Flag); err != nil {
				return fmt.Errorf("failed to mark flag %q as hidden: %w", o.Flag, err)
			}
		}
	}

	return nil
}

// lookupEnv returns the value for a CLI option found in the environment, if any.
//
// The name understates what viper resolves: a config file key answers here too,
// and so does a flag this viper has already bound -- v.Get falls back to a
// bound flag's default value, rendered as a string, when nothing else supplies
// the key. BindOptions binds the same options onto more than one command
// (influxd and influxd run, then print-config), so on every pass after the
// first this returns a value for each flag whether or not anyone configured it.
// Re-applying a default over itself is harmless, but a caller may not read a
// non-nil result as "an operator asked for this".
func lookupEnv(v *viper.Viper, o *Opt) interface{} {
	envVar := o.Flag
	if o.EnvVar != "" {
		envVar = o.EnvVar
	}
	return v.Get(envVar)
}

// toStringMapString reads the value viper produced for a map option.
//
// cast handles the shapes a config file yields, but from a string it accepts
// only JSON, and neither string that reaches here is JSON. viper renders an
// already-bound flag the way pflag prints one -- "[a=b,c=d]", or "[]" for the
// empty map -- so rejecting that form would fail every start after the first
// bind, print-config included. An operator setting INFLUXD_FEATURE_FLAGS writes
// the k=v form too, because that is the one --feature-flags documents; parsing
// it with pflag's own flag keeps the two spellings of an option in agreement
// instead of leaving the environment to be spelled as JSON alone.
func toStringMapString(value interface{}) (map[string]string, error) {
	s, ok := value.(string)
	if !ok {
		return cast.ToStringMapStringE(value)
	}

	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "{") {
		return cast.ToStringMapStringE(s)
	}

	s = trimFlagBrackets(s)
	if s == "" {
		return map[string]string{}, nil
	}

	var m map[string]string
	fs := pflag.NewFlagSet("", pflag.ContinueOnError)
	fs.StringToStringVar(&m, "value", nil, "")
	if err := fs.Set("value", s); err != nil {
		return nil, err
	}
	return m, nil
}

// toStringSlice reads the value viper produced for a list option.
//
// cast reads a string with strings.Fields, splitting it on whitespace, but the
// flag splits on commas: --measurement=cpu,mem names two measurements while
// INFLUXD_MEASUREMENT=cpu,mem used to arrive as the single element "cpu,mem",
// which matches nothing and exports nothing without complaint. pflag reads the
// value as one CSV record, so quoting an element that contains a comma works
// here exactly as it does on the command line, and the bracketed form viper
// hands back for an already-bound flag reads back as what it was.
func toStringSlice(value interface{}) ([]string, error) {
	s, ok := value.(string)
	if !ok {
		return cast.ToStringSliceE(value)
	}

	s = trimFlagBrackets(strings.TrimSpace(s))
	if s == "" {
		return []string{}, nil
	}

	var ss []string
	fs := pflag.NewFlagSet("", pflag.ContinueOnError)
	fs.StringSliceVar(&ss, "value", nil, "")
	if err := fs.Set("value", s); err != nil {
		return nil, err
	}
	return ss, nil
}

// toDuration reads the value viper produced for a duration option.
//
// cast reads a unitless string as nanoseconds -- ToDurationE appends "ns" to
// anything with no unit letter in it -- so
// INFLUXD_STORAGE_RETENTION_CHECK_INTERVAL=300 ran the retention enforcer every
// 300ns, a spin loop, while --storage-retention-check-interval=300 exits 64
// with "missing unit in duration". A bare number in a config file meant
// nanoseconds just as quietly. time.ParseDuration is what pflag's own duration
// flag calls, so parsing with it keeps every spelling of an option in
// agreement: a duration carries a unit or it is refused. The rebind case comes
// through unharmed, since the string viper hands back for an already-bound
// flag is time.Duration.String().
func toDuration(value interface{}) (time.Duration, error) {
	switch d := value.(type) {
	case time.Duration:
		return d, nil
	case string:
		return time.ParseDuration(d)
	default:
		return 0, fmt.Errorf(
			"cannot read a %T as a duration: a duration needs a unit, as in \"30s\" or \"5m\"",
			value)
	}
}

// toInt32 reads the value viper produced for an int32 option.
//
// cast.ToInt32E has no range check -- it parses at 64 bits and then converts --
// so INFLUXD_QUERY_CONCURRENCY=3000000000 silently became a concurrency quota
// of -1294967296 and the server started with it. A quota that wrapped negative
// is worse than a start refused. pflag's int32 flag parses at 32 bits and
// rejects that value, so parsing the same way keeps the two spellings in
// agreement, base and all: a 0x or 0b prefix, or a leading zero, means in the
// environment what it means on the command line.
func toInt32(value interface{}) (int32, error) {
	if s, ok := value.(string); ok {
		i, err := strconv.ParseInt(s, 0, 32)
		if err != nil {
			return 0, err
		}
		return int32(i), nil
	}

	// A config file yields a number rather than a string, and cast converts
	// between the numeric types without a range check of its own.
	i, err := cast.ToInt64E(value)
	if err != nil {
		return 0, err
	}
	if i < math.MinInt32 || i > math.MaxInt32 {
		return 0, fmt.Errorf("%d is out of range for a 32-bit integer", i)
	}
	return int32(i), nil
}

// trimFlagBrackets removes the brackets pflag's slice and map Stringers wrap a
// value in -- "[a,b]", "[a=b,c=d]", or "[]" when it is empty -- which is the
// form viper hands back for a flag this process has already bound.
//
// The brackets come off only as a pair. A value that merely contains one is an
// operator's, not a rendered flag, and has to reach the parser intact:
// feature-flags "a=[1,2]" is a legitimate setting, and trimming either end of
// it would quietly change what it says.
func trimFlagBrackets(s string) string {
	if strings.HasPrefix(s, "[") && strings.HasSuffix(s, "]") {
		return s[1 : len(s)-1]
	}
	return s
}

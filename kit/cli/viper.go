package cli

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// Opt is a single command-line option
type Opt struct {
	DestP interface{} // pointer to the destination

	EnvVar     string
	Flag       string
	Persistent bool
	Required   bool
	Short      rune // using rune b/c it guarantees correctness. a short must always be a string of length 1

	Default interface{}
	Desc    string
}

// NewOpt creates a new command line option.
func NewOpt(destP interface{}, flag string, dflt interface{}, desc string) Opt {
	return Opt{
		DestP:   destP,
		Flag:    flag,
		Default: dflt,
		Desc:    desc,
	}
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

// NewCommand creates a new cobra command to be executed that respects env vars.
//
// Uses the upper-case version of the program's name as a prefix
// to all environment variables.
//
// This is to simplify the viper/cobra boilerplate.
func NewCommand(p *Program) *cobra.Command {
	var cmd = &cobra.Command{
		Use:  p.Name,
		Args: cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			return p.Run()
		},
	}

	viper.SetEnvPrefix(strings.ToUpper(p.Name))
	viper.AutomaticEnv()
	// This normalizes "-" to an underscore in env names.
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))

	BindOptions(cmd, p.Opts)

	return cmd
}

// BindOptions adds opts to the specified command and automatically
// registers those options with viper.
func BindOptions(cmd *cobra.Command, opts []Opt) {
	for _, o := range opts {
		flagset := cmd.Flags()
		if o.Persistent {
			flagset = cmd.PersistentFlags()
		}

		if o.Required {
			cmd.MarkFlagRequired(o.Flag)
		}

		envVar := o.Flag
		if o.EnvVar != "" {
			envVar = o.EnvVar
		}

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
			mustBindPFlag(o.Flag, flagset)
			*destP = viper.GetString(envVar)
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
			mustBindPFlag(o.Flag, flagset)
			*destP = viper.GetInt(envVar)
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
			mustBindPFlag(o.Flag, flagset)
			*destP = viper.GetBool(envVar)
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
			mustBindPFlag(o.Flag, flagset)
			*destP = viper.GetDuration(envVar)
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
			mustBindPFlag(o.Flag, flagset)
			*destP = viper.GetStringSlice(envVar)
		case pflag.Value:
			if hasShort {
				flagset.VarP(destP, o.Flag, string(o.Short), o.Desc)
			} else {
				flagset.Var(destP, o.Flag, o.Desc)
			}
			if o.Default != nil {
				destP.Set(o.Default.(string))
			}
			mustBindPFlag(o.Flag, flagset)
			destP.Set(viper.GetString(envVar))
		default:
			// if you get a panic here, sorry about that!
			// anyway, go ahead and make a PR and add another type.
			panic(fmt.Errorf("unknown destination type %t", o.DestP))
		}
	}
}

func mustBindPFlag(key string, flagset *pflag.FlagSet) {
	if err := viper.BindPFlag(key, flagset.Lookup(key)); err != nil {
		panic(err)
	}
}

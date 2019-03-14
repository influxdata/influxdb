package cli

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// Opt is a single command-line option
type Opt struct {
	DestP   interface{} // pointer to the destination
	Flag    string
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
		switch destP := o.DestP.(type) {
		case *string:
			var d string
			if o.Default != nil {
				d = o.Default.(string)
			}
			cmd.Flags().StringVar(destP, o.Flag, d, o.Desc)
			mustBindPFlag(o.Flag, cmd)
			*destP = viper.GetString(o.Flag)
		case *int:
			var d int
			if o.Default != nil {
				d = o.Default.(int)
			}
			cmd.Flags().IntVar(destP, o.Flag, d, o.Desc)
			mustBindPFlag(o.Flag, cmd)
			*destP = viper.GetInt(o.Flag)
		case *bool:
			var d bool
			if o.Default != nil {
				d = o.Default.(bool)
			}
			cmd.Flags().BoolVar(destP, o.Flag, d, o.Desc)
			mustBindPFlag(o.Flag, cmd)
			*destP = viper.GetBool(o.Flag)
		case *time.Duration:
			var d time.Duration
			if o.Default != nil {
				d = o.Default.(time.Duration)
			}
			cmd.Flags().DurationVar(destP, o.Flag, d, o.Desc)
			mustBindPFlag(o.Flag, cmd)
			*destP = viper.GetDuration(o.Flag)
		case *[]string:
			var d []string
			if o.Default != nil {
				d = o.Default.([]string)
			}
			cmd.Flags().StringSliceVar(destP, o.Flag, d, o.Desc)
			mustBindPFlag(o.Flag, cmd)
			*destP = viper.GetStringSlice(o.Flag)
		default:
			// if you get a panic here, sorry about that!
			// anyway, go ahead and make a PR and add another type.
			panic(fmt.Errorf("unknown destination type %t", o.DestP))
		}
	}
}

func mustBindPFlag(key string, cmd *cobra.Command) {
	if err := viper.BindPFlag(key, cmd.Flags().Lookup(key)); err != nil {
		panic(err)
	}
}

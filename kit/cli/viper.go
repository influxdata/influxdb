package cli

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"time"
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

	for _, o := range p.Opts {
		switch o.DestP.(type) {
		case *string:
			if o.Default == nil {
				o.Default = ""
			}
			cmd.Flags().StringVar(o.DestP.(*string), o.Flag, o.Default.(string), o.Desc)
			viper.BindPFlag(o.Flag, cmd.Flags().Lookup(o.Flag))
			*o.DestP.(*string) = viper.GetString(o.Flag)
		case *int:
			if o.Default == nil {
				o.Default = 0
			}
			cmd.Flags().IntVar(o.DestP.(*int), o.Flag, o.Default.(int), o.Desc)
			viper.BindPFlag(o.Flag, cmd.Flags().Lookup(o.Flag))
			*o.DestP.(*int) = viper.GetInt(o.Flag)
		case *bool:
			if o.Default == nil {
				o.Default = false
			}
			cmd.Flags().BoolVar(o.DestP.(*bool), o.Flag, o.Default.(bool), o.Desc)
			viper.BindPFlag(o.Flag, cmd.Flags().Lookup(o.Flag))
			*o.DestP.(*bool) = viper.GetBool(o.Flag)
		case *time.Duration:
			if o.Default == nil {
				o.Default = time.Duration(0)
			}
			cmd.Flags().DurationVar(o.DestP.(*time.Duration), o.Flag, o.Default.(time.Duration), o.Desc)
			viper.BindPFlag(o.Flag, cmd.Flags().Lookup(o.Flag))
			*o.DestP.(*time.Duration) = viper.GetDuration(o.Flag)
		case *[]string:
			if o.Default == nil {
				o.Default = []string{}
			}
			cmd.Flags().StringSliceVar(o.DestP.(*[]string), o.Flag, o.Default.([]string), o.Desc)
			viper.BindPFlag(o.Flag, cmd.Flags().Lookup(o.Flag))
			*o.DestP.(*[]string) = viper.GetStringSlice(o.Flag)
		default:
			// if you get a panic here, sorry about that!
			// anyway, go ahead and make a PR and add another type.
			panic(fmt.Errorf("unknown destination type %t", o.DestP))
		}
	}

	return cmd
}

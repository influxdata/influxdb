package main

import (
	"fmt"

	"github.com/influxdata/flux/plan"
	"github.com/influxdata/flux/repl"
	"github.com/influxdata/flux/runtime"
	_ "github.com/influxdata/flux/stdlib"
	"github.com/influxdata/flux/stdlib/influxdata/influxdb"
	_ "github.com/influxdata/influxdb/v2/query/stdlib"
	"github.com/spf13/cobra"
)

var queryFlags struct {
	org organization
}

func cmdQuery(f *globalFlags, opts genericCLIOpts) *cobra.Command {
	cmd := opts.newCmd("query [query literal or @/path/to/query.flux]", fluxQueryF, true)
	cmd.Short = "Execute a Flux query"
	cmd.Long = `Execute a literal Flux query provided as a string,
or execute a literal Flux query contained in a file by specifying the file prefixed with an @ sign.`
	cmd.Args = cobra.ExactArgs(1)

	queryFlags.org.register(cmd, true)

	return cmd
}

func fluxQueryF(cmd *cobra.Command, args []string) error {
	if flags.local {
		return fmt.Errorf("local flag not supported for query command")
	}

	if err := queryFlags.org.validOrgFlags(&flags); err != nil {
		return err
	}

	q, err := repl.LoadQuery(args[0])
	if err != nil {
		return fmt.Errorf("failed to load query: %v", err)
	}

	plan.RegisterLogicalRules(
		influxdb.DefaultFromAttributes{
			Org: &influxdb.NameOrID{
				ID:   queryFlags.org.id,
				Name: queryFlags.org.name,
			},
			Host:  &flags.Host,
			Token: &flags.Token,
		},
	)
	runtime.FinalizeBuiltIns()

	r, err := getFluxREPL(flags.skipVerify)
	if err != nil {
		return fmt.Errorf("failed to get the flux REPL: %v", err)
	}

	if err := r.Input(q); err != nil {
		return fmt.Errorf("failed to execute query: %v", err)
	}

	return nil
}

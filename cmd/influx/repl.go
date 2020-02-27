package main

import (
	"context"
	"fmt"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/dependencies/filesystem"
	"github.com/influxdata/flux/repl"
	"github.com/influxdata/flux/runtime"
	_ "github.com/influxdata/flux/stdlib"
	_ "github.com/influxdata/influxdb/query/stdlib"
	"github.com/spf13/cobra"
)

var replFlags struct {
	org organization
}

func cmdREPL() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "repl",
		Short: "Interactive Flux REPL (read-eval-print-loop)",
		Args:  cobra.NoArgs,
		RunE:  wrapCheckSetup(replF),
	}
	replFlags.org.register(cmd, false)

	return cmd
}

func replF(cmd *cobra.Command, args []string) error {
	if flags.local {
		return fmt.Errorf("local flag not supported for repl command")
	}

	if err := replFlags.org.validOrgFlags(); err != nil {
		return err
	}

	// TODO(jsternberg): Restore the repl by merging the influxdb client.
	// orgSVC, err := newOrganizationService()
	// if err != nil {
	// 	return err
	// }
	//
	// orgID, err := replFlags.org.getID(orgSVC)
	// if err != nil {
	// 	return err
	// }

	runtime.FinalizeBuiltIns()

	r, err := getFluxREPL()
	if err != nil {
		return err
	}

	r.Run()
	return nil
}

func getFluxREPL() (*repl.REPL, error) {
	deps := flux.NewDefaultDependencies()
	deps.Deps.FilesystemService = filesystem.SystemFS
	ctx := deps.Inject(context.Background())
	return repl.New(ctx, deps), nil
}

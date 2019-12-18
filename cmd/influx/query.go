package main

import (
	"context"
	"fmt"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/repl"
	_ "github.com/influxdata/flux/stdlib"
	platform "github.com/influxdata/influxdb"
	_ "github.com/influxdata/influxdb/query/stdlib"
	"github.com/spf13/cobra"
)

var queryCmd = &cobra.Command{
	Use:   "query [query literal or @/path/to/query.flux]",
	Short: "Execute a Flux query",
	Long: `Execute a literal Flux query provided as a string,
or execute a literal Flux query contained in a file by specifying the file prefixed with an @ sign.`,
	Args: cobra.ExactArgs(1),
	RunE: wrapCheckSetup(fluxQueryF),
}

var queryFlags struct {
	organization
}

func init() {
	queryFlags.organization.register(queryCmd)
}

func fluxQueryF(cmd *cobra.Command, args []string) error {
	if flags.local {
		return fmt.Errorf("local flag not supported for query command")
	}

	if err := queryFlags.organization.validOrgFlags(); err != nil {
		return err
	}

	q, err := repl.LoadQuery(args[0])
	if err != nil {
		return fmt.Errorf("failed to load query: %v", err)
	}

	var orgID platform.ID

	if queryFlags.organization.id != "" {
		if err := orgID.DecodeFromString(queryFlags.organization.id); err != nil {
			return fmt.Errorf("failed to decode org-id: %v", err)
		}
	}

	if queryFlags.organization.name != "" {
		orgSvc, err := newOrganizationService()
		if err != nil {
			return fmt.Errorf("failed to initialized organization service client: %v", err)
		}

		filter := platform.OrganizationFilter{Name: &queryFlags.organization.name}
		o, err := orgSvc.FindOrganization(context.Background(), filter)
		if err != nil {
			return fmt.Errorf("failed to retrieve organization %q: %v", queryFlags.organization.name, err)
		}

		orgID = o.ID
	}

	flux.FinalizeBuiltIns()

	r, err := getFluxREPL(flags.host, flags.token, flags.skipVerify, orgID)
	if err != nil {
		return fmt.Errorf("failed to get the flux REPL: %v", err)
	}

	if err := r.Input(q); err != nil {
		return fmt.Errorf("failed to execute query: %v", err)
	}

	return nil
}

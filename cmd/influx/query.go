package main

import (
	"context"
	"fmt"

	"github.com/influxdata/flux/repl"
	platform "github.com/influxdata/influxdb"
	_ "github.com/influxdata/influxdb/query/builtin"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
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
	OrgID string
	Org   string
}

func init() {
	queryCmd.PersistentFlags().StringVar(&queryFlags.OrgID, "org-id", "", "The organization ID")
	viper.BindEnv("ORG_ID")
	if h := viper.GetString("ORG_ID"); h != "" {
		queryFlags.OrgID = h
	}

	queryCmd.PersistentFlags().StringVarP(&queryFlags.Org, "org", "o", "", "The organization name")
	viper.BindEnv("ORG")
	if h := viper.GetString("ORG"); h != "" {
		queryFlags.Org = h
	}
}

func fluxQueryF(cmd *cobra.Command, args []string) error {
	if flags.local {
		return fmt.Errorf("local flag not supported for query command")
	}

	if (queryFlags.OrgID != "" && queryFlags.Org != "") || (queryFlags.OrgID == "" && queryFlags.Org == "") {
		return fmt.Errorf("must specify exactly one of org or org-id")
	}

	q, err := repl.LoadQuery(args[0])
	if err != nil {
		return fmt.Errorf("failed to load query: %v", err)
	}

	var orgID platform.ID

	if queryFlags.OrgID != "" {
		if err := orgID.DecodeFromString(queryFlags.OrgID); err != nil {
			return fmt.Errorf("failed to decode org-id: %v", err)
		}
	}

	if queryFlags.Org != "" {
		orgSvc, err := newOrganizationService(flags)
		if err != nil {
			return fmt.Errorf("failed to initialized organization service client: %v", err)
		}

		filter := platform.OrganizationFilter{Name: &queryFlags.Org}
		o, err := orgSvc.FindOrganization(context.Background(), filter)
		if err != nil {
			return fmt.Errorf("failed to retrieve organization %q: %v", queryFlags.Org, err)
		}

		orgID = o.ID
	}

	r, err := getFluxREPL(flags.host, flags.token, orgID)
	if err != nil {
		return fmt.Errorf("failed to get the flux REPL: %v", err)
	}

	if err := r.Input(q); err != nil {
		return fmt.Errorf("failed to execute query: %v", err)
	}

	return nil
}

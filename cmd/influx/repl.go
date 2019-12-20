package main

import (
	"context"
	"fmt"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/repl"
	_ "github.com/influxdata/flux/stdlib"
	platform "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/http"
	"github.com/influxdata/influxdb/v2/query"
	_ "github.com/influxdata/influxdb/v2/query/stdlib"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var replCmd = &cobra.Command{
	Use:   "repl",
	Short: "Interactive Flux REPL (read-eval-print-loop)",
	Args:  cobra.NoArgs,
	RunE:  wrapCheckSetup(replF),
}

var replFlags struct {
	OrgID string
	Org   string
}

func init() {
	replCmd.PersistentFlags().StringVar(&replFlags.OrgID, "org-id", "", "The ID of organization to query")
	viper.BindEnv("ORG_ID")
	if h := viper.GetString("ORG_ID"); h != "" {
		replFlags.OrgID = h
	}

	replCmd.PersistentFlags().StringVarP(&replFlags.Org, "org", "o", "", "The name of the organization")
	viper.BindEnv("ORG")
	if h := viper.GetString("ORG"); h != "" {
		replFlags.Org = h
	}
}

func replF(cmd *cobra.Command, args []string) error {
	if flags.local {
		return fmt.Errorf("local flag not supported for repl command")
	}

	if replFlags.OrgID == "" && replFlags.Org == "" {
		return fmt.Errorf("must specify exactly one of org or org-id")
	}

	if replFlags.OrgID != "" && replFlags.Org != "" {
		return fmt.Errorf("must specify exactly one of org or org-id")
	}

	var orgID platform.ID
	if replFlags.OrgID != "" {
		err := orgID.DecodeFromString(replFlags.OrgID)
		if err != nil {
			return fmt.Errorf("invalid org id: %v", err)
		}
	}

	if replFlags.Org != "" {
		ctx := context.Background()
		var err error
		orgID, err = findOrgID(ctx, replFlags.Org)
		if err != nil {
			return fmt.Errorf("unable to find organization: %v", err)
		}
	}

	flux.FinalizeBuiltIns()

	r, err := getFluxREPL(flags.host, flags.token, flags.skipVerify, orgID)
	if err != nil {
		return err
	}

	r.Run()
	return nil
}

func findOrgID(ctx context.Context, org string) (platform.ID, error) {
	client, err := newHTTPClient()
	if err != nil {
		return 0, err
	}
	svc := &http.OrganizationService{
		Client: client,
	}

	o, err := svc.FindOrganization(ctx, platform.OrganizationFilter{
		Name: &org,
	})
	if err != nil {
		return platform.InvalidID(), err
	}

	return o.ID, nil
}

func getFluxREPL(addr, token string, skipVerify bool, orgID platform.ID) (*repl.REPL, error) {
	qs := &http.FluxQueryService{
		Addr:               addr,
		Token:              token,
		InsecureSkipVerify: skipVerify,
	}
	q := &query.REPLQuerier{
		OrganizationID: orgID,
		QueryService:   qs,
	}
	// background context is OK here, and DefaultDependencies are noop deps.  Also safe since we send all queries to the
	// server side.
	return repl.New(context.Background(), flux.NewDefaultDependencies(), q), nil
}

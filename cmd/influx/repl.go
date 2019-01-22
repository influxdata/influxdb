package main

import (
	"context"
	"fmt"

	"github.com/influxdata/flux/repl"
	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/http"
	"github.com/influxdata/influxdb/query"
	_ "github.com/influxdata/influxdb/query/builtin"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var replCmd = &cobra.Command{
	Use:   "repl",
	Short: "Interactive REPL (read-eval-print-loop)",
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

	r, err := getFluxREPL(flags.host, flags.token, orgID)
	if err != nil {
		return err
	}

	r.Run()
	return nil
}

func findOrgID(ctx context.Context, org string) (platform.ID, error) {
	svc := &http.OrganizationService{
		Addr:  flags.host,
		Token: flags.token,
	}

	o, err := svc.FindOrganization(ctx, platform.OrganizationFilter{
		Name: &org,
	})
	if err != nil {
		return platform.InvalidID(), err
	}

	return o.ID, nil
}

func getFluxREPL(addr, token string, orgID platform.ID) (*repl.REPL, error) {
	qs := &http.FluxQueryService{
		Addr:  addr,
		Token: token,
	}
	q := &query.REPLQuerier{
		OrganizationID: orgID,
		QueryService:   qs,
	}
	return repl.New(q), nil
}

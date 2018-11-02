package main

import (
	"context"
	"fmt"
	"os"

	"github.com/influxdata/flux/repl"
	"github.com/influxdata/platform"
	"github.com/influxdata/platform/http"
	"github.com/influxdata/platform/query"
	_ "github.com/influxdata/platform/query/builtin"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var replCmd = &cobra.Command{
	Use:   "repl",
	Short: "Interactive REPL (read-eval-print-loop)",
	Args:  cobra.NoArgs,
	Run:   replF,
}

var replFlags struct {
	OrgID string
	Org   string
}

func init() {
	replCmd.PersistentFlags().StringVar(&replFlags.OrgID, "org-id", "", "ID of organization to query")
	viper.BindEnv("ORG_ID")
	if h := viper.GetString("ORG_ID"); h != "" {
		replFlags.OrgID = h
	}

	replCmd.PersistentFlags().StringVarP(&replFlags.Org, "org", "o", "", "name of the organization")
	viper.BindEnv("ORG")
	if h := viper.GetString("ORG"); h != "" {
		replFlags.Org = h
	}
}

func replF(cmd *cobra.Command, args []string) {
	if flags.local {
		fmt.Println("Local flag not supported for repl command")
		os.Exit(1)
	}

	if replFlags.OrgID == "" && replFlags.Org == "" {
		fmt.Fprintln(os.Stderr, "must specify exactly one of org or org-id")
		_ = cmd.Usage()
		os.Exit(1)
	}

	if replFlags.OrgID != "" && replFlags.Org != "" {
		fmt.Fprintln(os.Stderr, "must specify exactly one of org or org-id")
		_ = cmd.Usage()
		os.Exit(1)
	}

	var orgID platform.ID
	if replFlags.OrgID != "" {
		err := orgID.DecodeFromString(replFlags.OrgID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid org id: %v\n", err)
			os.Exit(1)
		}
	}

	if replFlags.Org != "" {
		ctx := context.Background()
		var err error
		orgID, err = findOrgID(ctx, replFlags.Org)
		if err != nil {
			fmt.Fprintf(os.Stderr, "unable to find organization: %v\n", err)
			os.Exit(1)
		}
	}

	r, err := getFluxREPL(flags.host, flags.token, orgID)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	r.Run()
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

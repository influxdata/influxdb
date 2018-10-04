package main

import (
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
}

func init() {
	replCmd.PersistentFlags().StringVar(&replFlags.OrgID, "org-id", "", "Organization ID")
	viper.BindEnv("ORG_ID")
	if h := viper.GetString("ORG_ID"); h != "" {
		replFlags.OrgID = h
	}
}

func replF(cmd *cobra.Command, args []string) {
	var orgID platform.ID
	err := orgID.DecodeFromString(replFlags.OrgID)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	r, err := getFluxREPL(flags.host, flags.token, orgID)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	r.Run()
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

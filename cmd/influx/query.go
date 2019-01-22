package main

import (
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
}

func init() {
	queryCmd.PersistentFlags().StringVar(&queryFlags.OrgID, "org-id", "", "The organization ID")
	viper.BindEnv("ORG_ID")
	if h := viper.GetString("ORG_ID"); h != "" {
		queryFlags.OrgID = h
	}
	queryCmd.MarkPersistentFlagRequired("org-id")
}

func fluxQueryF(cmd *cobra.Command, args []string) error {
	if flags.local {
		return fmt.Errorf("local flag not supported for query command")
	}

	q, err := repl.LoadQuery(args[0])
	if err != nil {
		return err
	}

	var orgID platform.ID
	err = orgID.DecodeFromString(queryFlags.OrgID)
	if err != nil {
		return err
	}

	r, err := getFluxREPL(flags.host, flags.token, orgID)
	if err != nil {
		return err
	}

	if err := r.Input(q); err != nil {
		return err
	}

	return nil
}

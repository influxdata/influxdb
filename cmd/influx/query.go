package main

import (
	"fmt"
	"os"

	"github.com/influxdata/flux/repl"
	"github.com/influxdata/platform"
	_ "github.com/influxdata/platform/query/builtin"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var queryCmd = &cobra.Command{
	Use:   "query [query literal or @/path/to/query.flux]",
	Short: "Execute an Flux query",
	Long: `Execute a literal Flux query provided as a string,
		or execute a literal Flux query contained in a file by specifying the file prefixed with an @ sign.`,
	Args: cobra.ExactArgs(1),
	Run:  fluxQueryF,
}

var queryFlags struct {
	OrgID string
}

func init() {
	queryCmd.PersistentFlags().StringVar(&queryFlags.OrgID, "org-id", "", "Organization ID")
	viper.BindEnv("ORG_ID")
	if h := viper.GetString("ORG_ID"); h != "" {
		queryFlags.OrgID = h
	}
	queryCmd.MarkPersistentFlagRequired("org-id")
}

func fluxQueryF(cmd *cobra.Command, args []string) {
	if flags.local {
		fmt.Println("Local flag not supported for query command")
		os.Exit(1)
	}

	q, err := repl.LoadQuery(args[0])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	var orgID platform.ID
	err = orgID.DecodeFromString(queryFlags.OrgID)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	r, err := getFluxREPL(flags.host, flags.token, orgID)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if err := r.Input(q); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

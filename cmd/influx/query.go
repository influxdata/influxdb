package main

import (
	"fmt"
	"os"

	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/functions"
	"github.com/influxdata/flux/functions/storage"
	"github.com/influxdata/flux/repl"
	"github.com/influxdata/platform"
	"github.com/influxdata/platform/http"
	"github.com/influxdata/platform/query"
	_ "github.com/influxdata/platform/query/builtin"
	"github.com/influxdata/platform/query/functions/storage/pb"
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
}

func fluxQueryF(cmd *cobra.Command, args []string) {
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

func injectDeps(deps execute.Dependencies, hosts storage.Reader, buckets platform.BucketService, orgs platform.OrganizationService) error {
	return functions.InjectFromDependencies(deps, storage.Dependencies{
		Reader:             hosts,
		BucketLookup:       query.FromBucketService(buckets),
		OrganizationLookup: query.FromOrganizationService(orgs),
	})
}

func storageHostReader(hosts []string) (storage.Reader, error) {
	return pb.NewReader(storage.NewStaticLookup(hosts))
}

func bucketService(addr, token string) (platform.BucketService, error) {
	if addr == "" {
		return nil, fmt.Errorf("bucket host address required")
	}

	return &http.BucketService{
		Addr:  addr,
		Token: token,
	}, nil
}

func orgService(addr, token string) (platform.OrganizationService, error) {
	if addr == "" {
		return nil, fmt.Errorf("organization host address required")
	}

	return &http.OrganizationService{
		Addr:  addr,
		Token: token,
	}, nil
}

func orgID(org string) (platform.ID, error) {
	var oid platform.ID
	err := oid.DecodeFromString(org)
	return oid, err
}

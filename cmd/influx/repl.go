package main

import (
	"fmt"
	"math"
	"os"
	"runtime"
	"strings"

	"github.com/influxdata/flux/control"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/functions/storage"
	"github.com/influxdata/flux/repl"
	"github.com/influxdata/platform"
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
	StorageHosts string
	OrgID        string
	Verbose      bool
}

func init() {
	replCmd.PersistentFlags().StringVar(&replFlags.StorageHosts, "storage-hosts", "localhost:8082", "Comma-separated list of storage hosts")
	viper.BindEnv("STORAGE_HOSTS")
	if h := viper.GetString("STORAGE_HOSTS"); h != "" {
		replFlags.StorageHosts = h
	}

	replCmd.PersistentFlags().BoolVarP(&replFlags.Verbose, "verbose", "v", false, "Verbose output")
	viper.BindEnv("VERBOSE")
	if viper.GetBool("VERBOSE") {
		replFlags.Verbose = true
	}

	replCmd.PersistentFlags().StringVar(&replFlags.OrgID, "org-id", "", "Organization ID")
	viper.BindEnv("ORG_ID")
	if h := viper.GetString("ORG_ID"); h != "" {
		replFlags.OrgID = h
	}
}

func replF(cmd *cobra.Command, args []string) {
	hosts, err := storageHostReader(strings.Split(replFlags.StorageHosts, ","))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	buckets, err := bucketService(flags.host, flags.token)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	orgs, err := orgService(flags.host, flags.token)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	r, err := getFluxREPL(hosts, buckets, orgs, replFlags.Verbose)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	r.Run()
}

func getFluxREPL(storageHosts storage.Reader, buckets platform.BucketService, orgs platform.OrganizationService, verbose bool) (*repl.REPL, error) {
	conf := control.Config{
		ExecutorDependencies: make(execute.Dependencies),
		ConcurrencyQuota:     runtime.NumCPU() * 2,
		MemoryBytesQuota:     math.MaxInt64,
		Verbose:              verbose,
	}

	if err := injectDeps(conf.ExecutorDependencies, storageHosts, buckets, orgs); err != nil {
		return nil, err
	}

	c := control.New(conf)
	return repl.New(c), nil
}

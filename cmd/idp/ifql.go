package main

import (
	"fmt"
	"math"
	"os"
	"runtime"
	"strings"

	"github.com/influxdata/ifql"
	"github.com/influxdata/ifql/functions"
	"github.com/influxdata/ifql/functions/storage"
	"github.com/influxdata/ifql/functions/storage/pb"
	ifqlid "github.com/influxdata/ifql/id"
	"github.com/influxdata/ifql/query/execute"
	"github.com/influxdata/ifql/repl"
	"github.com/influxdata/platform"
	"github.com/influxdata/platform/http"
	"github.com/influxdata/platform/query"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var ifqlCmd = &cobra.Command{
	Use:   "ifql",
	Short: "Commands to interact with an IFQL server",
	Run:   ifqlF,
}

var ifqlFlags struct {
	StorageHosts string
	OrgID        string
	Verbose      bool
}

func init() {
	ifqlCmd.PersistentFlags().StringVar(&ifqlFlags.StorageHosts, "storage-hosts", "localhost:8082", "Comma-separated list of storage hosts")
	viper.BindEnv("STORAGE_HOSTS")
	if h := viper.GetString("STORAGE_HOSTS"); h != "" {
		ifqlFlags.StorageHosts = h
	}

	ifqlCmd.PersistentFlags().BoolVarP(&ifqlFlags.Verbose, "verbose", "v", false, "Verbose output")
	viper.BindEnv("VERBOSE")
	if viper.GetBool("VERBOSE") {
		ifqlFlags.Verbose = true
	}

	ifqlCmd.PersistentFlags().StringVar(&ifqlFlags.OrgID, "org-id", "", "Organization ID")
	viper.BindEnv("ORG_ID")
	if h := viper.GetString("ORG_ID"); h != "" {
		ifqlFlags.OrgID = h
	}
}

func ifqlF(cmd *cobra.Command, args []string) {
	cmd.Usage()
}

func init() {
	ifqlCmd.AddCommand(&cobra.Command{
		Use:   "repl",
		Short: "Interactive IFQL REPL (read-eval-print-loop)",
		Args:  cobra.NoArgs,
		Run:   ifqlReplF,
	})
}

func ifqlReplF(cmd *cobra.Command, args []string) {
	hosts, err := storageHostReader(strings.Split(ifqlFlags.StorageHosts, ","))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	org, err := orgID(ifqlFlags.OrgID)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	buckets, err := bucketService(flags.host, flags.token)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	r, err := getIFQLREPL(hosts, buckets, org, ifqlFlags.Verbose)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	r.Run()
}

func init() {
	ifqlCmd.AddCommand(&cobra.Command{
		Use:   "query [query literal or @/path/to/ifql]",
		Short: "Execute an IFQL query",
		Long: `Execute a literal IFQL query provided as a string,
		or execute a literal IFQL query contained in a file by specifying the file prefixed with an @ sign.`,
		Args: cobra.ExactArgs(1),
		Run:  ifqlQueryF,
	})
}

func ifqlQueryF(cmd *cobra.Command, args []string) {
	q, err := repl.LoadQuery(args[0])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	hosts, err := storageHostReader(strings.Split(ifqlFlags.StorageHosts, ","))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	org, err := orgID(ifqlFlags.OrgID)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	buckets, err := bucketService(flags.host, flags.token)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	r, err := getIFQLREPL(hosts, buckets, org, ifqlFlags.Verbose)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if err := r.Input(q); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func injectDeps(deps execute.Dependencies, hosts storage.Reader, buckets platform.BucketService) error {
	return functions.InjectFromDependencies(deps, storage.Dependencies{
		Reader:       hosts,
		BucketLookup: query.FromBucketService(buckets),
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

func orgID(org string) (ifqlid.ID, error) {
	var oid ifqlid.ID
	err := oid.DecodeFromString(ifqlFlags.OrgID)
	return oid, err
}

func getIFQLREPL(storageHosts storage.Reader, buckets platform.BucketService, org ifqlid.ID, verbose bool) (*repl.REPL, error) {
	conf := ifql.Config{
		Dependencies:     make(execute.Dependencies),
		ConcurrencyQuota: runtime.NumCPU() * 2,
		MemoryBytesQuota: math.MaxInt64,
		Verbose:          verbose,
	}

	if err := injectDeps(conf.Dependencies, storageHosts, buckets); err != nil {
		return nil, err
	}

	c, err := ifql.NewController(conf)
	if err != nil {
		return nil, err
	}

	return repl.New(c, org), nil
}

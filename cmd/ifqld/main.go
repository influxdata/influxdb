package main

import (
	"context"
	"fmt"
	nethttp "net/http"
	"os"
	"runtime"

	"github.com/influxdata/ifql"
	"github.com/influxdata/ifql/functions"
	"github.com/influxdata/ifql/functions/storage"
	"github.com/influxdata/ifql/id"
	"github.com/influxdata/ifql/query"
	"github.com/influxdata/ifql/query/execute"
	influxlogger "github.com/influxdata/influxdb/logger"
	"github.com/influxdata/platform"
	"github.com/influxdata/platform/http"
	platformquery "github.com/influxdata/platform/query"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var ifqlCmd = &cobra.Command{
	Use:   "ifqld",
	Short: "IFQL Enterprise Server",
	Run:   ifqlF,
}

var (
	bindAddr         string
	concurrencyQuota int
	memoryBytesQuota int
)

func init() {
	viper.SetEnvPrefix("IFQLD")

	ifqlCmd.PersistentFlags().StringVar(&bindAddr, "bind-addr", ":8093", "The bind address for this daemon.")
	viper.BindEnv("BIND_ADDR")
	viper.BindPFlag("bind_addr", ifqlCmd.PersistentFlags().Lookup("bind-addr"))

	ifqlCmd.PersistentFlags().IntVar(&concurrencyQuota, "concurrency", runtime.NumCPU()*2, "The concurrency quota capacity for this daemon.")
	viper.BindEnv("CONCURRENCY")
	viper.BindPFlag("concurrency", ifqlCmd.PersistentFlags().Lookup("cuncurrency"))

	ifqlCmd.PersistentFlags().IntVar(&memoryBytesQuota, "mem-bytes", 0, "The memory-bytes quota capacity for this daemon.")
	viper.BindEnv("MEM_BYTES")
	viper.BindPFlag("mem_bytes", ifqlCmd.PersistentFlags().Lookup("mem-bytes"))

	ifqlCmd.PersistentFlags().String("storage-hosts", "", "host:port address of the storage server.")
	viper.BindEnv("STORAGE_HOSTS")
	viper.BindPFlag("STORAGE_HOSTS", ifqlCmd.PersistentFlags().Lookup("storage-hosts"))

	ifqlCmd.PersistentFlags().String("bucket-host", "", "The bucket service host. ")
	viper.BindEnv("BUCKET_HOST")
	viper.BindPFlag("BUCKET_HOST", ifqlCmd.PersistentFlags().Lookup("bucket-hosts"))

	ifqlCmd.PersistentFlags().String("organization-hosts", "", "The organization service host.")
	viper.BindEnv("ORGANIZATION_HOSTS")
	viper.BindPFlag("ORGANIZATION_HOSTS", ifqlCmd.PersistentFlags().Lookup("organization-hosts"))

}

var logger *zap.Logger

func ifqlF(cmd *cobra.Command, args []string) {
	// Create top level logger
	logger = influxlogger.New(os.Stdout)

	config := ifql.Config{
		Dependencies:     make(execute.Dependencies),
		ConcurrencyQuota: concurrencyQuota,
		MemoryBytesQuota: memoryBytesQuota,
	}
	if err := injectDeps(config.Dependencies); err != nil {
		logger.Error("error injecting dependencies", zap.Error(err))
		os.Exit(1)
	}
	c, err := ifql.NewController(config)
	if err != nil {
		logger.Error("error creating controller", zap.Error(err))
		os.Exit(1)
	}

	var orgSvc platform.OrganizationService
	// TODO(adam): figure out what orgSvc we need to create here.

	queryHandler := http.NewQueryHandler()
	queryHandler.QueryService = platform.QueryServiceBridge{
		AsyncQueryService: wrapController{Controller: c},
	}
	queryHandler.OrganizationService = orgSvc

	handler := http.NewHandler("query")
	handler.Handler = queryHandler

	logger.Info("listening", zap.String("transport", "http"), zap.String("addr", bindAddr))
	if err := nethttp.ListenAndServe(bindAddr, handler); err != nil {
		logger.Error("encountered fatal error", zap.Error(err))
		os.Exit(1)
	}
}

func injectDeps(deps execute.Dependencies) error {

	// TODO(adam): figure out the correct read service
	var sr storage.Reader
	// TODO(adam): figure correct bucket service
	var bucketSvc platform.BucketService

	return functions.InjectFromDependencies(deps, storage.Dependencies{
		Reader:       sr,
		BucketLookup: platformquery.FromBucketService(bucketSvc),
	})
}

func main() {
	if err := ifqlCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

// wrapController is needed to make *ifql.Controller implement platform.AsyncQueryService.
// TODO(nathanielc): Remove this type and make ifql.Controller implement the platform.AsyncQueryService directly.
type wrapController struct {
	*ifql.Controller
}

func (c wrapController) Query(ctx context.Context, orgID platform.ID, query *query.Spec) (platform.Query, error) {
	q, err := c.Controller.Query(ctx, id.ID(orgID), query)
	return q, err
}

func (c wrapController) QueryWithCompile(ctx context.Context, orgID platform.ID, query string) (platform.Query, error) {
	q, err := c.Controller.QueryWithCompile(ctx, id.ID(orgID), query)
	return q, err
}

type bucketLookup struct {
	BucketService platform.BucketService
}

func (b bucketLookup) Lookup(orgID id.ID, name string) (id.ID, bool) {
	oid := platform.ID(orgID)
	filter := platform.BucketFilter{
		OrganizationID: &oid,
		Name:           &name,
	}
	bucket, err := b.BucketService.FindBucket(context.Background(), filter)
	if err != nil {
		return nil, false
	}
	return id.ID(bucket.ID), true
}

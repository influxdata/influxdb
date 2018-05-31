package main

import (
	"context"
	"fmt"
	nethttp "net/http"
	"os"
	"runtime"
	"strings"
	"time"

	influxlogger "github.com/influxdata/influxdb/logger"
	"github.com/influxdata/platform"
	"github.com/influxdata/platform/http"
	"github.com/influxdata/platform/query"
	_ "github.com/influxdata/platform/query/builtin"
	"github.com/influxdata/platform/query/control"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/functions"
	"github.com/influxdata/platform/query/functions/storage"
	"github.com/influxdata/platform/query/functions/storage/pb"
	"github.com/influxdata/platform/query/id"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var (
	// commit represents a hash of the commit that build this binary.
	// Commit is populated with build flags.
	commit string
)

var ifqlCmd = &cobra.Command{
	Use:     "ifqld",
	Short:   "IFQL Server",
	Version: commit,
	Run:     ifqlF,
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

	ifqlCmd.PersistentFlags().String("storage-hosts", "localhost:8082", "host:port address of the storage server.")
	viper.BindEnv("STORAGE_HOSTS")
	viper.BindPFlag("STORAGE_HOSTS", ifqlCmd.PersistentFlags().Lookup("storage-hosts"))

	ifqlCmd.PersistentFlags().String("bucket-name", "defaultbucket", "The bucket to access. ")
	viper.BindEnv("BUCKET_NAME")
	viper.BindPFlag("BUCKET_NAME", ifqlCmd.PersistentFlags().Lookup("bucket-name"))

	ifqlCmd.PersistentFlags().String("organization-name", "defaultorgname", "The organization name to use.")
	viper.BindEnv("ORGANIZATION_NAME")
	viper.BindPFlag("ORGANIZATION_NAME", ifqlCmd.PersistentFlags().Lookup("organization-name"))

}

var logger *zap.Logger

func ifqlF(cmd *cobra.Command, args []string) {
	// Create top level logger
	logger = influxlogger.New(os.Stdout)

	config := control.Config{
		ExecutorDependencies: make(execute.Dependencies),
		ConcurrencyQuota:     concurrencyQuota,
		MemoryBytesQuota:     int64(memoryBytesQuota),
	}
	if err := injectDeps(config.ExecutorDependencies); err != nil {
		logger.Error("error injecting dependencies", zap.Error(err))
		os.Exit(1)
	}
	c := control.New(config)

	orgName, err := getStrList("ORGANIZATION_NAME")
	if err != nil {
		return
	}
	orgSvc := StaticOrganizationService{Name: orgName[0]}

	queryHandler := http.NewQueryHandler()
	queryHandler.QueryService = query.QueryServiceBridge{
		AsyncQueryService: wrapController{Controller: c},
	}
	queryHandler.OrganizationService = &orgSvc

	handler := http.NewHandler("query")
	handler.Handler = queryHandler

	logger.Info("listening", zap.String("transport", "http"), zap.String("addr", bindAddr))
	if err := nethttp.ListenAndServe(bindAddr, handler); err != nil {
		logger.Error("encountered fatal error", zap.Error(err))
		os.Exit(1)
	}
}

func getStrList(key string) ([]string, error) {
	v := viper.GetViper()
	valStr := v.GetString(key)
	if valStr == "" {
		return nil, errors.New("empty value")
	}

	return strings.Split(valStr, ","), nil
}

func injectDeps(deps execute.Dependencies) error {
	storageHosts, err := getStrList("STORAGE_HOSTS")
	if err != nil {
		return errors.Wrap(err, "failed to get storage hosts")
	}
	sr, err := pb.NewReader(storage.NewStaticLookup(storageHosts))
	if err != nil {
		return err
	}

	bucketName, err := getStrList("BUCKET_NAME")
	if err != nil {
		return errors.Wrap(err, "failed to get bucket name")
	}
	bucketSvc := StaticBucketService{Name: bucketName[0]}

	return functions.InjectFromDependencies(deps, storage.Dependencies{
		Reader:       sr,
		BucketLookup: query.FromBucketService(&bucketSvc),
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
	*control.Controller
}

func (c wrapController) Query(ctx context.Context, orgID platform.ID, query *query.Spec) (query.Query, error) {
	q, err := c.Controller.Query(ctx, id.ID(orgID), query)
	return q, err
}

func (c wrapController) QueryWithCompile(ctx context.Context, orgID platform.ID, query string) (query.Query, error) {
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

var (
	staticBucketID, staticOrgID platform.ID
)

func init() {
	staticBucketID.DecodeFromString("abba")
	staticOrgID.DecodeFromString("baab")
}

// StaticOrganizationService connects to Influx via HTTP using tokens to manage organizations.
type StaticOrganizationService struct {
	Name string
}

func (s *StaticOrganizationService) FindOrganizationByID(ctx context.Context, id platform.ID) (*platform.Organization, error) {
	return s.FindOrganization(ctx, platform.OrganizationFilter{})
}

func (s *StaticOrganizationService) FindOrganization(ctx context.Context, filter platform.OrganizationFilter) (*platform.Organization, error) {
	os, n, err := s.FindOrganizations(ctx, filter)
	if err != nil {
		return nil, err
	}

	if n < 1 {
		return nil, fmt.Errorf("expected at least one organization")
	}

	return os[0], nil
}

func (s *StaticOrganizationService) FindOrganizations(ctx context.Context, filter platform.OrganizationFilter, opt ...platform.FindOptions) ([]*platform.Organization, int, error) {
	po := platform.Organization{
		ID:   staticOrgID,
		Name: s.Name,
	}

	return []*platform.Organization{&po}, 1, nil

}

// CreateOrganization creates an organization.
func (s *StaticOrganizationService) CreateOrganization(ctx context.Context, o *platform.Organization) error {
	panic("not implemented")
}

func (s *StaticOrganizationService) UpdateOrganization(ctx context.Context, id platform.ID, upd platform.OrganizationUpdate) (*platform.Organization, error) {
	panic("not implemented")
}

func (s *StaticOrganizationService) DeleteOrganization(ctx context.Context, id platform.ID) error {
	panic("not implemented")
}

// StaticBucketService connects to Influx via HTTP using tokens to manage buckets
type StaticBucketService struct {
	Name string
}

// FindBucketByID returns a single bucket by ID.
func (s *StaticBucketService) FindBucketByID(ctx context.Context, id platform.ID) (*platform.Bucket, error) {
	return s.FindBucket(ctx, platform.BucketFilter{})
}

// FindBucket returns the first bucket that matches filter.
func (s *StaticBucketService) FindBucket(ctx context.Context, filter platform.BucketFilter) (*platform.Bucket, error) {
	bs, n, err := s.FindBuckets(ctx, filter)
	if err != nil {
		return nil, err
	}

	if n == 0 {
		return nil, fmt.Errorf("found no matching buckets")
	}

	return bs[0], nil
}

// FindBuckets returns a list of buckets that match filter and the total count of matching buckets.
// Additional options provide pagination & sorting.
func (s *StaticBucketService) FindBuckets(ctx context.Context, filter platform.BucketFilter, opt ...platform.FindOptions) ([]*platform.Bucket, int, error) {
	bo := platform.Bucket{
		ID:              staticBucketID,
		OrganizationID:  staticOrgID,
		Name:            s.Name,
		RetentionPeriod: 1000 * time.Hour,
	}

	return []*platform.Bucket{&bo}, 1, nil
}

// CreateBucket creates a new bucket and sets b.ID with the new identifier.
func (s *StaticBucketService) CreateBucket(ctx context.Context, b *platform.Bucket) error {
	panic("not implemented")
}

// UpdateBucket updates a single bucket with changeset.
// Returns the new bucket state after update.
func (s *StaticBucketService) UpdateBucket(ctx context.Context, id platform.ID, upd platform.BucketUpdate) (*platform.Bucket, error) {
	panic("not implemented")
}

// DeleteBucket removes a bucket by ID.
func (s *StaticBucketService) DeleteBucket(ctx context.Context, id platform.ID) error {
	panic("not implemented")
}

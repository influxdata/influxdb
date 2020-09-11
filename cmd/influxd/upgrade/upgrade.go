package upgrade

import (
	"context"
	"fmt"
	"os"
	"os/user"
	"path/filepath"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorizer"
	"github.com/influxdata/influxdb/v2/bolt"
	"github.com/influxdata/influxdb/v2/dbrp"
	"github.com/influxdata/influxdb/v2/internal/fs"
	"github.com/influxdata/influxdb/v2/kit/metric"
	"github.com/influxdata/influxdb/v2/kit/prom"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/kv/migration"
	"github.com/influxdata/influxdb/v2/kv/migration/all"
	"github.com/influxdata/influxdb/v2/tenant"
	"github.com/influxdata/influxdb/v2/v1/services/meta"
	"github.com/influxdata/influxdb/v2/v1/services/meta/filestore"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var Command = &cobra.Command{
	Use:   "upgrade",
	Short: "Upgrade a 1.x version of InfluxDB",
	RunE:  runUpgradeE,
}

type optionsV1 struct {
	metaDir string
}

type optionsV2 struct {
	boltPath string
}

var options = struct {
	// flags for source InfluxDB
	source optionsV1

	// flags for target InfluxDB
	target optionsV2
}{}

func init() {
	flags := Command.Flags()

	// source flags
	v1dir, err := influxDirV1()
	if err != nil {
		panic("error fetching default InfluxDB 1.x dir: " + err.Error())
	}

	flags.StringVar(&options.source.metaDir, "v1-meta-dir", filepath.Join(v1dir, "meta"), "Path to 1.x meta.db directory")

	// target flags
	v2dir, err := fs.InfluxDir()
	if err != nil {
		panic("error fetching default InfluxDB 2.0 dir: " + err.Error())
	}

	flags.StringVar(&options.target.boltPath, "v2-bolt-path", filepath.Join(v2dir, "influxd.bolt"), "Path to 2.0 metadata")

	// add sub commands
	Command.AddCommand(v1DumpMetaCommand)
	Command.AddCommand(v2DumpMetaCommand)
}

type influxDBv1 struct {
	meta *meta.Client
}

type influxDBv2 struct {
	boltClient  *bolt.Client
	store       *bolt.KVStore
	kvStore     kv.SchemaStore
	tenantStore *tenant.Store
	ts          *tenant.Service
	dbrpSvc     influxdb.DBRPMappingServiceV2
	onboardSvc  influxdb.OnboardingService
	kvService   *kv.Service
	meta        *meta.Client
}

func runUpgradeE(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	v1, err := newInfluxDBv1(&options.source)
	if err != nil {
		return err
	}
	_ = v1

	v2, err := newInfluxDBv2(ctx, &options.target)
	if err != nil {
		return err
	}
	_ = v2

	// 1. Onboard the initial admin user
	// v2.onboardSvc.OnboardInitialUser()

	// 2. read each database / retention policy from v1.meta and create bucket db-name/rp-name
	// newBucket := v2.ts.CreateBucket(ctx, Bucket{})
	//
	// 3. create database in v2.meta
	// v2.meta.CreateDatabase(newBucket.ID.String())
	// copy shard info from v1.meta

	return nil
}

func newInfluxDBv1(opts *optionsV1) (svc *influxDBv1, err error) {
	svc = &influxDBv1{}
	svc.meta, err = openV1Meta(opts.metaDir)
	if err != nil {
		return nil, fmt.Errorf("error opening 1.x meta.db: %w", err)
	}

	return svc, nil
}

func newInfluxDBv2(ctx context.Context, opts *optionsV2) (svc *influxDBv2, err error) {
	log := zap.NewNop()
	reg := prom.NewRegistry(log.With(zap.String("service", "prom_registry")))

	svc = &influxDBv2{}

	// *********************
	// V2 specific services
	serviceConfig := kv.ServiceConfig{}

	// Create BoltDB store and K/V service
	svc.boltClient = bolt.NewClient(log.With(zap.String("service", "bolt")))
	svc.boltClient.Path = opts.boltPath
	if err := svc.boltClient.Open(ctx); err != nil {
		log.Error("Failed opening bolt", zap.Error(err))
		return nil, err
	}

	svc.store = bolt.NewKVStore(log.With(zap.String("service", "kvstore-bolt")), opts.boltPath)
	svc.store.WithDB(svc.boltClient.DB())
	svc.kvStore = svc.store
	svc.kvService = kv.NewService(log.With(zap.String("store", "kv")), svc.store, serviceConfig)

	// ensure migrator is run
	migrator, err := migration.NewMigrator(
		log.With(zap.String("service", "migrations")),
		svc.kvStore,
		all.Migrations[:]...,
	)
	if err != nil {
		log.Error("Failed to initialize kv migrator", zap.Error(err))
		return nil, err
	}

	// apply migrations to metadata store
	if err := migrator.Up(ctx); err != nil {
		log.Error("Failed to apply migrations", zap.Error(err))
		return nil, err
	}

	// other required services
	var (
		authSvc influxdb.AuthorizationService = svc.kvService
	)

	// Create Tenant service (orgs, buckets, )
	svc.tenantStore = tenant.NewStore(svc.kvStore)
	svc.ts = tenant.NewSystem(svc.tenantStore, log.With(zap.String("store", "new")), reg, metric.WithSuffix("new"))

	svc.meta = meta.NewClient(meta.NewConfig(), svc.kvStore)
	if err := svc.meta.Open(); err != nil {
		return nil, err
	}

	// DB/RP service
	svc.dbrpSvc = dbrp.NewService(ctx, authorizer.NewBucketService(svc.ts.BucketService), svc.kvStore)

	// on-boarding service (influx setup)
	svc.onboardSvc = tenant.NewOnboardService(svc.ts, authSvc)

	return svc, nil
}

func openV1Meta(dir string) (*meta.Client, error) {
	cfg := meta.NewConfig()
	cfg.Dir = dir
	store := filestore.New(cfg.Dir, string(meta.BucketName), "meta.db")
	c := meta.NewClient(cfg, store)
	if err := c.Open(); err != nil {
		return nil, err
	}

	return c, nil
}

// influxDirV1 retrieves the influxdb directory.
func influxDirV1() (string, error) {
	var dir string
	// By default, store meta and data files in current users home directory
	u, err := user.Current()
	if err == nil {
		dir = u.HomeDir
	} else if home := os.Getenv("HOME"); home != "" {
		dir = home
	} else {
		wd, err := os.Getwd()
		if err != nil {
			return "", err
		}
		dir = wd
	}
	dir = filepath.Join(dir, ".influxdb")

	return dir, nil
}

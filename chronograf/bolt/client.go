package bolt

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"time"

	bolt "github.com/coreos/bbolt"
	"github.com/influxdata/platform/chronograf"
	"github.com/influxdata/platform/chronograf/id"
)

const (
	// ErrUnableToOpen means we had an issue establishing a connection (or creating the database)
	ErrUnableToOpen = "Unable to open boltdb; is there a chronograf already running?  %v"
	// ErrUnableToBackup means we couldn't copy the db file into ./backup
	ErrUnableToBackup = "Unable to backup your database prior to migrations:  %v"
	// ErrUnableToInitialize means we couldn't create missing Buckets (maybe a timeout)
	ErrUnableToInitialize = "Unable to boot boltdb:  %v"
	// ErrUnableToMigrate means we had an issue changing the db schema
	ErrUnableToMigrate = "Unable to migrate boltdb:  %v"
)

// Client is a client for the boltDB data store.
type Client struct {
	Path      string
	db        *bolt.DB
	logger    chronograf.Logger
	isNew     bool
	Now       func() time.Time
	LayoutIDs chronograf.ID

	BuildStore              *BuildStore
	SourcesStore            *SourcesStore
	ServersStore            *ServersStore
	LayoutsStore            *LayoutsStore
	DashboardsStore         *DashboardsStore
	UsersStore              *UsersStore
	OrganizationsStore      *OrganizationsStore
	ConfigStore             *ConfigStore
	MappingsStore           *MappingsStore
	OrganizationConfigStore *OrganizationConfigStore
}

// NewClient initializes all stores
func NewClient() *Client {
	c := &Client{Now: time.Now}
	c.BuildStore = &BuildStore{client: c}
	c.SourcesStore = &SourcesStore{client: c}
	c.ServersStore = &ServersStore{client: c}
	c.LayoutsStore = &LayoutsStore{
		client: c,
		IDs:    &id.UUID{},
	}
	c.DashboardsStore = &DashboardsStore{
		client: c,
		IDs:    &id.UUID{},
	}
	c.UsersStore = &UsersStore{client: c}
	c.OrganizationsStore = &OrganizationsStore{client: c}
	c.ConfigStore = &ConfigStore{client: c}
	c.MappingsStore = &MappingsStore{client: c}
	c.OrganizationConfigStore = &OrganizationConfigStore{client: c}
	return c
}

// WithDB sets the boltdb database for a client. It should not be called
// after a call to Open.
func (c *Client) WithDB(db *bolt.DB) {
	c.db = db
}

// Option to change behavior of Open()
type Option interface {
	Backup() bool
}

// WithBackup returns a Backup
func WithBackup() Option {
	return Backup{}
}

// Backup tells Open to perform a backup prior to initialization
type Backup struct {
}

// Backup returns true
func (b Backup) Backup() bool {
	return true
}

// Open / create boltDB file.
func (c *Client) Open(ctx context.Context, logger chronograf.Logger, build chronograf.BuildInfo, opts ...Option) error {
	if c.db == nil {
		if _, err := os.Stat(c.Path); os.IsNotExist(err) {
			c.isNew = true
		} else if err != nil {
			return err
		}

		// Open database file.
		db, err := bolt.Open(c.Path, 0600, &bolt.Options{Timeout: 1 * time.Second})
		if err != nil {
			return fmt.Errorf(ErrUnableToOpen, err)
		}
		c.db = db
		c.logger = logger

		for _, opt := range opts {
			if opt.Backup() {
				if err = c.backup(ctx, build); err != nil {
					return fmt.Errorf(ErrUnableToBackup, err)
				}
			}
		}
	}

	if err := c.initialize(ctx); err != nil {
		return fmt.Errorf(ErrUnableToInitialize, err)
	}
	if err := c.migrate(ctx, build); err != nil {
		return fmt.Errorf(ErrUnableToMigrate, err)
	}

	return nil
}

// initialize creates Buckets that are missing
func (c *Client) initialize(ctx context.Context) error {
	if err := c.db.Update(func(tx *bolt.Tx) error {
		// Always create SchemaVersions bucket.
		if _, err := tx.CreateBucketIfNotExists(SchemaVersionBucket); err != nil {
			return err
		}
		// Always create Organizations bucket.
		if _, err := tx.CreateBucketIfNotExists(OrganizationsBucket); err != nil {
			return err
		}
		// Always create Sources bucket.
		if _, err := tx.CreateBucketIfNotExists(SourcesBucket); err != nil {
			return err
		}
		// Always create Servers bucket.
		if _, err := tx.CreateBucketIfNotExists(ServersBucket); err != nil {
			return err
		}
		// Always create Layouts bucket.
		if _, err := tx.CreateBucketIfNotExists(LayoutsBucket); err != nil {
			return err
		}
		// Always create Dashboards bucket.
		if _, err := tx.CreateBucketIfNotExists(DashboardsBucket); err != nil {
			return err
		}
		// Always create Users bucket.
		if _, err := tx.CreateBucketIfNotExists(UsersBucket); err != nil {
			return err
		}
		// Always create Config bucket.
		if _, err := tx.CreateBucketIfNotExists(ConfigBucket); err != nil {
			return err
		}
		// Always create Build bucket.
		if _, err := tx.CreateBucketIfNotExists(BuildBucket); err != nil {
			return err
		}
		// Always create Mapping bucket.
		if _, err := tx.CreateBucketIfNotExists(MappingsBucket); err != nil {
			return err
		}
		// Always create OrganizationConfig bucket.
		if _, err := tx.CreateBucketIfNotExists(OrganizationConfigBucket); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	return nil
}

// migrate moves data from an old schema to a new schema in each Store
func (c *Client) migrate(ctx context.Context, build chronograf.BuildInfo) error {
	if c.db != nil {
		// Runtime migrations
		if err := c.OrganizationsStore.Migrate(ctx); err != nil {
			return err
		}
		if err := c.SourcesStore.Migrate(ctx); err != nil {
			return err
		}
		if err := c.ServersStore.Migrate(ctx); err != nil {
			return err
		}
		if err := c.LayoutsStore.Migrate(ctx); err != nil {
			return err
		}
		if err := c.DashboardsStore.Migrate(ctx); err != nil {
			return err
		}
		if err := c.ConfigStore.Migrate(ctx); err != nil {
			return err
		}
		if err := c.BuildStore.Migrate(ctx, build); err != nil {
			return err
		}
		if err := c.MappingsStore.Migrate(ctx); err != nil {
			return err
		}
		if err := c.OrganizationConfigStore.Migrate(ctx); err != nil {
			return err
		}

		MigrateAll(c)
	}
	return nil
}

// Close the connection to the bolt database
func (c *Client) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

// copy creates a copy of the database in toFile
func (c *Client) copy(ctx context.Context, version string) error {
	backupDir := path.Join(path.Dir(c.Path), "backup")
	if _, err := os.Stat(backupDir); os.IsNotExist(err) {
		if err = os.Mkdir(backupDir, 0700); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}

	fromFile, err := os.Open(c.Path)
	if err != nil {
		return err
	}
	defer fromFile.Close()

	toName := fmt.Sprintf("%s.%s", path.Base(c.Path), version)
	toPath := path.Join(backupDir, toName)
	toFile, err := os.OpenFile(toPath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	defer toFile.Close()

	_, err = io.Copy(toFile, fromFile)
	if err != nil {
		return err
	}

	c.logger.Info("Successfully created ", toPath)

	return nil
}

// backup makes a copy of the database to the backup/ directory, if necessary:
// - If this is a fresh install, don't create a backup and store the current version
// - If we are on the same version, don't create a backup
// - If the version has changed, create a backup and store the current version
func (c *Client) backup(ctx context.Context, build chronograf.BuildInfo) error {
	lastBuild, err := c.BuildStore.Get(ctx)
	if err != nil {
		return err
	}
	if lastBuild.Version == build.Version {
		return nil
	}
	if c.isNew {
		return nil
	}

	// The database was pre-existing, and the version has changed
	// and so create a backup

	c.logger.Info("Moving from version ", lastBuild.Version)
	c.logger.Info("Moving to version ", build.Version)

	return c.copy(ctx, lastBuild.Version)
}

func bucket(b []byte, org string) []byte {
	return []byte(path.Join(string(b), org))
}

package influxdb

import (
	"context"
)

// MigrationManager is the manager of migrators.
type MigrationManager interface {
	Register(ms ...Migrator)
	DryRun(ctx context.Context, errCh chan<- error, done chan<- struct{})
	Migrate(ctx context.Context, up bool) (err error)
}

type migrationManager struct {
	// Limit states how many ops per transaction
	Limit int
	MigratorService
	Migrators []Migrator
}

// NewMigrationManager create a new NewMigrationManager.
func NewMigrationManager(svc MigratorService, limit int) MigrationManager {
	return &migrationManager{
		Limit:           limit,
		MigratorService: svc,
		Migrators:       make([]Migrator, 0),
	}
}

// Register add migrator to the manager.
func (mg *migrationManager) Register(ms ...Migrator) {
	mg.Migrators = append(mg.Migrators, ms...)
}

// DryRun will perform the dry run, and stream out error.
func (mg *migrationManager) DryRun(ctx context.Context, errCh chan<- error, done chan<- struct{}) {
	for _, m := range mg.Migrators {
		if !m.NeedMigration() {
			continue
		}
		mg.MigratorService.DryRun(ctx, m, true, errCh)
	}
	// close it
	close(done)
}

// Migrate will perform the migration, store the value.
func (mg *migrationManager) Migrate(ctx context.Context, up bool) (err error) {
	for _, m := range mg.Migrators {
		if !m.NeedMigration() {
			continue
		}
		if err = mg.MigratorService.Migrate(ctx, m, true, mg.Limit); err != nil {
			return err
		}
	}
	return nil
}

// Migrator is the interface of schema migration.
type Migrator interface {
	// Bucket is the bucket of the data stored.
	Bucket() []byte
	// NeedMigration will return whether this migrator is needed at runtime.
	NeedMigration() bool
	// Up convert the old version to the new version.
	Up(src []byte) (dst []byte, err error)
	// Down convert the new version to the old version.
	Down(src []byte) (dst []byte, err error)
}

// MigratorService is the service to fetch and store bytes for migration.
type MigratorService interface {
	// DryRun will do a scan of the current data, and stream the error to the channel.
	DryRun(ctx context.Context, m Migrator, up bool, errCh chan<- error)
	// Migrate will do the migration and store the data.
	Migrate(ctx context.Context, m Migrator, up bool, limit int) error
}

// errors
var (
	ErrNoNeedToConvert = &Error{
		Code: EInvalid,
		Msg:  "This entry doesn't need to be converted.",
	}
	ErrBadSourceType = &Error{
		Code: EInternal,
		Msg:  "This entry is a bad source type.",
	}
	ErrDamagedData = &Error{
		Code: EInternal,
		Msg:  "This entry has a damaged data.",
	}
)

// Version constants.
// Please use any uuid generator to generate a timestamp based uuid.
const (
	NewestTelegrafConfig = "a08b5a68-8ff8-48fd-9f60-c1cd9aea6d3c"
)

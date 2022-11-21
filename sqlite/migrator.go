package sqlite

import (
	"context"
	"embed"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/influxdata/influxdb/v2/kit/migration"
	"go.uber.org/zap"
)

type Migrator struct {
	store *SqlStore
	log   *zap.Logger

	backupPath string
}

func NewMigrator(store *SqlStore, log *zap.Logger) *Migrator {
	return &Migrator{
		store: store,
		log:   log,
	}
}

// SetBackupPath records the filepath where pre-migration state should be written prior to running migrations.
func (m *Migrator) SetBackupPath(path string) {
	m.backupPath = path
}

func (m *Migrator) Up(ctx context.Context, source embed.FS) error {
	return m.UpUntil(ctx, -1, source)
}

// UpUntil migrates until a specific migration.
// -1 or 0 will run all migrations, any other number will run up until that.
// Returns no error untilMigration is less than the already run migrations.
func (m *Migrator) UpUntil(ctx context.Context, untilMigration int, source embed.FS) error {
	knownMigrations, err := source.ReadDir(".")
	if err != nil {
		return err
	}

	// sort the list according to the version number to ensure the migrations are applied in the correct order
	sort.Slice(knownMigrations, func(i, j int) bool {
		return knownMigrations[i].Name() < knownMigrations[j].Name()
	})

	executedMigrations, err := m.store.allMigrationNames()
	if err != nil {
		return err
	}

	var lastMigration int
	for idx := range executedMigrations {
		if idx > len(knownMigrations)-1 || executedMigrations[idx] != dropExtension(knownMigrations[idx].Name()) {
			return migration.ErrInvalidMigration(executedMigrations[idx])
		}

		lastMigration, err = scriptVersion(executedMigrations[idx])
		if err != nil {
			return err
		}
	}

	var migrationsToDo int
	if untilMigration < 1 {
		migrationsToDo = len(knownMigrations[lastMigration:])
		untilMigration = len(knownMigrations)
	} else if untilMigration >= lastMigration {
		migrationsToDo = len(knownMigrations[lastMigration:untilMigration])
	} else {
		return nil
	}

	if migrationsToDo == 0 {
		return nil
	}

	if m.backupPath != "" && lastMigration != 0 {
		m.log.Info("Backing up pre-migration metadata", zap.String("backup_path", m.backupPath))
		if err := func() error {
			out, err := os.Create(m.backupPath)
			if err != nil {
				return err
			}
			defer out.Close()

			if err := m.store.BackupSqlStore(ctx, out); err != nil {
				return err
			}
			return nil
		}(); err != nil {
			return fmt.Errorf("failed to back up pre-migration metadata: %w", err)
		}
	}

	m.log.Info("Bringing up metadata migrations", zap.Int("migration_count", migrationsToDo))

	for _, f := range knownMigrations[lastMigration:untilMigration] {
		n := f.Name()

		m.log.Debug("Executing metadata migration", zap.String("migration_name", n))
		mBytes, err := source.ReadFile(n)
		if err != nil {
			return err
		}

		recordStmt := fmt.Sprintf(`INSERT INTO %s (name) VALUES (%q);`, migrationsTableName, dropExtension(n))

		if err := m.store.execTrans(ctx, string(mBytes)+recordStmt); err != nil {
			return err
		}

	}

	return nil
}

// Down applies the "down" migrations until the SQL database has migrations only >= untilMigration. Use untilMigration = 0 to apply all
// down migrations, which will delete all data from the database.
func (m *Migrator) Down(ctx context.Context, untilMigration int, source embed.FS) error {
	knownMigrations, err := source.ReadDir(".")
	if err != nil {
		return err
	}

	// sort the list according to the version number to ensure the migrations are applied in the correct order
	sort.Slice(knownMigrations, func(i, j int) bool {
		return knownMigrations[i].Name() < knownMigrations[j].Name()
	})

	executedMigrations, err := m.store.allMigrationNames()
	if err != nil {
		return err
	}

	migrationsToDo := len(executedMigrations) - untilMigration
	if migrationsToDo == 0 {
		return nil
	}

	if migrationsToDo < 0 {
		m.log.Warn("SQL metadata is already on a schema older than target, nothing to do")
		return nil
	}

	if m.backupPath != "" {
		m.log.Info("Backing up pre-migration metadata", zap.String("backup_path", m.backupPath))
		if err := func() error {
			out, err := os.Create(m.backupPath)
			if err != nil {
				return err
			}
			defer out.Close()

			if err := m.store.BackupSqlStore(ctx, out); err != nil {
				return err
			}
			return nil
		}(); err != nil {
			return fmt.Errorf("failed to back up pre-migration metadata: %w", err)
		}
	}

	m.log.Info("Tearing down metadata migrations", zap.Int("migration_count", migrationsToDo))

	for i := len(executedMigrations) - 1; i >= untilMigration; i-- {
		downName := knownMigrations[i].Name()
		downNameNoExtension := dropExtension(downName)

		m.log.Debug("Executing metadata migration", zap.String("migration_name", downName))
		mBytes, err := source.ReadFile(downName)
		if err != nil {
			return err
		}

		deleteStmt := fmt.Sprintf(`DELETE FROM %s WHERE name = %q;`, migrationsTableName, downNameNoExtension)

		if err := m.store.execTrans(ctx, deleteStmt+string(mBytes)); err != nil {
			return err
		}
	}

	return nil
}

// extract the version number as an integer from a file named like "0002_migration_name.sql"
func scriptVersion(filename string) (int, error) {
	vString := strings.Split(filename, "_")[0]
	vInt, err := strconv.Atoi(vString)
	if err != nil {
		return 0, err
	}

	return vInt, nil
}

// dropExtension returns the filename excluding anything after the first "."
func dropExtension(filename string) string {
	idx := strings.Index(filename, ".")
	if idx == -1 {
		return filename
	}

	return filename[:idx]
}

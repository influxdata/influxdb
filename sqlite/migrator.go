package sqlite

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"os"
	"strconv"
	"strings"

	"github.com/influxdata/influxdb/v2/kit/migration"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"go.uber.org/zap"
)

func errMigrationNameMismatch(downName, name string) *errors.Error {
	return &errors.Error{
		Code: errors.EInternal,
		Msg:  fmt.Sprintf(`cannot execute down migration %q against up migration record %q`, downName, name),
	}
}

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
	knownMigrations, err := source.ReadDir(".")
	if err != nil {
		return err
	}

	upMigrations := filteredMigrations(knownMigrations, true)

	executedMigrations, err := m.store.allMigrationNames()
	if err != nil {
		return err
	}

	var lastMigration int
	for idx := range executedMigrations {
		if idx > len(upMigrations)-1 || executedMigrations[idx] != dropExtension(upMigrations[idx].Name()) {
			return errInvalidMigration(executedMigrations[idx])
		if idx > len(knownMigrations)-1 || executedMigrations[idx] != dropExtension(knownMigrations[idx].Name()) {
			return migration.ErrInvalidMigration(executedMigrations[idx])
		}

		lastMigration, err = scriptVersion(executedMigrations[idx])
		if err != nil {
			return err
		}
	}

	migrationsToDo := len(upMigrations[lastMigration:])
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

	for _, f := range upMigrations[lastMigration:] {
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

	downMigrations := filteredMigrations(knownMigrations, false)

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
		downName := downMigrations[i].Name()
		downNameNoExtension := dropExtension(downName)
		upName := executedMigrations[i]

		if downNameNoExtension != upName {
			return errMigrationNameMismatch(downNameNoExtension, upName)
		}

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

// filteredMigrations filters the list of migration files to include only "up" migrations or only "down" migrations. The
// returned list is sorted in ascending order.
func filteredMigrations(list []fs.DirEntry, filterForUps bool) []fs.DirEntry {
	out := make([]fs.DirEntry, 0, len(list))

	for _, m := range list {
		if strings.HasSuffix(m.Name(), downMigrationFileSuffix) {
			if !filterForUps {
				out = append(out, m)
			}
		} else {
			if filterForUps {
				out = append(out, m)
			}
		}
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].Name() < out[j].Name()
	})

	return out
}

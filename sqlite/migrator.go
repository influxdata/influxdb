package sqlite

import (
	"context"
	"embed"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

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
	list, err := source.ReadDir(".")
	if err != nil {
		return err
	}
	// sort the list according to the version number to ensure the migrations are applied in the correct order
	sort.Slice(list, func(i, j int) bool {
		return list[i].Name() < list[j].Name()
	})

	// get the current value for user_version from the database
	current, err := m.store.userVersion()
	if err != nil {
		return err
	}

	// get the migration number of the latest migration for logging purposes
	final, err := scriptVersion(list[len(list)-1].Name())
	if err != nil {
		return err
	}

	if final == current {
		return nil
	}

	if m.backupPath != "" && current != 0 {
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

	m.log.Info("Bringing up metadata migrations", zap.Int("migration_count", final-current))

	for _, f := range list {
		n := f.Name()
		// get the version of this migration script
		v, err := scriptVersion(n)
		if err != nil {
			return err
		}

		// get the current value for user_version from the database. this is done in the loop as well to ensure
		// that if for some reason the migrations are out of order, newer migrations are not applied after older ones.
		c, err := m.store.userVersion()
		if err != nil {
			return err
		}

		// if the version of the script is greater than the current user_version,
		// execute the script to apply the migration
		if v > c {
			m.log.Debug("Executing metadata migration", zap.String("migration_name", n))
			mBytes, err := source.ReadFile(n)
			if err != nil {
				return err
			}

			if err := m.store.execTrans(ctx, string(mBytes)); err != nil {
				return err
			}
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

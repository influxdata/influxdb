package sqlite

import (
	"sort"
	"strconv"
	"strings"

	"go.uber.org/zap"
)

type MigrationSource interface {
	ListNames() []string
	MustAssetString(string) string
}

type Migrator struct {
	store *SqlStore
	log   *zap.Logger
}

func NewMigrator(store *SqlStore, log *zap.Logger) *Migrator {
	return &Migrator{
		store: store,
		log:   log,
	}
}

func (m *Migrator) Up(source MigrationSource) error {
	// get the current value for user_version from the database
	c, err := m.store.UserVersion()
	if err != nil {
		return err
	}

	list := source.ListNames()
	// sort the list according to the version number to ensure the migrations are applied in the correct order
	sort.Strings(list)

	m.log.Info("Bringing up metadata migrations", zap.Int("migration_count", len(list)))

	for _, n := range list {
		// get the version of this migration script
		v, err := scriptVersion(n)
		if err != nil {
			return err
		}

		// if the version of the script is greater than the current user_version,
		// execute the script to apply the migration
		if v > c {
			m.log.Debug("Executing metadata migration", zap.String("migration_name", n))
			stmt := source.MustAssetString(n)
			err := m.store.ExecTrans(stmt)
			if err != nil {
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

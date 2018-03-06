package migration

import (
	"context"

	"github.com/influxdata/chronograf/bolt"
)

// Migration defines a database state/schema transition
// 	ID: After the migration is run, this id is stored in the database.
//      We don't want to run a state transition twice
//  Up: The forward-transition function. After a version upgrade, a number
// 			of these will run on database startup in order to bring a user's
// 			schema in line with struct definitions in the new version.
//  Down: The backward-transition function. We don't expect these to be
// 				run on a user's database -- if the user needs to rollback
// 				to a previous version, it will be easier for them to replace
// 				their current database with one of their backups. The primary
// 				purpose of a Down() function is to help contributors move across
// 				development branches that have different schema definitions.
type Migration struct {
	ID   string
	Up   func(ctx context.Context, client bolt.Client) error
	Down func(ctx context.Context, client bolt.Client) error
}

// Migrate runs one migration's Up() function, if it has not already been run
func (m Migration) Migrate(ctx context.Context, client bolt.Client) error {
	complete, err := client.BuildStore.IsMigrationComplete(m.ID)
	if err != nil {
		return err
	}
	if complete {
		return nil
	}

	// client.logger.Info("Running migration (", m.ID, ")")

	if err = m.Up(ctx, client); err != nil {
		return err
	}

	return client.BuildStore.MarkMigrationAsComplete(m.ID)
}

// MigrateAll iterates through all known migrations and runs them in order
func MigrateAll(ctx context.Context, client bolt.Client) error {
	for _, m := range migrations {
		if err := m.Migrate(ctx, client); err != nil {
			return err
		}
	}

	return nil
}

var migrations = []Migration{
	changeIntervalToDuration,
}

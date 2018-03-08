package bolt

import (
	"fmt"

	"github.com/boltdb/bolt"
)

// SchemaVersionBucket stores ids of completed migrations
var SchemaVersionBucket = []byte("SchemaVersions")

// IsMigrationComplete checks for the presence of a particular migration id
func IsMigrationComplete(db bolt.DB, id string) (bool, error) {
	complete := false
	if err := db.View(func(tx *bolt.Tx) error {
		migration := tx.Bucket(SchemaVersionBucket).Get([]byte(id))
		if migration != nil {
			complete = true
		}
		return nil
	}); err != nil {
		// If there's an error, represent this migration as complete
		// so that future downstream code doesn't run it
		return true, err
	}

	return complete, nil
}

// MarkMigrationAsComplete adds the migration id to the schema bucket
func MarkMigrationAsComplete(db bolt.DB, id string) error {
	if err := db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(SchemaVersionBucket).Put([]byte(id), []byte{})
	}); err != nil {
		return err
	}

	return nil
}

// Migration defines a database state/schema transition
//  ID: After the migration is run, this id is stored in the database.
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
	Up   func(db bolt.DB) error
	Down func(db bolt.DB) error
}

// Migrate runs one migration's Up() function, if it has not already been run
func (m Migration) Migrate(db bolt.DB) error {
	_, err := IsMigrationComplete(db, m.ID)
	if err != nil {
		return err
	}
	// if complete {
	// 	return nil
	// }

	fmt.Println("Running a migration!")
	// client.logger.Info("Running migration (", m.ID, ")")

	if err = m.Up(db); err != nil {
		return err
	}

	return MarkMigrationAsComplete(db, m.ID)
}

// MigrateAll iterates through all known migrations and runs them in order
func MigrateAll(db bolt.DB) error {
	for _, m := range migrations {
		err := m.Migrate(db)

		if err != nil {
			return err
		}
	}

	return nil
}

var migrations = []Migration{
	changeIntervalToDuration,
}

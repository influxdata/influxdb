package migration

import (
	"time"
)

// MigratorSetNow sets the now function on the migrator.
// This function is only reachable via tests defined within this
// package folder.
func MigratorSetNow(migrator *Migrator, now func() time.Time) {
	migrator.now = now
}

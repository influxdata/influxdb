package kv

import (
	"testing"
	"time"
)

func MigratorSetNow(t *testing.T, migrator *Migrator, now func() time.Time) {
	t.Helper()

	migrator.now = now
}

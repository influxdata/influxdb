package kv

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	influxdb "github.com/influxdata/influxdb/v2"
	"go.uber.org/zap"
)

var (
	migrationBucket = []byte("migrationsv1")

	// ErrMigrationSpecNotFound is returned when a migration specification is missing
	// for an already applied migration.
	ErrMigrationSpecNotFound = errors.New("migration specification not found")
)

// MigrationState is a type for describing the state of a migration.
type MigrationState uint

const (
	// DownMigrationState is for a migration not yet applied.
	DownMigrationState MigrationState = iota
	// UpMigration State is for a migration which has been applied.
	UpMigrationState
)

// String returns a string representation for a migration state.
func (s MigrationState) String() string {
	switch s {
	case DownMigrationState:
		return "down"
	case UpMigrationState:
		return "up"
	default:
		return "unknown"
	}
}

// Migration is a record of a particular migration.
type Migration struct {
	ID         influxdb.ID    `json:"id"`
	Name       string         `json:"name"`
	State      MigrationState `json:"-"`
	StartedAt  *time.Time     `json:"started_at"`
	FinishedAt *time.Time     `json:"finished_at,omitempty"`
}

// MigrationSpec is a specification for a particular migration.
// It describes the name of the migration and up and down operations
// needed to fulfill the migration.
type MigrationSpec interface {
	MigrationName() string
	Up(ctx context.Context, store Store) error
	Down(ctx context.Context, store Store) error
}

// MigrationFunc is a function which can be used as either an up or down operation.
type MigrationFunc func(context.Context, Store) error

// AnonymousMigration is a utility type for creating migrations from anonyomous functions.
type AnonymousMigration struct {
	name string
	up   MigrationFunc
	down MigrationFunc
}

// NewAnonymousMigration constructs a new migration from a name string and an up and a down function.
func NewAnonymousMigration(name string, up, down MigrationFunc) AnonymousMigration {
	return AnonymousMigration{name, up, down}
}

// Name returns the name of the migration.
func (a AnonymousMigration) MigrationName() string { return a.name }

// Up calls the underlying up migration func.
func (a AnonymousMigration) Up(ctx context.Context, store Store) error { return a.up(ctx, store) }

// Down calls the underlying down migration func.
func (a AnonymousMigration) Down(ctx context.Context, store Store) error { return a.down(ctx, store) }

// Migrator is a type which manages migrations.
// It takes a list of migration specifications and undo (down) all or apply (up) outstanding migrations.
// It records the state of the world in store under the migrations bucket.
type Migrator struct {
	logger         *zap.Logger
	MigrationSpecs []MigrationSpec

	now func() time.Time
}

// NewMigrator constructs and configures a new Migrator.
func NewMigrator(logger *zap.Logger, ms ...MigrationSpec) *Migrator {
	m := &Migrator{
		logger: logger,
		now: func() time.Time {
			return time.Now().UTC()
		},
	}
	m.AddMigrations(ms...)
	return m
}

// AddMigrations appends the provided migration specs onto the Migrator.
func (m *Migrator) AddMigrations(ms ...MigrationSpec) {
	m.MigrationSpecs = append(m.MigrationSpecs, ms...)
}

// Initialize creates the migration bucket if it does not yet exist.
func (m *Migrator) Initialize(ctx context.Context, store Store) error {
	return store.Update(ctx, func(tx Tx) error {
		_, err := tx.Bucket(migrationBucket)
		return err
	})
}

// List returns a list of migrations and their states within the provided store.
func (m *Migrator) List(ctx context.Context, store Store) (migrations []Migration, _ error) {
	if err := m.walk(ctx, store, func(id influxdb.ID, m Migration) {
		migrations = append(migrations, m)
	}); err != nil {
		return nil, err
	}

	migrationsLen := len(migrations)
	for idx, spec := range m.MigrationSpecs[migrationsLen:] {
		migration := Migration{
			ID:   influxdb.ID(migrationsLen + idx + 1),
			Name: spec.MigrationName(),
		}

		migrations = append(migrations, migration)
	}

	return
}

// Up applies each outstanding migration in order.
// Migrations are applied in order from the lowest indexed migration in a down state.
//
// For example, given:
// 0001 add bucket foo         | (up)
// 0002 add bucket bar         | (down)
// 0003 add index "foo on baz" | (down)
//
// Up would apply migration 0002 and then 0003.
func (m *Migrator) Up(ctx context.Context, store Store) error {
	wrapErr := func(err error) error {
		if err == nil {
			return nil
		}

		return fmt.Errorf("up: %w", err)
	}

	var lastMigration int
	if err := m.walk(ctx, store, func(id influxdb.ID, mig Migration) {
		// we're interested in the last up migration
		if mig.State == UpMigrationState {
			lastMigration = int(id)
		}
	}); err != nil {
		return wrapErr(err)
	}

	for idx, spec := range m.MigrationSpecs[lastMigration:] {
		startedAt := m.now()
		migration := Migration{
			ID:        influxdb.ID(lastMigration + idx + 1),
			Name:      spec.MigrationName(),
			StartedAt: &startedAt,
		}

		m.logMigrationEvent(UpMigrationState, migration, "started")

		if err := m.putMigration(ctx, store, migration); err != nil {
			return wrapErr(err)
		}

		if err := spec.Up(ctx, store); err != nil {
			return wrapErr(err)
		}

		finishedAt := m.now()
		migration.FinishedAt = &finishedAt
		migration.State = UpMigrationState

		if err := m.putMigration(ctx, store, migration); err != nil {
			return wrapErr(err)
		}

		m.logMigrationEvent(UpMigrationState, migration, "completed")
	}

	return nil
}

// Down applies the down operation of each currently applied migration.
// Migrations are applied in reverse order from the highest indexed migration in a down state.
//
// For example, given:
// 0001 add bucket foo         | (up)
// 0002 add bucket bar         | (up)
// 0003 add index "foo on baz" | (down)
//
// Down would call down() on 0002 and then on 0001.
func (m *Migrator) Down(ctx context.Context, store Store) (err error) {
	wrapErr := func(err error) error {
		if err == nil {
			return nil
		}

		return fmt.Errorf("down: %w", err)
	}

	var migrations []struct {
		MigrationSpec
		Migration
	}

	if err := m.walk(ctx, store, func(id influxdb.ID, mig Migration) {
		migrations = append(
			migrations,
			struct {
				MigrationSpec
				Migration
			}{
				m.MigrationSpecs[int(id)-1],
				mig,
			},
		)
	}); err != nil {
		return wrapErr(err)
	}

	for i := len(migrations) - 1; i >= 0; i-- {
		migration := migrations[i]

		m.logMigrationEvent(DownMigrationState, migration.Migration, "started")

		if err := migration.MigrationSpec.Down(ctx, store); err != nil {
			return wrapErr(err)
		}

		if err := m.deleteMigration(ctx, store, migration.Migration); err != nil {
			return wrapErr(err)
		}

		m.logMigrationEvent(DownMigrationState, migration.Migration, "completed")
	}

	return nil
}

func (m *Migrator) logMigrationEvent(state MigrationState, mig Migration, event string) {
	m.logger.Info(fmt.Sprintf("Migration %q %s (%s)", mig.Name, event, state))
}

func (m *Migrator) walk(ctx context.Context, store Store, fn func(id influxdb.ID, m Migration)) error {
	if err := store.View(ctx, func(tx Tx) error {
		bkt, err := tx.Bucket(migrationBucket)
		if err != nil {
			return err
		}

		cursor, err := bkt.ForwardCursor(nil)
		if err != nil {
			return err
		}

		return WalkCursor(ctx, cursor, func(k, v []byte) error {
			var id influxdb.ID
			if err := id.Decode(k); err != nil {
				return fmt.Errorf("decoding migration id: %w", err)
			}

			var migration Migration
			if err := json.Unmarshal(v, &migration); err != nil {
				return err
			}

			idx := int(id) - 1
			if idx >= len(m.MigrationSpecs) {
				return fmt.Errorf("migration %q: %w", migration.Name, ErrMigrationSpecNotFound)
			}

			if spec := m.MigrationSpecs[idx]; spec.MigrationName() != migration.Name {
				return fmt.Errorf("expected migration %q, found %q", spec.MigrationName(), migration.Name)
			}

			if migration.FinishedAt != nil {
				migration.State = UpMigrationState
			}

			fn(id, migration)

			return nil
		})
	}); err != nil {
		return fmt.Errorf("reading migrations: %w", err)
	}

	return nil
}

func (*Migrator) putMigration(ctx context.Context, store Store, m Migration) error {
	return store.Update(ctx, func(tx Tx) error {
		bkt, err := tx.Bucket(migrationBucket)
		if err != nil {
			return err
		}

		data, err := json.Marshal(m)
		if err != nil {
			return err
		}

		id, _ := m.ID.Encode()
		return bkt.Put(id, data)
	})
}

func (*Migrator) deleteMigration(ctx context.Context, store Store, m Migration) error {
	return store.Update(ctx, func(tx Tx) error {
		bkt, err := tx.Bucket(migrationBucket)
		if err != nil {
			return err
		}

		id, _ := m.ID.Encode()
		return bkt.Delete(id)
	})
}

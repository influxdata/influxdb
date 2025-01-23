package testutil

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb_pro/influxdb3_license/service/internal/projectpath"

	_ "github.com/lib/pq"
)

const (
	// Default connection parameters
	defaultHost     = "localhost"
	defaultPort     = 5432
	defaultUser     = "postgres"
	defaultPassword = "postgres"
	defaultDBName   = "testdb"
)

// TestDB wraps the database connection and testing utilities
type TestDB struct {
	DB             *sql.DB
	t              *testing.T
	dbName         string
	migrationUps   []string
	migrationDowns []string
}

// NewTestDB creates a new test database instance
func NewTestDB(t *testing.T) *TestDB {
	t.Helper()

	// Create connection string with SSL disabled
	baseConnStr := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=postgres sslmode=disable",
		defaultHost, defaultPort, defaultUser, defaultPassword,
	)

	// Connect to default postgres database first
	db, err := sql.Open("postgres", baseConnStr)
	if err != nil {
		t.Fatalf("Failed to connect to postgres: %v", err)
	}

	// Create test database with unique name
	dbName := fmt.Sprintf("%s_%d", defaultDBName, time.Now().UnixNano())
	_, err = db.Exec(fmt.Sprintf("CREATE DATABASE %s", dbName))
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	// Close connection to postgres and connect to new test database
	db.Close()

	// Connect to the new database with SSL disabled
	testDBConnStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", defaultHost, defaultPort, defaultUser, defaultPassword, dbName)
	db, err = sql.Open("postgres", testDBConnStr)
	if err != nil {
		t.Fatalf("Failed to connect to test database: %v", err)
	}

	// Read migration files
	rootPath := projectpath.Root()
	migrationsDir := filepath.Join(rootPath, "store", "postgres", "migrations")

	// Get the names of all migration files
	migrationFiles, err := os.ReadDir(migrationsDir)
	if err != nil {
		t.Fatalf("Failed to read migration files: %v", err)
	}

	tdb := &TestDB{
		DB:             db,
		t:              t,
		dbName:         dbName,
		migrationUps:   []string{},
		migrationDowns: []string{},
	}

	// Sort migration files into up and down slices
	for _, file := range migrationFiles {
		migrationPath := filepath.Join(migrationsDir, file.Name())

		if strings.Contains(file.Name(), ".up.") {
			tdb.migrationUps = append(tdb.migrationUps, migrationPath)
		} else if strings.Contains(file.Name(), ".down.") {
			tdb.migrationDowns = append(tdb.migrationDowns, migrationPath)
		}
	}

	return tdb
}

// Setup runs migrations and prepares the database for testing
func (tdb *TestDB) Setup(ctx context.Context) {
	tdb.t.Helper()

	// Run up migrations
	for _, file := range tdb.migrationUps {
		migration, err := os.ReadFile(file)
		if err != nil {
			tdb.t.Fatalf("Failed to read migration file: %v", err)
		}

		_, err = tdb.DB.ExecContext(ctx, string(migration))
		if err != nil {
			tdb.t.Fatalf("Failed to run migration: %v", err)
		}
	}
}

// Cleanup closes the database connection and drops the test database
func (tdb *TestDB) Cleanup() {
	tdb.t.Helper()

	// Close connection to test database
	if err := tdb.DB.Close(); err != nil {
		tdb.t.Errorf("Failed to close test database connection: %v", err)
		return
	}

	// Connect to postgres to drop test database
	connStr := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s sslmode=disable",
		defaultHost, defaultPort, defaultUser, defaultPassword,
	)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		tdb.t.Errorf("Failed to connect to postgres for cleanup: %v", err)
		return
	}
	defer db.Close()

	// Drop test database
	_, err = db.Exec(fmt.Sprintf("DROP DATABASE %s WITH (FORCE)", tdb.dbName))
	if err != nil {
		tdb.t.Errorf("Failed to drop test database: %v", err)
	}
}

// TruncateAllTables removes all data from all tables
func (tdb *TestDB) TruncateAllTables(ctx context.Context) {
	tdb.t.Helper()

	tables := []string{"user_ips", "emails", "licenses", "users"}
	for _, table := range tables {
		_, err := tdb.DB.ExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE %s CASCADE", table))
		if err != nil {
			tdb.t.Fatalf("Failed to truncate table %s: %v", table, err)
		}
	}
}

// RunWithinTransaction runs the given function within a transaction and rolls back afterwards
func (tdb *TestDB) RunWithinTransaction(ctx context.Context, fn func(tx *sql.Tx) error) error {
	tx, err := tdb.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func(tx *sql.Tx) { _ = tx.Rollback() }(tx)

	if err := fn(tx); err != nil {
		return err
	}

	return tx.Commit()
}

package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"sync"

	// sqlite3 driver
	_ "github.com/mattn/go-sqlite3"

	"go.uber.org/zap"
)

const (
	DefaultFilename = "influxd.sqlite"
	InmemPath       = ":memory:"
)

// SqlStore is a wrapper around the db and provides basic functionality for maintaining the db
// including flushing the data from the db during end-to-end testing.
type SqlStore struct {
	mu  sync.Mutex
	db  *sql.DB
	log *zap.Logger
}

func NewSqlStore(path string, log *zap.Logger) (*SqlStore, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}
	log.Info("Resources opened", zap.String("path", path))

	// If using an in-memory database, don't allow more than 1 connection. Each connection
	// is given a "new" database. We can't use a shared cache in-memory database because
	// parallel tests that run multiple launchers in the same process will have issues doing
	// concurrent writes to the database. See: https://sqlite.org/inmemorydb.html
	if path == InmemPath {
		db.SetMaxOpenConns(1)
	}

	return &SqlStore{
		db:  db,
		log: log,
	}, nil
}

// Close the connection to the sqlite database
func (s *SqlStore) Close() error {
	err := s.db.Close()
	if err != nil {
		return err
	}

	return nil
}

// Flush deletes all records for all tables in the database.
func (s *SqlStore) Flush(ctx context.Context) {
	tables, err := s.tableNames()
	if err != nil {
		s.log.Fatal("unable to flush sqlite", zap.Error(err))
	}

	for _, t := range tables {
		stmt := fmt.Sprintf("DELETE FROM %s", t)
		err := s.execTrans(ctx, stmt)
		if err != nil {
			s.log.Fatal("unable to flush sqlite", zap.Error(err))
		}
	}
	s.log.Debug("sqlite data flushed successfully")
}

func (s *SqlStore) execTrans(ctx context.Context, stmt string) error {
	// use a lock to prevent two potential simultaneous write operations to the database,
	// which would throw an error
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, stmt)
	if err != nil {
		tx.Rollback()
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (s *SqlStore) userVersion() (int, error) {
	stmt := `PRAGMA user_version`
	res, err := s.queryToStrings(stmt)
	if err != nil {
		return 0, err
	}

	val, err := strconv.Atoi(res[0])
	if err != nil {
		return 0, err
	}

	return val, nil
}

func (s *SqlStore) tableNames() ([]string, error) {
	stmt := `SELECT name FROM sqlite_master WHERE type='table'`
	return s.queryToStrings(stmt)
}

// helper function for running a read-only query resulting in a slice of strings from
// an arbitrary statement.
func (s *SqlStore) queryToStrings(stmt string) ([]string, error) {
	var output []string

	rows, err := s.db.Query(stmt)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var i string
		err = rows.Scan(&i)
		if err != nil {
			return nil, err
		}

		output = append(output, i)
	}

	return output, nil
}

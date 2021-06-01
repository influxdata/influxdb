package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"sync"

	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/jmoiron/sqlx"
	"github.com/mattn/go-sqlite3"

	"go.uber.org/zap"
)

const (
	DefaultFilename = "influxd.sqlite"
	InmemPath       = ":memory:"
)

// SqlStore is a wrapper around the db and provides basic functionality for maintaining the db
// including flushing the data from the db during end-to-end testing.
type SqlStore struct {
	Mu  sync.Mutex
	DB  *sqlx.DB
	log *zap.Logger
}

func NewSqlStore(path string, log *zap.Logger) (*SqlStore, error) {
	db, err := sqlx.Open("sqlite3", path)
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
		DB:  db,
		log: log,
	}, nil
}

// Close the connection to the sqlite database
func (s *SqlStore) Close() error {
	err := s.DB.Close()
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

// BackupSqlStore creates a new temporary database and uses the sqlite backup API
// to back the database up into the temporary database. It then writes the temporary
// database file to the writer. Using the sqlite backup API allows the backup to be
// performed without needing to lock the database, and also allows it to work with
// in-memory databases. See: https://www.sqlite.org/backup.html
//
// The backup works by copying the SOURCE database to the DESTINATION database.
// The SOURCE is the running database that needs to be backed up, and the DESTINATION
// is the resulting backup. The underlying sqlite connection is needed for both
// SOURCE and DESTINATION databases to use the sqlite backup API made available by the
// go-sqlite3 driver.
func (s *SqlStore) BackupSqlStore(ctx context.Context, w io.Writer) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	// create a destination db in a temporary directory to hold the backup.
	tempDir, err := ioutil.TempDir("", "")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tempDir)

	destPath := tempDir + "/" + DefaultFilename
	dest, err := NewSqlStore(destPath, zap.NewNop())
	if err != nil {
		return err
	}

	// get the connection for the destination so we can get the underlying sqlite connection
	destConn, err := dest.DB.Conn(ctx)
	if err != nil {
		return err
	}
	defer destConn.Close()

	// get the sqlite connection for the destination to access the sqlite backup API
	destSqliteConn, err := sqliteFromSqlConn(destConn)
	if err != nil {
		return err
	}

	// get the connection for the source database so we can get the underlying sqlite connection
	srcConn, err := s.DB.Conn(ctx)
	if err != nil {
		return err
	}
	defer srcConn.Close()

	// get the sqlite connection for the source to access the sqlite backup API
	srcSqliteConn, err := sqliteFromSqlConn(srcConn)
	if err != nil {
		return err
	}

	// call Backup on the destination sqlite connection - which initializes the backup
	bk, err := destSqliteConn.Backup("main", srcSqliteConn, "main")
	if err != nil {
		return err
	}

	// perform the backup
	_, err = bk.Step(-1)
	if err != nil {
		return err
	}

	// close the backup once it's done
	err = bk.Finish()
	if err != nil {
		return err
	}

	// open the backup file so it can be copied to the destination writer
	f, err := os.Open(destPath)
	if err != nil {
		return err
	}
	defer f.Close()

	// copy the backup to the destination writer
	_, err = io.Copy(w, f)
	return err
}

func (s *SqlStore) RestoreSqlStore(ctx context.Context, r io.Reader) error {
	return nil
}

func (s *SqlStore) execTrans(ctx context.Context, stmt string) error {
	// use a lock to prevent two potential simultaneous write operations to the database,
	// which would throw an error
	s.Mu.Lock()
	defer s.Mu.Unlock()

	tx, err := s.DB.BeginTx(ctx, nil)
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

	rows, err := s.DB.Query(stmt)
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

// sqliteFromSqlConn returns the underlying sqlite3 connection from an sql connection
func sqliteFromSqlConn(c *sql.Conn) (*sqlite3.SQLiteConn, error) {
	var sqliteConn *sqlite3.SQLiteConn
	err := c.Raw(func(driverConn interface{}) error {
		sqliteConn = driverConn.(*sqlite3.SQLiteConn)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return sqliteConn, nil
}

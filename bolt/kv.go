package bolt

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/influxdata/influxdb/v2/kit/check"
	errors2 "github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/kv/migration"
	"github.com/influxdata/influxdb/v2/kv/migration/all"
	"github.com/influxdata/influxdb/v2/pkg/fs"
	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"
)

// check that *KVStore implement kv.SchemaStore interface.
var _ kv.SchemaStore = (*KVStore)(nil)

const (
	// DefaultProbeInterval is how often the background prober probes the
	// bolt DB. Picked to give /health callers near-fresh results without
	// adding meaningful load to bolt.
	DefaultProbeInterval = 1 * time.Second
	// DefaultProbeStaleness is the default staleness budget on the
	// freshness wrapper. After this much time without a successful probe
	// update, /health reports fail with a "stale" message. Sized at 5x
	// DefaultProbeInterval so a single skipped tick does not flip the
	// status, but a wedged or dead prober goroutine becomes visible
	// within a small multiple of the probe interval.
	DefaultProbeStaleness = 5 * DefaultProbeInterval
	// msgDatabaseNotOpen is the message recorded by runOneProbe when
	// the underlying DB pointer is nil (Open hasn't been called yet, or
	// WithDB was never given a DB).
	msgDatabaseNotOpen = "bolt database not open"
)

// KVStore is a kv.Store backed by boltdb.
type KVStore struct {
	path string
	mu   sync.RWMutex
	db   *bolt.DB
	log  *zap.Logger

	// name is the check.Response name reported by this store. Set by
	// WithCheckName; empty when no caller registers the store with
	// kit/check (recovery, upgrade, tests). The launcher passes its
	// own SubsystemKV here so subsystem identity stays owned in one
	// place outside this package.
	name string
	// probeStaleness is the staleness budget used when probeState is
	// constructed at the end of NewKVStore. Set by WithStaleness;
	// defaults to DefaultProbeStaleness.
	probeStaleness time.Duration

	// Background probe state. The prober is started by Open or WithDB,
	// which runs one synchronous probe to seed probeState and then
	// starts a background loop on DefaultProbeInterval calling
	// runOneProbe. probeState is the live FreshnessResponse returned
	// by Check; its Status() flips to fail when no Update arrives
	// within probeState.staleness.
	//
	// probeStop is nil before startProberOnce; non-nil-and-open while
	// the prober is running; non-nil-and-closed after Close. The first
	// startProberOnce wins; subsequent calls (and post-Close Open/WithDB
	// — user error) no-op when they observe probeStop != nil.
	probeState *check.FreshnessResponse
	probeMu    sync.Mutex
	probeStop  chan struct{}

	noSync bool
}

type KVOption func(*KVStore)

// WithNoSync WARNING: this is useful for tests only
// this skips fsyncing on every commit to improve
// write performance in exchange for no guarantees
// that the db will persist.
func WithNoSync(s *KVStore) {
	s.noSync = true
}

// WithStaleness overrides the staleness budget used when the probe
// state's FreshnessResponse is constructed. Intended for tests that
// need to observe staleness quickly; production callers should accept
// the default.
func WithStaleness(d time.Duration) KVOption {
	return func(s *KVStore) {
		s.probeStaleness = d
	}
}

// WithCheckName sets the name reported by Check and CheckName.
// Required for callers that register the store with a kit/check.Check
// registry; other callers can omit it and leave CheckName empty.
func WithCheckName(name string) KVOption {
	return func(s *KVStore) {
		s.name = name
	}
}

// NewKVStore returns an instance of KVStore with the file at
// the provided path.
func NewKVStore(log *zap.Logger, path string, opts ...KVOption) *KVStore {
	store := &KVStore{
		path:           path,
		log:            log,
		probeStaleness: DefaultProbeStaleness,
	}

	for _, opt := range opts {
		opt(store)
	}

	// Build probeState after options so WithCheckName and WithStaleness
	// can be passed in any order and both are reflected on the wrapper.
	store.probeState = check.NewFreshnessResponse(store.name, store.probeStaleness)

	return store
}

// CheckName satisfies check.NamedChecker so registration via
// AddHealthCheck takes the named-fast-path without an extra wrapper.
// Returns the name configured by WithCheckName, or "" if unset.
func (s *KVStore) CheckName() string { return s.name }

// tempPath returns the path to the temporary file used by Restore().
func (s *KVStore) tempPath() string {
	return s.path + ".tmp"
}

// Open creates boltDB file it doesn't exists and opens it otherwise.
func (s *KVStore) Open(ctx context.Context) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	// Ensure the required directory structure exists.
	if err := os.MkdirAll(filepath.Dir(s.path), 0700); err != nil {
		return fmt.Errorf("unable to create directory %s: %v", s.path, err)
	}

	if _, err := os.Stat(s.path); err != nil && !os.IsNotExist(err) {
		return err
	}

	// Remove any temporary file created during a failed restore.
	if err := os.Remove(s.tempPath()); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("unable to remove boltdb partial restore file: %w", err)
	}

	// Open database file.
	if err := s.openDB(); err != nil {
		return fmt.Errorf("unable to open boltdb file %v", err)
	}

	s.startProberOnce()

	s.log.Info("Resources opened", zap.String("path", s.path))
	return nil
}

func (s *KVStore) openDB() (err error) {
	if s.db, err = bolt.Open(s.path, 0600, &bolt.Options{Timeout: 1 * time.Second}); err != nil {
		return fmt.Errorf("unable to open boltdb file %v", err)
	}
	s.db.NoSync = s.noSync
	return nil
}

// StopProber signals the background prober (if any) to exit. It does
// not close the underlying *bolt.DB. Idempotent and safe to call
// concurrently. Intended for callers that own the DB lifecycle
// separately (e.g. constructed the store via WithDB) and need to stop
// the prober without closing the DB they own.
func (s *KVStore) StopProber() {
	s.probeMu.Lock()
	defer s.probeMu.Unlock()
	if s.probeStop == nil {
		return
	}
	select {
	case <-s.probeStop:
		// Already stopped (idempotent).
	default:
		close(s.probeStop)
	}
}

// Close signals the background prober (if any) to exit and closes the
// underlying bolt database. Close does not wait for the prober: the
// prober simply stops updating the freshness wrapper, which ages out
// to a fail status within DefaultProbeStaleness. There is a window
// after Close returns where Check may still report the last probe's
// result; once staleness elapses, /health flips to fail.
func (s *KVStore) Close() error {
	s.StopProber()

	if db := s.DB(); db != nil {
		return db.Close()
	}
	return nil
}

func (s *KVStore) RLock() {
	s.mu.RLock()
}

func (s *KVStore) RUnlock() {
	s.mu.RUnlock()
}

// DB returns a reference to the current Bolt database.
func (s *KVStore) DB() *bolt.DB {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db
}

// Flush removes all bolt keys within each bucket.
func (s *KVStore) Flush(ctx context.Context) {
	_ = s.DB().Update(
		func(tx *bolt.Tx) error {
			return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
				s.cleanBucket(tx, b)
				return nil
			})
		},
	)
}

func (s *KVStore) cleanBucket(tx *bolt.Tx, b *bolt.Bucket) {
	// nested bucket recursion base case:
	if b == nil {
		return
	}
	c := b.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		_ = v
		if err := c.Delete(); err != nil {
			// clean out nexted buckets
			s.cleanBucket(tx, b.Bucket(k))
		}
	}
}

// WithDB sets the boltdb on the store and starts the background health
// prober if it has not yet been started.
func (s *KVStore) WithDB(db *bolt.DB) {
	s.mu.Lock()
	s.db = db
	s.mu.Unlock()
	s.startProberOnce()
}

// Check returns the live freshness-aware probe state. It is
// non-blocking: a background prober (started by Open / WithDB)
// periodically probes the underlying bolt database via runOneProbe,
// which calls Update on the wrapper. The wrapper's Status/Message
// derive from the cached snapshot's age vs. configured staleness at
// the moment they're read, so a wedged or dead prober flips /health
// to fail once the snapshot ages past the budget. ctx is unused; it
// is retained to satisfy the check.Checker interface.
func (s *KVStore) Check(_ context.Context) check.Response {
	return s.probeState
}

// startProberOnce starts the background prober the first time it is
// called. Subsequent calls — including any Open/WithDB after Close —
// observe probeStop != nil and no-op. One synchronous probe runs
// before the goroutine is spawned so the freshness wrapper has a
// snapshot by the time the caller continues.
func (s *KVStore) startProberOnce() {
	s.probeMu.Lock()
	if s.probeStop != nil {
		s.probeMu.Unlock()
		return
	}
	s.probeStop = make(chan struct{})
	s.probeMu.Unlock()
	s.runOneProbe()
	go s.proberLoop()
}

func (s *KVStore) proberLoop() {
	t := time.NewTicker(DefaultProbeInterval)
	defer t.Stop()
	for {
		select {
		case <-s.probeStop:
			return
		case <-t.C:
			s.runOneProbe()
		}
	}
}

// runOneProbe issues a no-op View against the bolt database and
// updates the freshness wrapper with the result. Called serially by
// proberLoop, so it has no concurrency guard of its own. If db.View
// wedges — bbolt's View cannot be cancelled — proberLoop blocks
// inside this call and no further Updates arrive; the freshness
// wrapper then ages past its staleness budget and /health flips to
// fail. db.Close (on shutdown) unsticks any wedged View, letting the
// goroutine exit.
func (s *KVStore) runOneProbe() {
	db := s.DB()
	if db == nil {
		s.probeState.Update(check.Fail(msgDatabaseNotOpen))
		return
	}
	if err := db.View(func(*bolt.Tx) error { return nil }); err != nil {
		s.probeState.Update(check.Error(err))
		return
	}
	s.probeState.Update(check.Pass())
}

// View opens up a view transaction against the store.
func (s *KVStore) View(ctx context.Context, fn func(tx kv.Tx) error) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	err := s.DB().View(func(tx *bolt.Tx) error {
		return fn(&Tx{
			tx:  tx,
			ctx: ctx,
		})
	})
	return errors2.BoltToInfluxError(err)
}

// Update opens up an update transaction against the store.
func (s *KVStore) Update(ctx context.Context, fn func(tx kv.Tx) error) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	err := s.DB().Update(func(tx *bolt.Tx) error {
		return fn(&Tx{
			tx:  tx,
			ctx: ctx,
		})
	})
	return errors2.BoltToInfluxError(err)
}

// CreateBucket creates a bucket in the underlying boltdb store if it
// does not already exist
func (s *KVStore) CreateBucket(ctx context.Context, name []byte) error {
	return s.DB().Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(name)
		return errors2.BoltToInfluxError(err)
	})
}

// DeleteBucket creates a bucket in the underlying boltdb store if it
// does not already exist
func (s *KVStore) DeleteBucket(ctx context.Context, name []byte) error {
	return s.DB().Update(func(tx *bolt.Tx) error {
		if err := tx.DeleteBucket(name); err != nil && !errors.Is(err, bolt.ErrBucketNotFound) {
			return errors2.BoltToInfluxError(err)
		}

		return nil
	})
}

// Backup copies all K:Vs to a writer, in BoltDB format.
func (s *KVStore) Backup(ctx context.Context, w io.Writer) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	return s.DB().View(func(tx *bolt.Tx) error {
		_, err := tx.WriteTo(w)
		return errors2.BoltToInfluxError(err)
	})
}

// Restore replaces the underlying database with the data from r.
func (s *KVStore) Restore(ctx context.Context, r io.Reader) error {
	if err := func() error {
		f, err := os.Create(s.tempPath())
		if err != nil {
			return err
		}
		defer f.Close()

		if _, err := io.Copy(f, r); err != nil {
			return err
		} else if err := f.Sync(); err != nil {
			return err
		} else if err := f.Close(); err != nil {
			return err
		}

		// Run the migrations on the restored database prior to swapping it in.
		if err := s.migrateRestored(ctx); err != nil {
			return err
		}

		// Swap and reopen under lock.
		s.mu.Lock()
		defer s.mu.Unlock()

		if err := s.db.Close(); err != nil {
			return err
		}

		// Atomically swap temporary file with current DB file.
		if err := fs.RenameFileWithReplacement(s.tempPath(), s.path); err != nil {
			return err
		}

		// Reopen with new database file.
		return s.openDB()
	}(); err != nil {
		os.Remove(s.tempPath()) // clean up on error
		return err
	}
	return nil
}

// migrateRestored opens the database at the temporary path and applies the
// migrations to it. The database at the temporary path is closed after the
// migrations are complete. This should be used as part of the restore
// operation, prior to swapping the restored database with the active database.
func (s *KVStore) migrateRestored(ctx context.Context) error {
	restoredClient := NewClient(s.log.With(zap.String("service", "restored bolt")))
	restoredClient.Path = s.tempPath()
	if err := restoredClient.Open(ctx); err != nil {
		return err
	}
	defer restoredClient.Close()

	restoredKV := NewKVStore(s.log.With(zap.String("service", "restored kvstore-bolt")), s.tempPath())
	restoredKV.WithDB(restoredClient.DB())

	migrator, err := migration.NewMigrator(
		s.log.With(zap.String("service", "bolt restore migrations")),
		restoredKV,
		all.Migrations[:]...,
	)
	if err != nil {
		return err
	}

	return migrator.Up(ctx)
}

// Tx is a light wrapper around a boltdb transaction. It implements kv.Tx.
type Tx struct {
	tx  *bolt.Tx
	ctx context.Context
}

// Context returns the context for the transaction.
func (tx *Tx) Context() context.Context {
	return tx.ctx
}

// WithContext sets the context for the transaction.
func (tx *Tx) WithContext(ctx context.Context) {
	tx.ctx = ctx
}

// Bucket retrieves the bucket named b.
func (tx *Tx) Bucket(b []byte) (kv.Bucket, error) {
	bkt := tx.tx.Bucket(b)
	if bkt == nil {
		return nil, fmt.Errorf("bucket %q: %w", string(b), kv.ErrBucketNotFound)
	}
	return &Bucket{
		bucket: bkt,
	}, nil
}

// Bucket implements kv.Bucket.
type Bucket struct {
	bucket *bolt.Bucket
}

// Get retrieves the value at the provided key.
func (b *Bucket) Get(key []byte) ([]byte, error) {
	val := b.bucket.Get(key)
	if len(val) == 0 {
		return nil, kv.ErrKeyNotFound
	}

	return val, nil
}

// GetBatch retrieves the values for the provided keys.
func (b *Bucket) GetBatch(keys ...[]byte) ([][]byte, error) {
	values := make([][]byte, len(keys))
	for idx, key := range keys {
		val := b.bucket.Get(key)
		if len(val) == 0 {
			continue
		}

		values[idx] = val
	}

	return values, nil
}

// Put sets the value at the provided key.
func (b *Bucket) Put(key []byte, value []byte) error {
	err := b.bucket.Put(key, value)
	if err == bolt.ErrTxNotWritable {
		return kv.ErrTxNotWritable
	}
	return errors2.BoltToInfluxError(err)
}

// Delete removes the provided key.
func (b *Bucket) Delete(key []byte) error {
	err := b.bucket.Delete(key)
	if err == bolt.ErrTxNotWritable {
		return kv.ErrTxNotWritable
	}
	return errors2.BoltToInfluxError(err)
}

// ForwardCursor retrieves a cursor for iterating through the entries
// in the key value store in a given direction (ascending / descending).
func (b *Bucket) ForwardCursor(seek []byte, opts ...kv.CursorOption) (kv.ForwardCursor, error) {
	var (
		cursor     = b.bucket.Cursor()
		config     = kv.NewCursorConfig(opts...)
		key, value []byte
	)

	if len(seek) == 0 && config.Direction == kv.CursorDescending {
		seek, _ = cursor.Last()
	}

	key, value = cursor.Seek(seek)

	if config.Prefix != nil && !bytes.HasPrefix(seek, config.Prefix) {
		return nil, fmt.Errorf("seek bytes %q not prefixed with %q: %w", string(seek), string(config.Prefix), kv.ErrSeekMissingPrefix)
	}

	c := &Cursor{
		cursor: cursor,
		config: config,
	}

	// only remember first seeked item if not skipped
	if !config.SkipFirst {
		c.key = key
		c.value = value
	}

	return c, nil
}

// Cursor retrieves a cursor for iterating through the entries
// in the key value store.
func (b *Bucket) Cursor(opts ...kv.CursorHint) (kv.Cursor, error) {
	return &Cursor{
		cursor: b.bucket.Cursor(),
	}, nil
}

// Cursor is a struct for iterating through the entries
// in the key value store.
type Cursor struct {
	cursor *bolt.Cursor

	// previously seeked key/value
	key, value []byte

	config kv.CursorConfig
	closed bool
	seen   int
}

// Close sets the closed to closed
func (c *Cursor) Close() error {
	c.closed = true

	return nil
}

// Seek seeks for the first key that matches the prefix provided.
func (c *Cursor) Seek(prefix []byte) ([]byte, []byte) {
	if c.closed {
		return nil, nil
	}
	k, v := c.cursor.Seek(prefix)
	if len(k) == 0 && len(v) == 0 {
		return nil, nil
	}
	return k, v
}

// First retrieves the first key value pair in the bucket.
func (c *Cursor) First() ([]byte, []byte) {
	if c.closed {
		return nil, nil
	}
	k, v := c.cursor.First()
	if len(k) == 0 && len(v) == 0 {
		return nil, nil
	}
	return k, v
}

// Last retrieves the last key value pair in the bucket.
func (c *Cursor) Last() ([]byte, []byte) {
	if c.closed {
		return nil, nil
	}
	k, v := c.cursor.Last()
	if len(k) == 0 && len(v) == 0 {
		return nil, nil
	}
	return k, v
}

// Next retrieves the next key in the bucket.
func (c *Cursor) Next() (k []byte, v []byte) {
	if c.closed ||
		c.atLimit() ||
		(c.key != nil && c.missingPrefix(c.key)) {
		return nil, nil
	}

	// get and unset previously seeked values if they exist
	k, v, c.key, c.value = c.key, c.value, nil, nil
	if len(k) > 0 || len(v) > 0 {
		c.seen++
		return
	}

	next := c.cursor.Next
	if c.config.Direction == kv.CursorDescending {
		next = c.cursor.Prev
	}

	k, v = next()
	if (len(k) == 0 && len(v) == 0) || c.missingPrefix(k) {
		return nil, nil
	}

	c.seen++

	return k, v
}

// Prev retrieves the previous key in the bucket.
func (c *Cursor) Prev() (k []byte, v []byte) {
	if c.closed ||
		c.atLimit() ||
		(c.key != nil && c.missingPrefix(c.key)) {
		return nil, nil
	}

	// get and unset previously seeked values if they exist
	k, v, c.key, c.value = c.key, c.value, nil, nil
	if len(k) > 0 && len(v) > 0 {
		c.seen++
		return
	}

	prev := c.cursor.Prev
	if c.config.Direction == kv.CursorDescending {
		prev = c.cursor.Next
	}

	k, v = prev()
	if (len(k) == 0 && len(v) == 0) || c.missingPrefix(k) {
		return nil, nil
	}

	c.seen++

	return k, v
}

func (c *Cursor) missingPrefix(key []byte) bool {
	return c.config.Prefix != nil && !bytes.HasPrefix(key, c.config.Prefix)
}

func (c *Cursor) atLimit() bool {
	return c.config.Limit != nil && c.seen >= *c.config.Limit
}

// Err always returns nil as nothing can go wrong™ during iteration
func (c *Cursor) Err() error {
	return nil
}

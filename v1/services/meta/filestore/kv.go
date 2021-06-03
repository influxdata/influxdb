package filestore

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/pkg/file"
)

type KVStore struct {
	mu         sync.RWMutex
	path       string // root directory where file will be stored
	bucketName string // the name of the bucket
	keyName    string // the name of the file
	full       string
}

func New(path, bucketName, keyName string) *KVStore {
	return &KVStore{path: path, bucketName: bucketName, keyName: keyName, full: filepath.Join(path, keyName)}
}

func (s *KVStore) View(ctx context.Context, f func(kv.Tx) error) error {
	return f(&Tx{kv: s, ctx: ctx})
}

func (s *KVStore) Update(ctx context.Context, f func(kv.Tx) error) error {
	return f(&Tx{kv: s, ctx: ctx, writable: true})
}

// Lock and unlock are only used when doing a backup, so panic if they are called here.
func (s *KVStore) Lock() {
	panic("not implemented")
}

func (s *KVStore) Unlock() {
	panic("not implemented")
}

func (s *KVStore) Backup(ctx context.Context, w io.Writer) error {
	panic("not implemented")
}

func (s *KVStore) Restore(ctx context.Context, r io.Reader) error {
	panic("not implemented")
}

// Tx is an in memory transaction.
// TODO: make transactions actually transactional
type Tx struct {
	kv       *KVStore
	ctx      context.Context
	writable bool
}

func (t *Tx) Bucket(b []byte) (kv.Bucket, error) {
	if string(b) != t.kv.bucketName {
		return nil, kv.ErrBucketNotFound
	}

	return t.kv, nil
}

func (t *Tx) Context() context.Context {
	return t.ctx
}

func (t *Tx) WithContext(ctx context.Context) {
	t.ctx = ctx
}

// region: kv.Bucket implementation

func (s *KVStore) checkKey(key []byte) bool {
	return string(key) == s.keyName
}

func (s *KVStore) Get(key []byte) ([]byte, error) {
	if !s.checkKey(key) {
		return nil, kv.ErrKeyNotFound
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.get()
}

func (s *KVStore) GetBatch(keys ...[]byte) (values [][]byte, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	values = make([][]byte, len(keys))
	for i := range keys {
		if string(keys[i]) == s.keyName {
			if values[i], err = s.get(); err != nil {
				return nil, err
			}
		}
	}

	return values, nil
}

func (s *KVStore) get() ([]byte, error) {
	if d, err := ioutil.ReadFile(s.full); os.IsNotExist(err) {
		return nil, kv.ErrKeyNotFound
	} else if err != nil {
		return nil, err
	} else {
		return d, nil
	}
}

func (s *KVStore) Cursor(hints ...kv.CursorHint) (kv.Cursor, error) {
	panic("not implemented")
}

func (s *KVStore) Put(key, value []byte) error {
	if !s.checkKey(key) {
		return kv.ErrKeyNotFound
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	tmpFile := s.full + "tmp"

	f, err := os.Create(tmpFile)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	if _, err := f.Write(value); err != nil {
		return err
	}

	if err = f.Sync(); err != nil {
		return err
	}

	// close file handle before renaming to support Windows
	if err = f.Close(); err != nil {
		return err
	}

	return file.RenameFile(tmpFile, s.full)
}

func (s *KVStore) Delete(key []byte) error {
	if !s.checkKey(key) {
		return kv.ErrKeyNotFound
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return os.Remove(s.full)
}

func (s *KVStore) ForwardCursor(seek []byte, opts ...kv.CursorOption) (kv.ForwardCursor, error) {
	panic("not implemented")
}

// endregion

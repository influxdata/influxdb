package kv

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"

	platform "github.com/influxdata/influxdb"
)

var (
	kvlogBucket = []byte("keyvaluelogv1")
	kvlogIndex  = []byte("keyvaluelogindexv1")
)

var _ platform.KeyValueLog = (*Service)(nil)

type keyValueLogBounds struct {
	Start int64 `json:"start"`
	Stop  int64 `json:"stop"`
}

func newKeyValueLogBounds(now time.Time) *keyValueLogBounds {
	return &keyValueLogBounds{
		Start: now.UTC().UnixNano(),
		Stop:  now.UTC().UnixNano(),
	}
}

func (b *keyValueLogBounds) update(t time.Time) {
	now := t.UTC().UnixNano()
	if now < b.Start {
		b.Start = now
	} else if b.Stop < now {
		b.Stop = now
	}
}

// StartTime retrieves the start value of a bounds as a time.Time
func (b *keyValueLogBounds) StartTime() time.Time {
	return time.Unix(0, b.Start)
}

// StopTime retrieves the stop value of a bounds as a time.Time
func (b *keyValueLogBounds) StopTime() time.Time {
	return time.Unix(0, b.Stop)
}

// Bounds returns the key boundaries for the keyvaluelog for a resourceType/resourceID pair.
func (b *keyValueLogBounds) Bounds(k []byte) ([]byte, []byte, error) {
	start, err := encodeLogEntryKey(k, b.Start)
	if err != nil {
		return nil, nil, err
	}
	stop, err := encodeLogEntryKey(k, b.Stop)
	if err != nil {
		return nil, nil, err
	}
	return start, stop, nil
}

func encodeLogEntryKey(key []byte, v int64) ([]byte, error) {
	prefix := encodeKeyValueIndexKey(key)
	k := make([]byte, len(prefix)+8)

	buf := bytes.NewBuffer(k)
	_, err := buf.Write(prefix)
	if err != nil {
		return nil, err
	}

	// This needs to be big-endian so that the iteration order is preserved when scanning keys
	if err := binary.Write(buf, binary.BigEndian, v); err != nil {
		return nil, err
	}
	return buf.Bytes(), err
}

func decodeLogEntryKey(key []byte) ([]byte, time.Time, error) {
	buf := bytes.NewReader(key[len(key)-8:])
	var ts int64
	// This needs to be big-endian so that the iteration order is preserved when scanning keys
	err := binary.Read(buf, binary.BigEndian, &ts)
	if err != nil {
		return nil, time.Unix(0, 0), err
	}
	return key[:len(key)-8], time.Unix(0, ts), nil
}

func encodeKeyValueIndexKey(k []byte) []byte {
	// keys produced must be fixed length to ensure that we can iterate through the keyspace without any error.
	h := sha1.New()
	h.Write([]byte(k))
	return h.Sum(nil)
}

func (s *Service) initializeKVLog(ctx context.Context, tx Tx) error {
	if _, err := tx.Bucket(kvlogBucket); err != nil {
		return err
	}
	if _, err := tx.Bucket(kvlogIndex); err != nil {
		return err
	}
	return nil
}

var errKeyValueLogBoundsNotFound = fmt.Errorf("oplog not found")

func (s *Service) getKeyValueLogBounds(ctx context.Context, tx Tx, key []byte) (*keyValueLogBounds, error) {
	k := encodeKeyValueIndexKey(key)

	b, err := tx.Bucket(kvlogIndex)
	if err != nil {
		return nil, err
	}

	v, err := b.Get(k)
	if IsNotFound(err) {
		return nil, errKeyValueLogBoundsNotFound
	}

	if err != nil {
		return nil, err
	}

	bounds := &keyValueLogBounds{}
	if err := json.Unmarshal(v, bounds); err != nil {
		return nil, err
	}

	return bounds, nil
}

func (s *Service) putKeyValueLogBounds(ctx context.Context, tx Tx, key []byte, bounds *keyValueLogBounds) error {
	k := encodeKeyValueIndexKey(key)

	v, err := json.Marshal(bounds)
	if err != nil {
		return err
	}

	b, err := tx.Bucket(kvlogIndex)
	if err != nil {
		return err
	}

	if err := b.Put(k, v); err != nil {
		return err
	}

	return nil
}

func (s *Service) updateKeyValueLogBounds(ctx context.Context, tx Tx, k []byte, t time.Time) error {
	// retrieve the keyValue log boundaries
	bounds, err := s.getKeyValueLogBounds(ctx, tx, k)
	if err != nil && err != errKeyValueLogBoundsNotFound {
		return err
	}

	if err == errKeyValueLogBoundsNotFound {
		// if the bounds don't exist yet, create them
		bounds = newKeyValueLogBounds(t)
	}

	// update the bounds to if needed
	bounds.update(t)
	if err := s.putKeyValueLogBounds(ctx, tx, k, bounds); err != nil {
		return err
	}

	return nil
}

// ForEachLogEntry retrieves the keyValue log for a resource type ID combination. KeyValues may be returned in ascending and descending order.
func (s *Service) ForEachLogEntry(ctx context.Context, k []byte, opts platform.FindOptions, fn func([]byte, time.Time) error) error {
	return s.kv.View(ctx, func(tx Tx) error {
		return s.forEachLogEntry(ctx, tx, k, opts, fn)
	})
}

func (s *Service) forEachLogEntry(ctx context.Context, tx Tx, k []byte, opts platform.FindOptions, fn func([]byte, time.Time) error) error {
	b, err := s.getKeyValueLogBounds(ctx, tx, k)
	if err != nil {
		return err
	}

	bkt, err := tx.Bucket(kvlogBucket)
	if err != nil {
		return err
	}

	cur, err := bkt.Cursor()
	if err != nil {
		return err
	}

	next := cur.Next
	startKey, stopKey, err := b.Bounds(k)
	if err != nil {
		return err
	}

	if opts.Descending {
		next = cur.Prev
		startKey, stopKey = stopKey, startKey
	}

	k, v := cur.Seek(startKey)
	if !bytes.Equal(k, startKey) {
		return fmt.Errorf("the first key not the key found in the log bounds. This should be impossible. Please report this error")
	}

	count := 0

	if opts.Offset == 0 {
		// Seek returns the kv at the position that was seeked to which should be the first element
		// in the sequence of keyValues. If this condition is reached we need to start of iteration
		// at 1 instead of 0.
		_, ts, err := decodeLogEntryKey(k)
		if err != nil {
			return err
		}
		if err := fn(v, ts); err != nil {
			return err
		}
		count++
		if bytes.Equal(startKey, stopKey) {
			// If the start and stop are the same, then there is only a single entry in the log
			return nil
		}
	} else {
		// Skip offset many items
		for i := 0; i < opts.Offset-1; i++ {
			k, _ := next()
			if bytes.Equal(k, stopKey) {
				return nil
			}
		}
	}

	for {
		if count >= opts.Limit && opts.Limit != 0 {
			break
		}

		k, v := next()

		_, ts, err := decodeLogEntryKey(k)
		if err != nil {
			return err
		}

		if err := fn(v, ts); err != nil {
			return err
		}

		if bytes.Equal(k, stopKey) {
			// if we've reached the stop key, there are no keys log entries left
			// in the keyspace.
			break
		}

		count++
	}

	return nil

}

// AddLogEntry logs an keyValue for a particular resource type ID pairing.
func (s *Service) AddLogEntry(ctx context.Context, k, v []byte, t time.Time) error {
	return s.kv.Update(ctx, func(tx Tx) error {
		return s.addLogEntry(ctx, tx, k, v, t)
	})
}

func (s *Service) addLogEntry(ctx context.Context, tx Tx, k, v []byte, t time.Time) error {
	if err := s.updateKeyValueLogBounds(ctx, tx, k, t); err != nil {
		return err
	}

	if err := s.putLogEntry(ctx, tx, k, v, t); err != nil {
		return err
	}

	return nil
}

func (s *Service) putLogEntry(ctx context.Context, tx Tx, k, v []byte, t time.Time) error {
	key, err := encodeLogEntryKey(k, t.UTC().UnixNano())
	if err != nil {
		return err
	}

	b, err := tx.Bucket(kvlogBucket)
	if err != nil {
		return err
	}

	if err := b.Put(key, v); err != nil {
		return err
	}

	return nil
}

func (s *Service) getLogEntry(ctx context.Context, tx Tx, k []byte, t time.Time) ([]byte, time.Time, error) {
	key, err := encodeLogEntryKey(k, t.UTC().UnixNano())
	if err != nil {
		return nil, t, err
	}

	b, err := tx.Bucket(kvlogBucket)
	if err != nil {
		return nil, t, err
	}

	v, err := b.Get(key)
	if IsNotFound(err) {
		return nil, t, fmt.Errorf("log entry not found")
	}

	if err != nil {
		return nil, t, err
	}

	return v, t, nil
}

// FirstLogEntry retrieves the first log entry for a key value log.
func (s *Service) FirstLogEntry(ctx context.Context, k []byte) ([]byte, time.Time, error) {
	var v []byte
	var t time.Time

	err := s.kv.View(ctx, func(tx Tx) error {
		val, ts, err := s.firstLogEntry(ctx, tx, k)
		if err != nil {
			return err
		}

		v, t = val, ts

		return nil
	})

	if err != nil {
		return nil, t, err
	}

	return v, t, nil
}

// LastLogEntry retrieves the first log entry for a key value log.
func (s *Service) LastLogEntry(ctx context.Context, k []byte) ([]byte, time.Time, error) {
	var v []byte
	var t time.Time

	err := s.kv.View(ctx, func(tx Tx) error {
		val, ts, err := s.lastLogEntry(ctx, tx, k)
		if err != nil {
			return err
		}

		v, t = val, ts

		return nil
	})

	if err != nil {
		return nil, t, err
	}

	return v, t, nil
}

func (s *Service) firstLogEntry(ctx context.Context, tx Tx, k []byte) ([]byte, time.Time, error) {
	bounds, err := s.getKeyValueLogBounds(ctx, tx, k)
	if err != nil {
		return nil, bounds.StartTime(), err
	}

	return s.getLogEntry(ctx, tx, k, bounds.StartTime())
}

func (s *Service) lastLogEntry(ctx context.Context, tx Tx, k []byte) ([]byte, time.Time, error) {
	bounds, err := s.getKeyValueLogBounds(ctx, tx, k)
	if err != nil {
		return nil, bounds.StopTime(), err
	}

	return s.getLogEntry(ctx, tx, k, bounds.StopTime())
}

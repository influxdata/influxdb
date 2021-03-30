package kv

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"

	"github.com/influxdata/influxdb/v2/kit/platform/errors"

	platform "github.com/influxdata/influxdb/v2"
)

var (
	kvlogBucket = []byte("keyvaluelogv1")
	kvlogIndex  = []byte("keyvaluelogindexv1")

	// ErrKeyValueLogBoundsNotFound is returned when oplog entries cannot be located
	// for the provided bounds
	ErrKeyValueLogBoundsNotFound = &errors.Error{
		Code: errors.ENotFound,
		Msg:  "oplog not found",
	}
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

func (s *Service) getKeyValueLogBounds(ctx context.Context, tx Tx, key []byte) (*keyValueLogBounds, error) {
	k := encodeKeyValueIndexKey(key)

	b, err := tx.Bucket(kvlogIndex)
	if err != nil {
		return nil, err
	}

	v, err := b.Get(k)
	if IsNotFound(err) {
		return nil, ErrKeyValueLogBoundsNotFound
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
	if err != nil && err != ErrKeyValueLogBoundsNotFound {
		return err
	}

	if err == ErrKeyValueLogBoundsNotFound {
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
		return s.ForEachLogEntryTx(ctx, tx, k, opts, fn)
	})
}

func (s *Service) ForEachLogEntryTx(ctx context.Context, tx Tx, k []byte, opts platform.FindOptions, fn func([]byte, time.Time) error) error {
	b, err := s.getKeyValueLogBounds(ctx, tx, k)
	if err != nil {
		return err
	}

	bkt, err := tx.Bucket(kvlogBucket)
	if err != nil {
		return err
	}

	startKey, stopKey, err := b.Bounds(k)
	if err != nil {
		return err
	}

	direction := CursorAscending
	if opts.Descending {
		direction = CursorDescending
		startKey, stopKey = stopKey, startKey
	}
	cur, err := bkt.ForwardCursor(startKey, WithCursorDirection(direction))
	if err != nil {
		return err
	}

	count := 0

	if opts.Offset > 0 {
		// Skip offset many items
		for i := 0; i < opts.Offset; i++ {
			k, _ := cur.Next()
			if bytes.Equal(k, stopKey) {
				return nil
			}
		}
	}

	for k, v := cur.Next(); k != nil; k, v = cur.Next() {

		if count >= opts.Limit && opts.Limit != 0 {
			break
		}

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
		return s.AddLogEntryTx(ctx, tx, k, v, t)
	})
}

func (s *Service) AddLogEntryTx(ctx context.Context, tx Tx, k, v []byte, t time.Time) error {
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
		return nil, time.Time{}, err
	}

	return s.getLogEntry(ctx, tx, k, bounds.StartTime())
}

func (s *Service) lastLogEntry(ctx context.Context, tx Tx, k []byte) ([]byte, time.Time, error) {
	bounds, err := s.getKeyValueLogBounds(ctx, tx, k)
	if err != nil {
		return nil, time.Time{}, err
	}

	return s.getLogEntry(ctx, tx, k, bounds.StopTime())
}

package kv

import (
	"context"
	"fmt"

	influxdb "github.com/influxdata/influxdb"
)

// DryRun will do a scan of the current data, and stream the error to the channel.
func (s *Service) DryRun(ctx context.Context, m influxdb.Migrator, up bool, errCh chan<- error) {
	_ = s.kv.View(ctx, func(tx Tx) error {
		op := "kv/DryRun"
		_, cur, err := getBucketCursor(ctx, tx, m, op)
		if err != nil {
			return err
		}
		for key, src := cur.First(); key != nil; key, src = cur.Next() {
			if up {
				_, err = m.Up(src)
			} else {
				_, err = m.Down(src)
			}
			if err != nil && influxdb.ErrorCode(err) != influxdb.EInvalid {
				errCh <- &influxdb.Error{
					Msg: fmt.Sprintf("err in migration dry run, k: %s; v: %s", string(key), string(src)),
					Err: err,
					Op:  op,
				}
				continue
			}
		}
		return nil
	})
}

// Migrate will do the migration and store the data.
func (s *Service) Migrate(ctx context.Context, m influxdb.Migrator, up bool, limit int) (err error) {
	op := "kv/Migrate"
	var cur Cursor
	var counter int
	err = s.kv.View(ctx, func(tx Tx) error {
		_, cur, err = getBucketCursor(ctx, tx, m, op)
		if err != nil {
			return err
		}
		for key, _ := cur.First(); key != nil; key, _ = cur.Next() {
			counter++
		}
		return nil
	})
	if err != nil {
		return err
	}
	for i := 0; i < counter; i += limit {
		err = s.kv.Update(ctx, func(tx Tx) error {
			return migrate(ctx, tx, m, up, i, i+limit-1)
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func getBucketCursor(ctx context.Context, tx Tx, m influxdb.Migrator, op string) (bucket Bucket, cur Cursor, err error) {
	bucket, err = tx.Bucket(m.Bucket())
	if err != nil {
		return bucket, cur, &influxdb.Error{
			Code: influxdb.EInternal,
			Msg:  fmt.Sprintf("Unable to connect to kv service. Please try again; Err: %v", err),
			Op:   op,
		}
	}
	cur, err = bucket.Cursor()
	if err != nil {
		return bucket, cur, &influxdb.Error{
			Code: influxdb.EInternal,
			Msg:  fmt.Sprintf("unexpected error connecting kv service: %v", bucket),
			Err:  err,
			Op:   op,
		}
	}
	return bucket, cur, nil
}

func migrate(ctx context.Context, tx Tx, m influxdb.Migrator, up bool, start, end int) error {
	op := "kv/Migrate"
	bucket, cur, err := getBucketCursor(ctx, tx, m, op)
	if err != nil {
		return err
	}
	var dst []byte
	var counter int
	for key, src := cur.First(); key != nil; key, src = cur.Next() {
		if counter < start {
			continue
		}
		if counter > end {
			break
		}
		counter++
		if up {
			dst, err = m.Up(src)
		} else {
			dst, err = m.Down(src)
		}
		if err != nil && influxdb.ErrorCode(err) != influxdb.EInvalid {
			return &influxdb.Error{
				Msg: fmt.Sprintf("err in migration, k: %s; v: %s", string(key), string(src)),
				Err: err,
				Op:  "kv/Migrate",
			}
		}
		if err = bucket.Put(key, dst); err != nil {
			return &influxdb.Error{
				Msg: fmt.Sprintf("err in migration, k: %s; v: %s", string(key), string(src)),
				Err: err,
				Op:  "kv/Migrate",
			}
		}
	}
	return nil
}

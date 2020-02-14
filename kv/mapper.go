package kv

import (
	"context"

	"go.uber.org/zap"
)

type kvMapper struct {
	log *zap.Logger
	kv  Store
}

func NewMapper(log *zap.Logger, kv Store) *kvMapper {
	i := &kvMapper{
		log: log,
		kv:  kv,
	}
	return i
}

type MappingFunc func([]byte, []byte) ([]byte, []byte)

func (m *kvMapper) Map(ctx context.Context, sourceBucket, destinationBucket []byte, mf MappingFunc) error {

	// create a for loop that can run forever
	// this is required because the for loop creates a transaction and after a curtian number of loops in a forward cursor it will close
	var seek []byte
	var complete bool
	for {
		// use a counter to indicate how many operations we have used this transaction
		// once we exceed 100 operations we should cycle in a new counter
		counter := 0
		err := m.kv.Update(ctx, func(tx Tx) error {
			src, err := tx.Bucket(sourceBucket)
			if err != nil {
				return err
			}

			dest, err := tx.Bucket(destinationBucket)
			if err != nil {
				return err
			}
			var cursor ForwardCursor
			if seek != nil {
				cursor, err = src.ForwardCursor(seek, WithCursorSkipFirstItem())
			} else {
				cursor, err = src.ForwardCursor(nil)
			}
			if err != nil {
				return err
			}
			for k, v := cursor.Next(); true; k, v = cursor.Next() {
				if k == nil {
					complete = true
					break
				}
				counter++

				newK, newV := mf(k, v)
				if newK != nil {
					// add newk to the batch
					err := dest.Put(newK, newV)
					if err != nil {
						return err
					}
					counter++

				}
				if counter >= 100 {
					seek = k
					break
				}
			}

			if err := cursor.Err(); err != nil {
				return err
			}

			return cursor.Close()
		})
		if err != nil {
			return err
		}
		if complete {
			break
		}
	}
	return nil
}

package tenant

import (
	"context"
	"errors"
	"sync"

	"github.com/influxdata/influxdb/kv"
)

var errRollback = errors.New("rollback")

type Store struct {
	kvStore kv.Store
}

type TxType int

const (
	Write TxType = iota
	Read
)

type Tx struct {
	kv.Tx
	close    chan error // Commit() closes the channel, and Rollback sends a rollback error on the channel
	response chan error // used to get the response from the transaction handling go routine
}

func (tx *Tx) Commit() error {
	close(tx.close)
	return <-tx.response
}

func (tx *Tx) Rollback() error {
	tx.close <- errRollback
	close(tx.close)
	err := <-tx.response
	if err != errRollback {
		return err
	}
	return nil
}

func NewStore(kvStore kv.Store) (*Store, error) {
	st := &Store{
		kvStore: kvStore,
	}
	return st, st.setup()
}

func (s *Store) Begin(txType TxType) (*Tx, error) {
	tx := Tx{
		close:    make(chan error, 1),
		response: make(chan error, 1),
	}
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		txFunc := s.kvStore.View
		if txType == Write {
			txFunc = s.kvStore.Update
		}
		tx.response <- txFunc(context.Background(), func(kvTx kv.Tx) error {
			tx.Tx = kvTx
			wg.Done()
			return <-tx.close
		})
	}()
	wg.Wait()
	return &tx, nil
}

func (s *Store) setup() error {
	tx, err := s.Begin(Write)
	if err != nil {
		return err
	}

	if _, err := tx.Bucket(userBucket); err != nil {
		tx.Rollback()
		return err
	}

	if _, err := tx.Bucket(userIndex); err != nil {
		tx.Rollback()
		return err
	}

	if _, err := tx.Bucket(urmBucket); err != nil {
		tx.Rollback()
		return err
	}
	if _, err := tx.Bucket(organizationBucket); err != nil {
		tx.Rollback()
		return err
	}

	if _, err := tx.Bucket(organizationIndex); err != nil {
		tx.Rollback()
		return err
	}

	if _, err := tx.Bucket(bucketBucket); err != nil {
		tx.Rollback()
		return err
	}

	if _, err := tx.Bucket(bucketIndex); err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

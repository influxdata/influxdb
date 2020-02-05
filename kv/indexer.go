package kv

import (
	"context"
	"sync"

	"go.uber.org/zap"
)

type kvIndexer struct {
	log       *zap.Logger
	kv        Store
	ctx       context.Context
	cancel    context.CancelFunc
	indexChan chan indexBatch
	finished  chan struct{}
	oncer     sync.Once
}

type indexBatch struct {
	bucketName []byte
	keys       [][]byte
}

func NewIndexer(log *zap.Logger, kv Store) *kvIndexer {
	ctx, cancel := context.WithCancel(context.Background())
	i := &kvIndexer{
		log:       log,
		kv:        kv,
		ctx:       ctx,
		cancel:    cancel,
		indexChan: make(chan indexBatch, 10),
		finished:  make(chan struct{}),
	}

	go i.workIndexes()
	return i
}

func (i *kvIndexer) AddIndex(bucketName []byte, keys [][]byte) {
	// check for close
	select {
	case <-i.ctx.Done():
		return
	case i.indexChan <- indexBatch{bucketName, keys}:
	}
}

func (i *kvIndexer) workIndexes() {
	defer close(i.finished)
	for batch := range i.indexChan {
		// open update tx
		err := i.kv.Update(i.ctx, func(tx Tx) error {
			// create a bucket for this batch
			bucket, err := tx.Bucket(batch.bucketName)
			if err != nil {
				return err
			}
			// insert all the keys
			for _, key := range batch.keys {
				err := bucket.Put(key, nil)
				if err != nil {
					return err
				}
			}
			return nil
		})

		if err != nil {
			//only option is to log
			i.log.Error("failed to update index bucket", zap.Error(err))
		}
	}
}

func (i *kvIndexer) Stop() {
	i.cancel()
	i.oncer.Do(func() {
		close(i.indexChan)
	})

	<-i.finished
}

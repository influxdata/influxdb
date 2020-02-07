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
	wg        sync.WaitGroup
	working   chan struct{}
}

type indexBatch struct {
	bucketName []byte
	idxs       map[string][]byte
}

func NewIndexer(log *zap.Logger, kv Store) *kvIndexer {
	ctx, cancel := context.WithCancel(context.Background())
	return &kvIndexer{
		log:       log,
		kv:        kv,
		ctx:       ctx,
		cancel:    cancel,
		indexChan: make(chan indexBatch, 10),
		working:   make(chan struct{}, 1),
	}
}

func (i *kvIndexer) AddToIndex(bucketName []byte, idxs map[string][]byte) {
	// check for close
	select {
	case <-i.ctx.Done():
		return
	case i.indexChan <- indexBatch{bucketName, idxs}:
	}

	// add to the waitgroup and start the work process
	select {
	case i.working <- struct{}{}:
		// i was able to insert i should start a worker
		i.wg.Add(1)
		go i.workIndexes()
	default:
		// we have reached our worker limit and we cannot start any more.
		return
	}
}

func (i *kvIndexer) workIndexes() {
	// let the system know we have finished
	defer i.wg.Done()
	// releasee the worker hold so the system can start more later
	defer func() { <-i.working }()

	for {
		select {
		case batch := <-i.indexChan:
			// open update tx
			err := i.kv.Update(i.ctx, func(tx Tx) error {
				// create a bucket for this batch
				bucket, err := tx.Bucket(batch.bucketName)
				if err != nil {
					return err
				}
				// insert all the keys
				for k, v := range batch.idxs {
					err := bucket.Put([]byte(k), v)
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
		default:
			// we have finished working
			return
		}
	}
}

func (i *kvIndexer) Wait() {
	i.wg.Wait()
}

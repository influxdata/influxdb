package datastore

import (
	"cluster"
	"parser"

	"errors"

	"github.com/VividCortex/bolt"
)

func (s *BoltShard) executeListSeriesQuery(querySpec *parser.QuerySpec, processor cluster.QueryProcessor) error {
	databaseName := querySpec.Database()

	var (
		db  *bolt.DB
		ok  bool
		err error
	)

	db, ok = s.dbs[databaseName]
	if !ok {
		db, err = bolt.Open(s.baseDir+"/"+databaseName, 0666)
		if err != nil {
			return err
		}

		s.dbs[databaseName] = db
	}

	return db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("series"))
		if b == nil {
			return errors.New("datastore: bucket does not exist")
		}

		c := b.Cursor()

		for key, _ := c.First(); key != nil; key, _ = c.Next() {
			strKey := string(key)

			shouldContinue := processor.YieldPoint(&strKey, nil, nil)
			if !shouldContinue {
				return nil
			}
		}

		return nil
	})
}

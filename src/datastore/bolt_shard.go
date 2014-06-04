package datastore

import (
	"bytes"
	"cluster"
	"encoding/binary"
	"parser"
	"protocol"

	"code.google.com/p/goprotobuf/proto"
	"github.com/VividCortex/bolt"
)

type BoltShard struct {
	baseDir string
	dbs     map[string]*bolt.DB
	closed  bool
}

func NewBoltShard(baseDir string) *BoltShard {
	return &BoltShard{
		baseDir: baseDir,
		closed:  false,
		dbs:     make(map[string]*bolt.DB),
	}
}

func (s *BoltShard) DropDatabase(database string) error {
	return nil
}

func (s *BoltShard) close() {
	for _, db := range s.dbs {
		db.Close()
	}
	s.closed = true
}

func (s *BoltShard) IsClosed() bool {
	return s.closed
}

func (s *BoltShard) Query(querySpec *parser.QuerySpec, processor cluster.QueryProcessor) error {
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

	// special case series
	switch {
	case querySpec.IsListSeriesQuery():
		return s.executeListSeriesQuery(db, querySpec, processor)
	default:
		return s.executeSeriesQuery(db, querySpec, processor)
	}

	return nil
}

func (s *BoltShard) Write(database string, series []*protocol.Series) error {
	var err error
	db, ok := s.dbs[database]
	if !ok {
		db, err = bolt.Open(s.baseDir+"/"+database, 0666)
		if err != nil {
			return err
		}

		s.dbs[database] = db
	}

	// transactional insert
	return db.Update(func(tx *bolt.Tx) error {
		for _, serie := range series {
			seriesName := serie.GetName()
			if seriesName == "" {
				continue
			}

			b, err := tx.CreateBucketIfNotExists([]byte("series"))
			if err != nil {
				return err
			}

			b.Put([]byte(seriesName), nil)

			b, err = tx.CreateBucketIfNotExists([]byte("data"))
			if err != nil {
				return err
			}

			keyBuffer := bytes.NewBuffer(nil)
			valueBuffer := proto.NewBuffer(nil)

			for _, point := range serie.GetPoints() {
				// Each point has a timestamp and sequence number.
				timestamp := uint64(point.GetTimestamp())

				// key: <series name>\x00<timestamp><sequence number>\x00<field>
				keyBuffer.WriteString(seriesName)
				keyBuffer.WriteByte(0)

				binary.Write(keyBuffer, binary.BigEndian, &timestamp)
				binary.Write(keyBuffer, binary.BigEndian, point.SequenceNumber)

				for fieldIndex, field := range serie.Fields {
					if point.Values[fieldIndex].GetIsNull() {
						continue
					}
					err = valueBuffer.Marshal(point.Values[fieldIndex])
					if err != nil {
						return err
					}

					fieldBucket, fieldBucketErr := tx.CreateBucketIfNotExists([]byte("fields"))
					if fieldBucketErr != nil {
						return fieldBucketErr
					}
					fieldBucket.Put([]byte(seriesName+"\x00"+field), nil)
					b.Put(append(keyBuffer.Bytes(), []byte("\x00"+field)...), valueBuffer.Bytes())
				}
			}
		}

		return nil
	})
}

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
	dbs    map[string]*bolt.DB
	closed bool
}

func (s *BoltShard) NewListmapShard() (*BoltShard, error) {
	return &BoltShard{
		closed: false,
	}, nil
}

func (s *BoltShard) DropDatabase(database string) error {
	return nil
}

func (s *BoltShard) close() {
	s.db.Close()
	s.closed = true
}

func (s *BoltShard) IsClosed() bool {
	return s.closed
}

func (s *BoltShard) Query(querySpec *parser.QuerySpec, processor cluster.QueryProcessor) error {
	return nil
}

func (s *BoltShard) Write(database string, series []*protocol.Series) error {
	db, ok := s.dbs[database]
	if !ok {
		db = bolt.Open("/tmp/influxdb/"+database, 0666)
	}

	// transactional insert
	db.Update(func(tx *bolt.Tx) error {
		for _, serie := range series {
			seriesName := series.GetName()
			if seriesName == "" {
				continue
			}

			b, err := tx.CreateBucketIfNotExists([]byte(seriesName))
			if err != nil {
				return err
			}

			keyBuffer := bytes.NewBuffer(make([]byte, 0, 24))
			valueBuffer := proto.NewBuffer(nil)

			for _, point := range serie.GetPoints() {
				// Each point has a timestamp and sequence number.
				timestamp := uint64(point.GetTimestamp())
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

					b.Put(append(keyBuffer.Bytes(), []bytes(field)...), valueBuffer.Bytes())
				}
			}
		}
	})
}

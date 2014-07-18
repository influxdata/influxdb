package main

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"time"

	"code.google.com/p/goprotobuf/proto"
	"github.com/influxdb/influxdb/datastore/storage"
	"github.com/influxdb/influxdb/protocol"
)

type Config struct {
	points       int
	batch        int
	series       int
	nextSeriesId int
	nextSequence int64
	now          time.Time
	path         string
}

func (c *Config) MakeBatch() []storage.Write {
	ws := make([]storage.Write, 0, c.batch)
	for b := c.batch; b > 0; b-- {
		key := bytes.NewBuffer(nil)
		binary.Write(key, binary.BigEndian, int64(c.nextSeriesId))
		binary.Write(key, binary.BigEndian, c.now.UnixNano()/1000)
		binary.Write(key, binary.BigEndian, c.nextSequence)

		v := rand.Int63()
		fv := &protocol.FieldValue{
			Int64Value: &v,
		}
		b, err := proto.Marshal(fv)
		if err != nil {
			panic(err)
		}

		ws = append(ws, storage.Write{
			Key:   key.Bytes(),
			Value: b,
		})

		c.nextSeriesId++
		if c.nextSeriesId >= c.series {
			c.nextSeriesId = 0
		}
		c.nextSequence++
		c.now = c.now.Add(time.Microsecond)
	}
	return ws
}

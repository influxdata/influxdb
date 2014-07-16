package tcp

import (
	. "github.com/influxdb/influxdb/common"
	"bytes"
	"github.com/influxdb/influxdb/protocol"

	"code.google.com/p/goprotobuf/proto"
	log "code.google.com/p/log4go"
)

// TODO: don't define twice.
type Writer interface {
	yield(*protocol.Series) error
	done()
}

type ChunkedPointsWriter struct {
	memSeries map[string]*protocol.Series
	conn         Connection
	precision TimePrecision
	limit int
	c chan *Command
}

func NewChunkedPointsWriter(conn Connection, precision TimePrecision, limit, buffer int) *ChunkedPointsWriter {
	writer := &ChunkedPointsWriter{map[string]*protocol.Series{}, conn, precision, limit, nil}
	writer.c = make(chan *Command, buffer)

	return writer
}

func (self *ChunkedPointsWriter) yield(series *protocol.Series) error {
	if self.memSeries[*series.Name] == nil {
		self.memSeries[*series.Name] = series
	} else {
		self.memSeries[series.GetName()] = MergeSeries(self.memSeries[series.GetName()], series)
	}

	size := len(self.memSeries[series.GetName()].Points)
	end := 0

	if size > self.limit {
		for i := 0; (i * self.limit) < size; i++ {
			newSeries := &protocol.Series{}
			newSeries.Name = series.Name
			newSeries.Fields = series.Fields

			end = ((i+1) * self.limit)
			if end > size {
				end = size
			}

			newSeries.Points = self.memSeries[series.GetName()].Points[i*self.limit:end]
			response := &Command{
				Type: &C_QUERY,
				Result: &C_OK,
				Continue: proto.Bool(true),
				Query: &Command_Query{
					Series: &Command_Series{
						Series: []*protocol.Series{},
					},
				},
			}
			response.GetQuery().GetSeries().Series = append(response.GetQuery().GetSeries().Series, newSeries)

			d, _ := proto.Marshal(response)
			self.conn.Write(uint32(len(d)), bytes.NewReader(d))
		}
	}

	self.memSeries[series.GetName()].Points = self.memSeries[series.GetName()].Points[end:]
	return nil
}

func (self *ChunkedPointsWriter) done() {
	var rseries []*protocol.Series

	// TODO: keep message as small
	for _, series := range self.memSeries {
		rseries = append(rseries, series)
	}

	response := &Command{
		Type: &C_QUERY,
		Result: &C_OK,
		Continue: proto.Bool(false),
		Query: &Command_Query{
			Series: &Command_Series{
				Series: []*protocol.Series{},
			},
		},
	}

	response.GetQuery().GetSeries().Series = rseries

	d, _ := proto.Marshal(response)
	self.conn.Write(uint32(len(d)), bytes.NewReader(d))
	close(self.c)
	log.Debug("WRITE FINISHED!")
}

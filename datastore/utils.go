package datastore

import (
	"code.google.com/p/goprotobuf/proto"
	"code.google.com/p/log4go"

	"github.com/influxdb/influxdb/engine"
	"github.com/influxdb/influxdb/protocol"
)

func yieldToProcessor(s *protocol.Series, p engine.Processor, aliases []string) (bool, error) {
	for _, alias := range aliases {
		series := &protocol.Series{
			Name:   proto.String(alias),
			Fields: s.Fields,
			Points: s.Points,
		}
		log4go.Debug("Yielding to %s %s", p.Name(), series)
		if ok, err := p.Yield(series); !ok || err != nil {
			return ok, err
		}
	}
	return true, nil
}

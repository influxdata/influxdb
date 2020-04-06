package inputs

import "github.com/influxdata/influxdb/v2/telegraf/plugins"

type baseInput int

func (b baseInput) Type() plugins.Type {
	return plugins.Input
}

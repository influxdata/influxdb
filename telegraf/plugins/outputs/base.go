package outputs

import "github.com/influxdata/influxdb/telegraf/plugins"

type baseOutput int

func (b baseOutput) Type() plugins.Type {
	return plugins.Output
}

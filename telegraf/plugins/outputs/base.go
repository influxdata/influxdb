package outputs

import "github.com/influxdata/influxdb/v2/telegraf/plugins"

type baseOutput int

func (b baseOutput) Type() plugins.Type {
	return plugins.Output
}

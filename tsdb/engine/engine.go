package engine

import (
	// blank import to register engine b1
	_ "github.com/influxdb/influxdb/tsdb/engine/b1"
	// blank import to register engine bz1
	_ "github.com/influxdb/influxdb/tsdb/engine/bz1"
	// blank import to register engine tsm1
	_ "github.com/influxdb/influxdb/tsdb/engine/tsm1"
)

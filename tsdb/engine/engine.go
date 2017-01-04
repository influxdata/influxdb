// Package engine can be imported to initialize and register all available TSDB engines.
//
// Alternatively, you can import any individual subpackage underneath engine.
package engine // import "github.com/influxdata/influxdb/tsdb/engine"

import (
	// Initialize and register tsm1 engine
	_ "github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

package handlers

import "github.com/influxdata/mrfusion"

// Store handles REST calls to the persistence
type Store struct {
	ExplorationStore mrfusion.ExplorationStore
	SourcesStore     mrfusion.SourcesStore
	ServersStore     mrfusion.ServersStore
}

package handlers

import "github.com/influxdata/chronograf"

// Store handles REST calls to the persistence
type Store struct {
	ExplorationStore chronograf.ExplorationStore
	SourcesStore     chronograf.SourcesStore
	ServersStore     chronograf.ServersStore
	LayoutStore      chronograf.LayoutStore
}

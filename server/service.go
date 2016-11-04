package server

import "github.com/influxdata/chronograf"

// Service handles REST calls to the persistence
type Service struct {
	ExplorationStore chronograf.ExplorationStore
	SourcesStore     chronograf.SourcesStore
	ServersStore     chronograf.ServersStore
	LayoutStore      chronograf.LayoutStore
	AlertRulesStore  chronograf.AlertRulesStore
	TimeSeries       chronograf.TimeSeries
	Logger           chronograf.Logger
}

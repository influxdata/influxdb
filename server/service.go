package server

import "github.com/influxdata/chronograf"

// Service handles REST calls to the persistence
type Service struct {
	ExplorationStore chronograf.ExplorationStore
	SourcesStore     chronograf.SourcesStore
	ServersStore     chronograf.ServersStore
	LayoutStore      chronograf.LayoutStore
	AlertRulesStore  chronograf.AlertRulesStore
	UsersStore       chronograf.UsersStore
	DashboardsStore	 chronograf.DashboardsStore
	TimeSeries       chronograf.TimeSeries
	Logger           chronograf.Logger
	UseAuth          bool
}

// ErrorMessage is the error response format for all service errors
type ErrorMessage struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

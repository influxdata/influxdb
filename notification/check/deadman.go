package check

import (
	"encoding/json"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/notification"
)

var _ influxdb.Check = &Deadman{}

// Deadman is the deadman check.
type Deadman struct {
	Base
	// seconds before deadman triggers
	TimeSince uint `json:"timeSince"`
	// If only zero values reported since time, trigger alert.
	ReportZero bool                    `json:"reportZero"`
	Level      notification.CheckLevel `json:"level"`
}

// Type returns the type of the check.
func (c Deadman) Type() string {
	return "deadman"
}

// GenerateFlux returns a flux script for the Deadman provided.
func (c Deadman) GenerateFlux() (string, error) {
	// TODO(desa): needs implementation
	return `package main
data = from(bucket: "telegraf")
	|> range(start: -1m)

option task = {name: "name1", every: 1m}`, nil
}

type deadmanAlias Deadman

// MarshalJSON implement json.Marshaler interface.
func (c Deadman) MarshalJSON() ([]byte, error) {
	return json.Marshal(
		struct {
			deadmanAlias
			Type string `json:"type"`
		}{
			deadmanAlias: deadmanAlias(c),
			Type:         c.Type(),
		})
}
